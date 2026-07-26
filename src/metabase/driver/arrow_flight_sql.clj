(ns metabase.driver.arrow-flight-sql
  "Arrow Flight SQL Driver for Metabase.

  This driver uses the Apache Arrow Flight SQL JDBC driver.
  ...
  " 
  (:import
   (java.sql Connection PreparedStatement ResultSet Timestamp)
   (java.time LocalDate LocalTime LocalDateTime OffsetDateTime)
   )
  (:require
   ;; String manipulation functions.
   [clojure.string :as str]
   ;; JDBC library for database connectivity.
   [clojure.java.jdbc :as jdbc]
   ;; URL encoding/decoding utilities.
   [ring.util.codec :as codec]
   ;; Core Metabase driver functionality.
   [metabase.driver :as driver]
   ;; Sanctioned facade over Metabase internals (secrets, QP helpers, ...).
   [metabase.driver-api.core :as driver-api]
   ;; SQL generation and manipulation.
   [honey.sql :as sql]
   ;; Connection management for SQL-JDBC drivers.
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
   ;; Logging utilities.
   [metabase.util.log :as log]
   ;; SQL query processing.
   [metabase.driver.sql.query-processor :as sql.qp]
   ;; Schema synchronization for SQL-JDBC drivers.
   [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
   ;; Schema inclusion/exclusion patterns from the schema-filters property.
   [metabase.driver.sync :as driver.s]
   ;; SQL execution helper functions.
   [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
   [metabase.util.honey-sql-2        :as h2x] 

   ))

;; ----------------------------------------------------------------
;; In production the plugin manifest registers the (lazy-loading) driver
;; before this namespace loads, but Metabase's dev/test harness loads driver
;; namespaces directly and expects them to self-register — the same
;; unconditional call every in-tree module driver makes (re-registration is
;; idempotent).
(driver/register! :arrow-flight-sql :parent :sql-jdbc)

;; ----------------------------------------------------------------
;; Define the display name for the Arrow Flight SQL driver.
(defmethod driver/display-name :arrow-flight-sql [_]
  "Arrow Flight SQL")

;; ----------------------------------------------------------------
;; Override connection options to skip transaction isolation level check.
;; Arrow Flight SQL JDBC driver throws NullPointerException when checking
;; supportsTransactionIsolationLevel because some servers (like GizmoSQL)
;; don't return required SqlInfo metadata.
;; Executor used only to unblock stuck socket reads via setNetworkTimeout,
;; mirroring the default implementation's behavior.
(defonce ^:private network-timeout-executor
  (delay (java.util.concurrent.Executors/newCachedThreadPool)))

(def ^:private network-timeout-ms (* 10 60 1000))

(defmethod sql-jdbc.execute/do-with-connection-with-options :arrow-flight-sql
  [driver db-or-id-or-spec options f]
  (sql-jdbc.execute/do-with-resolved-connection
   driver
   db-or-id-or-spec
   options
   (fn [^Connection conn]
     ;; Deliberately skip set-best-transaction-level!: servers with incomplete
     ;; GetSqlInfo make DatabaseMetaData.supportsTransactionIsolationLevel NPE.
     ;; Session-timezone is not handled yet, matching :set-timezone unsupported.
     ;; Like the default impl, only set options on the outermost acquisition so
     ;; nested (e.g. :write?) scopes are not clobbered.
     (when-not (sql-jdbc.execute/recursive-connection?)
       (try
         ;; a hint for the server, and the honest value for future write support
         (.setReadOnly conn (not (:write? options)))
         (catch Throwable _ nil))
       (when-not (:write? options)
         (try
           (.setAutoCommit conn true)
           (catch Throwable _ nil)))
       (try
         ;; releases threads stuck in blocking socket reads; .close alone doesn't
         (.setNetworkTimeout conn ^java.util.concurrent.Executor @network-timeout-executor
                             network-timeout-ms)
         (catch Throwable _ nil))
       (try
         (.setHoldability conn ResultSet/CLOSE_CURSORS_AT_COMMIT)
         (catch Throwable _ nil)))
     (f conn))))

;; ----------------------------------------------------------------
;; Register feature support flags for the driver.
;; Note: the :sql parent already defaults ~26 features to true (expressions,
;; binning, nested-queries, joins, native-parameters, parameterized-sql, ...),
;; so only deviations from the parent defaults are declared here.
(doseq [[feature supported?]
        {;; one streaming information_schema.columns query for the whole DB
         :describe-fields           true
         :connection-impersonation  false
         ;; No sql.qp/->honeysql [:arrow-flight-sql :convert-timezone]
         ;; implementation exists (and there is no [:sql ...] default), so
         ;; declaring true exposed convertTimezone() in the expression editor
         ;; only for it to fail at query-compile time.
         :convert-timezone          false
         ;; FK metadata is not reliably available across Flight SQL servers.
         ;; The :sql parent defaults this to true, which (since Metabase 0.63
         ;; removed describe-table-fks) makes sync call driver/describe-fks,
         ;; whose sql-jdbc fallback reads JDBC DatabaseMetaData — a path that
         ;; NPEs on servers with incomplete GetSqlInfo support.
         :metadata/key-constraints  false
         ;; DuckDB has neither IDENTITY columns nor implicit rowids for plain
         ;; INTEGER PKs, so uploaded tables carry no _mb_row_id column (the
         ;; ClickHouse precedent).
         :upload-with-auto-pk       false}]
  (defmethod driver/database-supports? [:arrow-flight-sql feature]
    [_driver _feature _db]
    supported?))

;; Write-dependent features (CSV uploads and Data Studio table transforms)
;; are opt-in PER CONNECTION: only writable Flight SQL backends (e.g.
;; GizmoSQL/DuckDB, quack-on-demand, Doris, StarRocks) accept CREATE TABLE /
;; INSERT, while others (InfluxDB 3, Spice accelerations, ROAPI, kamu,
;; Deephaven) are read-only. The toggle lives in connection details so
;; read-only backends never advertise these features. Transforms machinery
;; (compile-transform CTAS, run-transform!, drop-transform-target!,
;; execute-raw-queries!) is fully inherited from the :sql/:sql-jdbc parents.
(doseq [feature [:uploads :transforms/table]]
  (defmethod driver/database-supports? [:arrow-flight-sql feature]
    [_driver _feature db]
    (boolean (get-in db [:details :enable-uploads]))))

;; ----------------------------------------------------------------
;; Build a connection spec from the provided database details.
;; This constructs a JDBC connection specification map for Arrow Flight SQL.
;; ----------------------------------------------------------------
;; Define the cast function once as a stable reference to avoid pool invalidation.
;; If this is an anonymous fn inside connection-details->spec, the hash changes
;; on every call, causing Metabase to constantly invalidate the connection pool.
(defn- arrow-flight-sql-cast-fn
  "Cast function for Arrow Flight SQL result values."
  [col val]
  (case (:base-type col)
    :type/Date     (if (instance? java.sql.Date val) (.toLocalDate ^java.sql.Date val) val)
    :type/Time     (if (instance? java.sql.Time val) (.toLocalTime ^java.sql.Time val) val)
    :type/DateTime (cond
                     (instance? java.sql.Timestamp val) (.toLocalDateTime ^java.sql.Timestamp val)
                     (instance? java.time.OffsetDateTime val) (.toLocalDateTime ^java.time.OffsetDateTime val)
                     :else val)
    val))

(defn- non-blank-str [s]
  (when (and (string? s) (not (str/blank? s)))
    s))

(defn- secret-string
  "Resolve a secret-typed connection property (stored by the UI as
  <prop>-value / <prop>-id) with a fallback to a plain key of the same name
  for connections created directly via the REST API."
  [driver details prop]
  (or (driver-api/secret-value-as-string driver details prop)
      (non-blank-str (get details (keyword prop)))))

(defn- secret-file-path
  "Materialize a secret-typed connection property to a file on disk and return
  its absolute path (the Arrow JDBC driver wants file paths for TLS material)."
  [driver details prop]
  (try
    (some-> (driver-api/secret-value-as-file! driver details prop) str non-blank-str)
    (catch Throwable e
      (log/warnf e "Could not resolve secret property %s as a file" prop)
      nil)))

(defn- auth-query-params
  "Query-string fragments for the configured authentication.

  - use-token true (UI toggle; legacy auth-method \"token\" also honored)
    -> authorization=Bearer <token> (InfluxDB 3 tokens, Dremio PATs, any
       pre-issued bearer/API token)
  - username + password -> Flight handshake basic auth. A blank username with
    a password is emitted as user=&password=<pw> — the Spice.ai API-key
    convention. GizmoSQL external JWTs use the literal username \"token\"
    with the JWT as the password.
  - nothing configured -> anonymous.

  Connections created before the use-token toggle carry neither flag; the
  mode is inferred from which credentials are present."
  [driver {:keys [use-token auth-method username user password] :as details}]
  (let [username* (or (non-blank-str username) (non-blank-str user))
        password* (non-blank-str password)
        token*    (secret-string driver details "token")
        token?    (cond
                    (some? use-token)           (boolean use-token)
                    (non-blank-str auth-method) (contains? #{"token" "jwt"} auth-method)
                    :else                       (some? token*))]
    (cond
      (and token? token*)
      [(str "authorization=Bearer%20" (codec/url-encode token*))]

      (and username* password*)
      [(str "user=" (codec/url-encode username*))
       (str "password=" (codec/url-encode password*))]

      password*
      ["user=" (str "password=" (codec/url-encode password*))]

      :else [])))

;; ----------------------------------------------------------------
;; Backfill the use-token toggle on details created before it existed, so the
;; admin form opens in the right mode when editing (the token field is hidden
;; behind visible-if use-token — without the flag, a stored token would be
;; invisible and could be lost on save).
(defn- backfill-auth-flags [details]
  (if (or (not (map? details)) (contains? details :use-token) (empty? details))
    details
    (let [token? (boolean (or (contains? #{"token" "jwt"} (:auth-method details))
                              (non-blank-str (:token details))
                              (:token-id details)
                              (non-blank-str (:token-value details))))]
      (assoc details :use-token token?))))

(defmethod driver/normalize-db-details :arrow-flight-sql
  [_driver database]
  (cond-> (update database :details backfill-auth-flags)
    (:write-data-details database) (update :write-data-details backfill-auth-flags)
    (:admin-details database)      (update :admin-details backfill-auth-flags)))

(defmethod sql-jdbc.conn/connection-details->spec :arrow-flight-sql
  [driver details]
  (let [{:keys [host port useEncryption disableCertificateVerification]
         :or   {useEncryption true
                disableCertificateVerification false}} details

        ;; TLS material is secret-typed; the JDBC driver takes file paths.
        root-certs-path  (secret-file-path driver details "tls-root-certs")
        client-cert-path (secret-file-path driver details "client-cert")
        client-key-path  (secret-file-path driver details "client-key")

        connect-timeout  (some-> (:connect-timeout-millis details) str non-blank-str)
        additional-opts  (non-blank-str (:additional-options details))

        ;; Build query params
        ;; NOTE: We intentionally do NOT include catalog in the JDBC URL.
        ;; The Arrow Flight SQL JDBC driver calls SetSessionOptions when catalog is specified,
        ;; but many Flight SQL servers (e.g., GizmoSQL/DuckDB) don't implement this RPC.
        ;; Instead, we use the catalog value only for filtering during schema sync
        ;; (see describe-database and describe-fields-sql methods).
        params    (cond-> (vec (auth-query-params driver details))
                    true (conj (str "useEncryption=" (boolean useEncryption)))
                    true (conj (str "disableCertificateVerification=" (boolean disableCertificateVerification)))
                    root-certs-path  (conj (str "tlsRootCerts=" (codec/url-encode root-certs-path)))
                    (and client-cert-path client-key-path)
                    (into [(str "clientCertificate=" (codec/url-encode client-cert-path))
                           (str "clientKey=" (codec/url-encode client-key-path))])
                    connect-timeout  (conj (str "connectTimeoutMillis=" connect-timeout))
                    ;; free-form passthrough: threadPoolSize, retainAuth/retainCookies,
                    ;; oauth.* (Arrow JDBC >= 19.0.0), or custom gRPC headers
                    additional-opts  (conj additional-opts))
        qp        (str/join "&" params)

        base-url  (str "jdbc:arrow-flight-sql://"
                       (or host "localhost") ":"
                       (or port 443))
        full-url  (str base-url (when-not (str/blank? qp) (str "?" qp)))]
    ;; never log the query string — it carries credentials
    (log/debug "Arrow Flight SQL connection URL (params redacted):" base-url)
    (let [scheme  "jdbc:arrow-flight-sql:"
          subname (subs full-url (count scheme))]
      {:classname   "org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver"
       :subprotocol "arrow-flight-sql"
       :subname     subname
       ;; Use stable function reference to prevent pool invalidation
       :cast arrow-flight-sql-cast-fn})))

;; ----------------------------------------------------------------
;; NOTE: no driver/can-connect? override. The :sql-jdbc default runs the
;; test query through the shared machinery, which also resolves secrets,
;; SSH tunnels, and EE auth providers. The old override returned false when
;; connection close threw a non-SQLException even though the test query had
;; succeeded (issue #11); Arrow JDBC >= 19.0.0 suppresses those benign
;; close-time gRPC exceptions at the source (apache/arrow-java GH-863).

;; ----------------------------------------------------------------
;; Map raw database types to Metabase base types.
;; This converts a database-specific type into a standardized Metabase type.
(def ^:private database-type->base-type
  (sql-jdbc.sync/pattern-based-database-type->base-type
   [[#"BOOL"                       :type/Boolean]
    [#"INT(8|16|32|64)?$"          :type/Integer]
    [#"UINT(8|16|32)$"             :type/Integer]
    [#"UINT64"                     :type/BigInteger]
    [#"BIGINT|HUGEINT"             :type/BigInteger]
    [#"FLOAT(16|32|64)?$"          :type/Float]
    [#"DOUBLE|REAL"                :type/Float]
    [#"DECIMAL|NUMERIC"            :type/Decimal]
    [#"DATE32|DATE"                :type/Date]
    [#"TIME(32|64)?$"              :type/Time]
    [#"TIMESTAMP"                  :type/DateTime]
    [#"UTF8|CHAR|STRING|TEXT|VARCHAR" :type/Text]
    [#"JSON"                       :type/JSON]
    [#"UUID"                       :type/UUID]
    ;; fallback for anything else
    [#".*"                         :type/*]]))

(defmethod sql-jdbc.sync/database-type->base-type :arrow-flight-sql
  [_ raw-db-type]
  ;; strip off any precision/scale qualifiers, e.g. "DECIMAL(10,2)" → "DECIMAL"
  (let [normalized (-> raw-db-type
                       (str/replace #"\(.*\)" "")
                       str/upper-case)]
    (database-type->base-type normalized)))


;; ----------------------------------------------------------------
;; Define a reader function for TIMESTAMP columns.
;; Retrieves a timestamp from the ResultSet and converts it to a local date-time.
(defmethod sql-jdbc.execute/read-column-thunk [:arrow-flight-sql java.sql.Types/TIMESTAMP]
  [_driver ^java.sql.ResultSet rs _rsmeta ^Integer i]
  (fn []
    (some-> (.getTimestamp rs i)
            .toLocalDateTime)))

;; ----------------------------------------------------------------
;; Define a reader function for DATE columns.
;; Retrieves a timestamp from the ResultSet and converts it to a local date.
(defmethod sql-jdbc.execute/read-column-thunk
  [:arrow-flight-sql java.sql.Types/DATE]
  [_driver ^java.sql.ResultSet rs _rsmeta ^Integer i]
  (fn [] (some-> (.getDate rs i) .toLocalDate)))

;; ----------------------------------------------------------------
;; Define a reader function for TIME columns.
;; Retrieves a timestamp from the ResultSet and converts it to a local time.
(defmethod sql-jdbc.execute/read-column-thunk
  [:arrow-flight-sql java.sql.Types/TIME]
  [_driver ^java.sql.ResultSet rs _rsmeta ^Integer i]
  (fn [] (some-> (.getTime rs i) .toLocalTime)))

;; ----------------------------------------------------------------
;; Custom Schema Sync Implementations
;; ----------------------------------------------------------------

;; Helper function to safely close connections when catalog parameter is used
(defn- safely-close-connection [conn]
  (try
    (.close conn)
    (catch java.sql.SQLException e
      (when-not (re-find #"Channel shutdown" (.getMessage e))
        (throw e))
      ;; Log but ignore "Channel shutdown" errors during close
      (log/debug "Ignoring connection close error (known Flight SQL issue with catalog param):" (.getMessage e)))))


;; System schemas that are never interesting to sync, regardless of user
;; schema filters. Consulted by our describe-database implementation.
(defmethod sql-jdbc.sync/excluded-schemas :arrow-flight-sql
  [_driver]
  #{"information_schema" "runtime" "pg_catalog"})

;; List tables by querying information_schema.tables.
;; When a catalog is specified in connection details, we filter by table_catalog.
;; Catalog hierarchy: Catalog → Schema → Table
;; User include/exclude patterns from the schema-filters connection property
;; are honored via driver.s/include-schema?.
(defmethod driver/describe-database :arrow-flight-sql
  [driver database]
  (let [spec     (sql-jdbc.conn/connection-details->spec driver (:details database))
        catalog  (non-blank-str (get-in database [:details :catalog]))
        ;; Inline (escaped) rather than a bound parameter: prepared-statement
        ;; params on information_schema queries return zero rows on some
        ;; Flight SQL servers (Spice/DataFusion, quack-on-demand) even though
        ;; they work on GizmoSQL.
        sql+args (if catalog
                   [(format "SELECT table_name, table_schema FROM information_schema.tables WHERE LOWER(table_catalog) = LOWER('%s')"
                            (str/replace catalog "'" "''"))]
                   ["SELECT table_name, table_schema FROM information_schema.tables"])
        excluded (sql-jdbc.sync/excluded-schemas driver)
        conn     (jdbc/get-connection spec)]
    (try
      (let [rows   (jdbc/query {:connection conn} sql+args {:identifiers str/lower-case})
            tables (keep (fn [{:keys [table_name table_schema]}]
                           (when (and (not (contains? excluded (str/lower-case (or table_schema ""))))
                                      (driver.s/include-schema? database table_schema))
                             {:name   table_name
                              :schema table_schema}))
                         rows)]
        {:tables (into #{} tables)})
      (finally
        (safely-close-connection conn)))))

;; ----------------------------------------------------------------
;; Describe a single table via information_schema.columns. This is mostly a
;; fallback path (sync uses the bulk describe-fields because :describe-fields
;; is true); it deliberately avoids DuckDB's DESCRIBE statement so it works on
;; any Flight SQL server exposing information_schema.
(defmethod driver/describe-table :arrow-flight-sql
  [driver database {:keys [name schema]}]
  (let [spec (sql-jdbc.conn/connection-details->spec driver (:details database))
        conn (jdbc/get-connection spec)]
    (try
      (let [results (jdbc/query {:connection conn}
                                [(str "SELECT column_name, data_type, is_nullable, ordinal_position"
                                      " FROM information_schema.columns"
                                      " WHERE LOWER(table_schema) = LOWER(?) AND LOWER(table_name) = LOWER(?)"
                                      " ORDER BY ordinal_position")
                                 schema name]
                                {:identifiers str/lower-case})
            fields  (into #{}
                          (map-indexed
                           (fn [idx {:keys [column_name data_type]}]
                             {:name              column_name
                              :database-type     data_type
                              :base-type         (sql-jdbc.sync/database-type->base-type driver data_type)
                              :database-position idx}))
                          results)]
        {:name   name
         :schema schema
         :fields fields})
      (finally
        (safely-close-connection conn)))))

;; ----------------------------------------------------------------
;; NOTE: driver/describe-table-fks is intentionally NOT implemented. The
;; multimethod was removed in Metabase 0.63 (a defmethod on it would fail the
;; namespace load there); FK sync is disabled via :metadata/key-constraints
;; false above. If per-backend FK support is wanted later, implement
;; sql-jdbc.sync/describe-fks-sql against information_schema instead.

;; ----------------------------------------------------------------
;; Describe fields by querying the information_schema.columns table.
;; Builds a dynamic SQL query based on provided schema and table names.
;; When a catalog is specified, we filter by table_catalog.
;; Catalog hierarchy: Catalog → Schema → Table
(defmethod sql-jdbc.sync/describe-fields-sql :arrow-flight-sql
  [driver & {:keys [schema-names table-names details]}]
  (let [catalog (:catalog details)
        use-catalog? (and (string? catalog) (not (str/blank? catalog)))
        base-condition [:>= [:inline 1] [:inline 1]]
        ;; Filter by table_catalog when specified
        catalog-condition (when use-catalog?
                            [:= [:lower :table_catalog] [:inline (str/lower-case catalog)]])
        schema-condition (when (seq schema-names)
                           [:in [:lower :table_schema]
                            (mapv (fn [s] [:inline (str/lower-case s)]) schema-names)])
        table-condition (when (seq table-names)
                          [:in [:lower :table_name]
                           (mapv (fn [t] [:inline (str/lower-case t)]) table-names)])
        where-clause (cond-> [base-condition]
                       catalog-condition (conj catalog-condition)
                       schema-condition (conj schema-condition)
                       table-condition (conj table-condition))]
    (sql/format
     {:select [[:column_name :name]
               [:ordinal_position :database-position]
               [:table_schema :table-schema]
               [:table_name :table-name]
               [[[:upper :data_type]] :database-type]
               [[:inline false] :database-is-auto-increment]
               [[:case-expr [:= :is_nullable [:inline "NO"]] [:inline true] [:inline false]]
                :database-required]
               ;; NULL, not "": FieldMetadataEntry requires field-comment to
               ;; be a non-blank string or nil (enforced by malli in dev/test)
               [[:inline nil] :field-comment]]
      :from [[:information_schema.columns]]
      :where (vec (cons :and where-clause))
      :order-by [:table_schema :table_name :ordinal_position]}
     :dialect (sql.qp/quote-style driver))))


;; ----------------------------------------------------------------
;; Support `… - INTERVAL 'N unit'` in filters like "yesterday"
(defmethod sql.qp/add-interval-honeysql-form :arrow-flight-sql
  [_driver hsql-form amount unit]
  (if (= unit :quarter)
    ;; Duck the lack of quarters by translating 1 quarter → 3 months, etc.
    (recur _driver hsql-form (* amount 3) :month)
    ;; Build: (<hsql-form> + INTERVAL 'amount unit')
    (h2x/+ (h2x/->timestamp-with-time-zone hsql-form)
           [:raw (format "(INTERVAL '%d' %s)" (int amount) (name unit))])))


;; ----------------------------------------------------------------
;; Support date‐truncation and extraction for Arrow Flight SQL
(defmethod sql.qp/date [:arrow-flight-sql :default]
  [_driver _expr-type expr]
  ;; fall back to the raw expression
  expr)

(defmethod sql.qp/date [:arrow-flight-sql :minute]
  [_driver _ expr]
  [:date_trunc (h2x/literal :minute) expr])

(defmethod sql.qp/date [:arrow-flight-sql :minute-of-hour]
  [_driver _ expr]
  [:minute expr])

(defmethod sql.qp/date [:arrow-flight-sql :hour]
  [_driver _ expr]
  [:date_trunc (h2x/literal :hour) expr])

(defmethod sql.qp/date [:arrow-flight-sql :hour-of-day]
  [_driver _ expr]
  [:hour expr])

(defmethod sql.qp/date [:arrow-flight-sql :day]
  [_driver _ expr]
  [:date_trunc (h2x/literal :day) expr])

(defmethod sql.qp/date [:arrow-flight-sql :day-of-month]
  [_driver _ expr]
  [:day expr])

(defmethod sql.qp/date [:arrow-flight-sql :day-of-year]
  [_driver _ expr]
  [:dayofyear expr])

(defmethod sql.qp/date [:arrow-flight-sql :day-of-week]
  [driver _ expr]
  ;; adjust to Metabase's configured start‐of‐week
  (sql.qp/adjust-day-of-week driver [:isodow expr]))

(defmethod sql.qp/date [:arrow-flight-sql :week]
  [driver _ expr]
  ;; preserves your db‐start‐of‐week setting
  (sql.qp/adjust-start-of-week
   driver
   (partial conj [:date_trunc] (h2x/literal :week))
   expr))

(defmethod sql.qp/date [:arrow-flight-sql :month]
  [_driver _ expr]
  [:date_trunc (h2x/literal :month) expr])

(defmethod sql.qp/date [:arrow-flight-sql :month-of-year]
  [_driver _ expr]
  [:month expr])

(defmethod sql.qp/date [:arrow-flight-sql :quarter]
  [_driver _ expr]
  [:date_trunc (h2x/literal :quarter) expr])

(defmethod sql.qp/date [:arrow-flight-sql :quarter-of-year]
  [_driver _ expr]
  [:quarter expr])

(defmethod sql.qp/date [:arrow-flight-sql :year]
  [_driver _ expr]
  [:date_trunc (h2x/literal :year) expr])

(defmethod sql-jdbc.execute/set-parameter
  ;; Bind a LocalDateTime into the PreparedStatement as a SQL Timestamp
  [:arrow-flight-sql LocalDateTime]
  [_driver ^PreparedStatement stmt ^Integer idx ^LocalDateTime dt]
  (.setTimestamp stmt idx (Timestamp/valueOf dt)))

(defmethod sql-jdbc.execute/set-parameter
  [:arrow-flight-sql OffsetDateTime]
  [_driver ^PreparedStatement stmt ^Integer idx ^OffsetDateTime odt]
  ;; convert OffsetDateTime → Timestamp at the same instant
  (.setTimestamp stmt
                 idx
                 (Timestamp/valueOf (.toLocalDateTime odt))))

;; CORRECT: exactly 4 parameters: driver, stmt, idx, value
(defmethod sql-jdbc.execute/set-parameter
  [:arrow-flight-sql LocalDate]
  [_driver ^PreparedStatement stmt ^Integer idx ^java.time.LocalDate d]
  (.setDate stmt idx (java.sql.Date/valueOf d)))

(defmethod sql-jdbc.execute/set-parameter
  [:arrow-flight-sql LocalTime]
  [_driver ^PreparedStatement stmt ^Integer idx ^java.time.LocalTime t]
  (.setTime stmt idx (java.sql.Time/valueOf t)))



(defmethod driver/db-start-of-week :arrow-flight-sql
  [_]
  :monday)

;; ----------------------------------------------------------------
;; CSV uploads (writable, DuckDB-flavored backends; gated per connection via
;; the enable-uploads detail above). The :sql-jdbc parent provides
;; create-table!/drop-table!/truncate!/insert-into!/add-columns!/
;; alter-table-columns! — the driver only supplies the type mapping.
(defmethod driver/upload-type->database-type :arrow-flight-sql
  [_driver upload-type]
  (case upload-type
    :metabase.upload/varchar-255              "VARCHAR"
    :metabase.upload/text                     "VARCHAR"
    :metabase.upload/int                      "BIGINT"
    :metabase.upload/auto-incrementing-int-pk "BIGINT"
    :metabase.upload/float                    "DOUBLE"
    :metabase.upload/boolean                  "BOOLEAN"
    :metabase.upload/date                     "DATE"
    :metabase.upload/datetime                 "TIMESTAMP"
    :metabase.upload/offset-datetime          "TIMESTAMP WITH TIME ZONE"))

(defmethod driver/table-name-length-limit :arrow-flight-sql
  [_driver]
  ;; DuckDB imposes no practical identifier-length limit
  nil)

(defmethod driver/allowed-promotions :arrow-flight-sql
  [_driver]
  ;; conservative: no implicit column-type relaxation on append/replace
  {})

(def ^:private ^java.time.format.DateTimeFormatter timestamp-literal-formatter
  (java.time.format.DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss"))

(def ^:private ^java.time.format.DateTimeFormatter time-literal-formatter
  (java.time.format.DateTimeFormatter/ofPattern "HH:mm:ss"))

(defn- upload-sql-literal
  "Render an upload value as a SQL literal. The default :sql-jdbc insert path
  binds values as JDBC parameters, but the Arrow JDBC driver cannot bind
  java.time temporals on the plain PreparedStatements uploads use
  (UnsupportedOperationException: Cannot convert from LocalDate to long) —
  so values are inlined instead."
  ^String [v]
  (cond
    (nil? v)                     "NULL"
    (instance? LocalDate v)      (format "DATE '%s'" v)
    (instance? LocalTime v)      (format "TIME '%s'" (.format ^LocalTime v time-literal-formatter))
    (instance? LocalDateTime v)  (format "TIMESTAMP '%s'" (.format ^LocalDateTime v timestamp-literal-formatter))
    ;; DuckDB's TIMESTAMPTZ parses ISO-8601 with offset
    (instance? OffsetDateTime v) (format "TIMESTAMP WITH TIME ZONE '%s'" v)
    (boolean? v)                 (if v "TRUE" "FALSE")
    (number? v)                  (str v)
    :else                        (str "'" (str/replace (str v) "'" "''") "'")))

(defmethod driver/insert-into! :arrow-flight-sql
  [driver db-id table-name column-names values]
  (let [dialect (sql.qp/quote-style driver)
        chunks  (partition-all (or driver/*insert-chunk-rows* 100) values)]
    (jdbc/with-db-transaction [conn (sql-jdbc.conn/db->pooled-connection-spec db-id)]
      (doseq [chunk chunks
              :let [statement (sql/format
                               {:insert-into (keyword table-name)
                                :columns     (mapv keyword column-names)
                                :values      (mapv (fn [row]
                                                     (mapv (fn [v] [:raw (upload-sql-literal v)]) row))
                                                   chunk)}
                               :quoted true
                               :dialect dialect)]]
        (jdbc/execute! conn statement)))))

;; ----------------------------------------------------------------
;; Inline absolute datetime values as typed SQL literals. Prepared-statement
;; temporal parameters are not reliably supported across Flight SQL servers,
;; so literals are safer for filters. The previous implementation formatted
;; every value with a "yyyy-MM-dd HH:mm:ss" pattern, which throws
;; UnsupportedTemporalTypeException for date-only (LocalDate) values.
(defmethod sql.qp/->honeysql [:arrow-flight-sql :absolute-datetime]
  [_driver [_ value _unit]]
  (condp instance? value
    LocalDate      [:raw (format "DATE '%s'" value)]
    LocalTime      [:raw (format "TIME '%s'" (.format ^LocalTime value time-literal-formatter))]
    LocalDateTime  [:raw (format "TIMESTAMP '%s'" (.format ^LocalDateTime value timestamp-literal-formatter))]
    ;; keep the wall-clock time, matching the previous behavior and the
    ;; TIMESTAMP-without-timezone storage of the demo backends
    OffsetDateTime [:raw (format "TIMESTAMP '%s'"
                                 (.format (.toLocalDateTime ^OffsetDateTime value)
                                          timestamp-literal-formatter))]
    ;; anything else: let the value flow through as a JDBC parameter
    ;; (set-parameter impls above cover the common temporal classes)
    value))
