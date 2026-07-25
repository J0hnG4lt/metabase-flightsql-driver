(ns metabase.driver.arrow-flight-sql
  "Arrow Flight SQL Driver for Metabase.

  This driver uses the Apache Arrow Flight SQL JDBC driver.
  ...
  " 
  (:import
   (java.sql Connection PreparedStatement ResultSet Timestamp Types Date Time)
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
   ;; SQL execution helper functions.
   [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
   [metabase.util.honey-sql-2        :as h2x] 

   ))

;; ----------------------------------------------------------------
;; Define the display name for the Arrow Flight SQL driver.
(defmethod driver/display-name :arrow-flight-sql [_]
  "Arrow Flight SQL")

;; ----------------------------------------------------------------
;; Override connection options to skip transaction isolation level check.
;; Arrow Flight SQL JDBC driver throws NullPointerException when checking
;; supportsTransactionIsolationLevel because some servers (like GizmoSQL)
;; don't return required SqlInfo metadata.
(defmethod sql-jdbc.execute/do-with-connection-with-options :arrow-flight-sql
  [driver db-or-id-or-spec options f]
  (sql-jdbc.execute/do-with-resolved-connection
   driver
   db-or-id-or-spec
   options
   (fn [^Connection conn]
     ;; Skip set-best-transaction-level! which causes NPE with Flight SQL.
     ;; Just set basic safe options that Flight SQL can handle.
     (try
       (.setReadOnly conn true)
       (catch Exception _ nil))
     (try
       (.setAutoCommit conn true)
       (catch Exception _ nil))
     (try
       (.setHoldability conn ResultSet/CLOSE_CURSORS_AT_COMMIT)
       (catch Exception _ nil))
     (f conn))))

;; ----------------------------------------------------------------
;; Register feature support flags for the driver.
;; This loop defines which features the driver supports.
(doseq [[feature supported?]
        {:describe-fields           true
         :connection-impersonation  false
         :convert-timezone          true
         :parameterized-sql         true}]
  (defmethod driver/database-supports? [:arrow-flight-sql feature]
    [_driver _feature _db]
    supported?))

;; ----------------------------------------------------------------
;; Helper function that returns the string if it is non-blank;
;; otherwise, it returns the provided default value.
(defn non-blank [s default]
  (if (and (string? s) (not (str/blank? s)))
    s
    default))

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

(defmethod sql-jdbc.conn/connection-details->spec :arrow-flight-sql
  [_ details]
  (let [{:keys [host port token username user password catalog useEncryption disableCertificateVerification]
         :or   {useEncryption true
                disableCertificateVerification false}} details
        ;; prefer :username (manifest) but fall back to :user
        username* (or (when (and (string? username) (not (str/blank? username))) username)
                      (when (and (string? user)      (not (str/blank? user)))      user))

        ;; URL-encode only provided creds
        enc-token (when (and (string? token)     (not (str/blank? token)))     (codec/url-encode token))
        enc-user  (when (and (string? username*) (not (str/blank? username*))) (codec/url-encode username*))
        enc-pass  (when (and (string? password)  (not (str/blank? password)))  (codec/url-encode password))

        ;; Choose exactly ONE auth mode: token wins over user/pass
        auth-qps  (cond
                    enc-token               [(str "authorization=Bearer%20" enc-token)]
                    (and enc-user enc-pass) [(str "user=" enc-user)
                                             (str "password=" enc-pass)]
                    :else                   [])

        ;; Build query params
        ;; NOTE: We intentionally do NOT include catalog in the JDBC URL.
        ;; The Arrow Flight SQL JDBC driver calls SetSessionOptions when catalog is specified,
        ;; but many Flight SQL servers (e.g., GizmoSQL/DuckDB) don't implement this RPC.
        ;; Instead, we use the catalog value only for filtering during schema sync
        ;; (see describe-database and describe-fields-sql methods).
        params    (cond-> []
                    true (into auth-qps)
                    true (conj (str "useEncryption=" (boolean useEncryption)))
                    true (conj (str "disableCertificateVerification=" (boolean disableCertificateVerification))))
        qp        (str/join "&" params)

        ;; Full JDBC URL
        full-url  (str "jdbc:arrow-flight-sql://"
                       (or host "localhost") ":"
                       (or port 443)
                       (when-not (str/blank? qp) (str "?" qp)))]
        (log/debug "Arrow Flight SQL connection URL:" full-url)
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


;; List tables by querying information_schema.tables.
;; When a catalog is specified in connection details, we filter by table_catalog.
;; Catalog hierarchy: Catalog → Schema → Table
(defmethod driver/describe-database :arrow-flight-sql
  [driver database]
  (let [spec (sql-jdbc.conn/connection-details->spec :arrow-flight-sql (:details database))
        catalog (get-in database [:details :catalog])
        use-catalog? (and (string? catalog) (not (str/blank? catalog)))
        ;; Filter by table_catalog when specified
        query (if use-catalog?
                (format "SELECT table_name, table_schema FROM information_schema.tables WHERE LOWER(table_catalog) = LOWER('%s')" catalog)
                "SELECT table_name, table_schema FROM information_schema.tables")
        conn (jdbc/get-connection spec)]
    (try
      (let [rows (jdbc/query {:connection conn}
                             [query]
                             {:identifiers str/lower-case})
            ;; Filter out system schemas
            formatted (->> rows
                           (filter #(not (contains? #{"information_schema" "runtime" "pg_catalog"}
                                                    (str/lower-case (or (:table_schema %) "")))))
                           (map (fn [row]
                                  {:name   (:table_name row)
                                   :schema (:table_schema row)})))]
        {:tables (into #{} formatted)})
      (finally
        (safely-close-connection conn)))))

;; ----------------------------------------------------------------
;; Describe a specific table by executing a DESCRIBE query.
(defmethod driver/describe-table :arrow-flight-sql
  [driver database {:keys [name schema]}]
  (let [spec (sql-jdbc.conn/connection-details->spec driver (:details database))
        conn (jdbc/get-connection spec)]
    (try
      (let [query   (format "DESCRIBE \"%s\".\"%s\"" schema name) ;; Build the DESCRIBE query using schema and table name
            results (jdbc/query {:connection conn} [query] {:identifiers str/lower-case})
            fields  (mapv (fn [{:keys [column_name data_type is_nullable]}]
                            (let [normalized-name (-> column_name
                                                      (str/replace #"^\"|\"$" "")
                                                      str/lower-case)]
                              {:name          normalized-name
                               :database-type data_type
                               :base-type     (sql-jdbc.sync/database-type->base-type driver data_type)
                               :nullable      (= "yes" (str/lower-case is_nullable))
                               :field-comment ""}))   ;; Default comment placeholder for each field
                          results)]
        (log/info "DESCRIBE query:" query)
        (log/info "DESCRIBE raw results:" results)
        (log/info "Parsed fields:" fields)
        {:name name
         :schema schema
         :fields fields})
      (finally
        (safely-close-connection conn)))))

;; ----------------------------------------------------------------
;; Define a method to describe table foreign keys.
;; Since FlightSQL does not support imported keys, this returns an empty set.
(defmethod driver/describe-table-fks :arrow-flight-sql
  [_ _ _]
  ;; Return an empty set so that foreign key synchronization doesn't fail.
  #{})

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
               [[:inline ""] :field-comment]]
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

(defmethod sql.qp/->honeysql
  ;; inline the two-arg ["absolute-datetime" value _] form
  [:arrow-flight-sql :absolute-datetime]
  [_driver [_ value _options]]
  ;; value is a java.time.LocalDate or LocalDateTime
  (let [fmt (java.time.format.DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss")]
    ;; embed as raw string literal
    [:raw (str "'" (.format value fmt) "'")]))
