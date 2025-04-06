(ns metabase.driver.flightsql
  "Arrow Flight SQL Driver for Metabase.

  This driver uses the Apache Arrow Flight SQL JDBC driver.
  ...
  "
  (:require
   ;; String manipulation functions.
   [clojure.string :as str]
   ;; JDBC library for database connectivity.
   [clojure.java.jdbc :as jdbc]
   ;; URL encoding/decoding utilities.
   [ring.util.codec :as codec]
   ;; Core Metabase driver functionality.
   [metabase.driver :as driver]
   ;; Common functions for Metabase drivers.
   [metabase.driver.common :as driver.common]
   ;; SQL generation and manipulation.
   [honey.sql :as sql]
   ;; Metabase SQL-JDBC integration.
   [metabase.driver.sql-jdbc :as sql-jdbc]
   ;; Common functions for SQL-JDBC drivers.
   [metabase.driver.sql-jdbc.common :as sql-jdbc.common]
   ;; Connection management for SQL-JDBC drivers.
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
   ;; Logging utilities.
   [metabase.util.log :as log]
   ;; SQL query processing.
   [metabase.driver.sql.query-processor :as sql.qp]
   ;; Schema synchronization for SQL-JDBC drivers.
   [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
   ;; SQL execution helper functions.
   [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]))

;; ----------------------------------------------------------------
;; Register this driver as a JDBC-based driver with parent :sql-jdbc.
(driver/register! :arrow-flight-sql, :parent #{:sql-jdbc})

;; ----------------------------------------------------------------
;; Define the display name for the Arrow Flight SQL driver.
(defmethod driver/display-name :arrow-flight-sql [_]
  "Arrow Flight SQL")

;; ----------------------------------------------------------------
;; Register feature support flags for the driver.
;; This loop defines which features the driver supports.
(doseq [[feature supported?]
        {:describe-fields           true
         :connection-impersonation  false
         :convert-timezone          true}]
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
(defmethod sql-jdbc.conn/connection-details->spec :arrow-flight-sql
  [_ details]
  (-> (merge
       {:classname   "org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver"  ;; JDBC driver class name
        :subprotocol "arrow-flight-sql"                                    ;; JDBC subprotocol identifier
        :subname     (let [host (if (and (string? (:host details))
                                         (not (str/blank? (:host details))))
                                  (:host details)
                                  "localhost")                              ;; Default host is "localhost"
                           port (if (or (nil? (:port details))
                                        (and (string? (:port details))
                                             (str/blank? (:port details))))
                                  443                                       ;; Default port is 443
                                  (:port details))
                           query-params (->> {:user          (:user details)
                                              :password      (:password details)
                                              :token         (:token details)
                                              :useEncryption (if (contains? details :useEncryption)
                                                               (:useEncryption details)
                                                               true)}
                                             (filter (comp some? second))              ;; Filter out nil values
                                             (map (fn [[k v]]
                                                    (str (name k) "=" (codec/url-encode (str v))))) ;; URL encode parameters
                                             (str/join "&"))]
                       (str "//" host ":" port
                            (when (not (str/blank? query-params))
                              (str "/?" query-params))))              ;; Construct subname with optional query string
        :cast        (fn [col val]
                       (cond
                         (and (= (:base-type col) :type/DateTime)
                              (instance? java.sql.Timestamp val))
                         (.toLocalDateTime ^java.sql.Timestamp val)  ;; Convert SQL Timestamp to LocalDateTime
                         :else val))}                                  ;; Return value unchanged for other types
       (dissoc details :host :port))
      (sql-jdbc.common/handle-additional-options details)))           ;; Apply additional options if provided

;; ----------------------------------------------------------------
;; Test the connection to the Arrow Flight SQL database.
;; Executes a simple "SELECT 1" query to verify connectivity.
(defmethod driver/can-connect? :arrow-flight-sql
  [driver details]
  (try
    (sql-jdbc.conn/with-connection-spec-for-testing-connection [spec [driver details]]
      (jdbc/query spec "SELECT 1"))
    true
    (catch Exception e
      (log/error e "Flight SQL connection test failed.")
      false)))

;; ----------------------------------------------------------------
;; Map raw database types to Metabase base types.
;; This converts a database-specific type into a standardized Metabase type.
(defmethod sql-jdbc.sync/database-type->base-type :arrow-flight-sql
  [_driver base-type]
  (let [normalized (-> base-type str str/upper-case keyword)]
    (case normalized
      :BOOLEAN            :type/Boolean
      :INT8               :type/Integer
      :INT16              :type/Integer
      :INT32              :type/Integer
      :INT64              :type/BigInteger
      :UINT8              :type/Integer
      :UINT16             :type/Integer
      :UINT32             :type/BigInteger
      :UINT64             :type/BigInteger
      :FLOAT16            :type/Float
      :FLOAT32            :type/Float
      :FLOAT64            :type/Float
      :DECIMAL128         :type/Decimal
      :DECIMAL256         :type/Decimal
      :DATE32             :type/Date
      :TIME32             :type/Time
      :TIME64             :type/Time
      :TIMESTAMP          :type/DateTime
      :TIMESTAMP_MILLISECOND :type/DateTime
      :TIMESTAMP_MICROSECOND :type/DateTime
      :TIMESTAMP_NANOSECOND  :type/DateTime
      :UTF8               :type/Text
      :BINARY             :type/*
      :FIXED_SIZE_BINARY  :type/*
      :INTERVAL           :type/*
      :type/*))) ;; Default type if none of the cases match

;; ----------------------------------------------------------------
;; Define a reader function for TIMESTAMP columns.
;; Retrieves a timestamp from the ResultSet and converts it to a local date-time.
(defmethod sql-jdbc.execute/read-column-thunk [:arrow-flight-sql java.sql.Types/TIMESTAMP]
  [_driver ^java.sql.ResultSet rs _rsmeta ^Integer i]
  (fn []
    (some-> (.getTimestamp rs i)
            .toLocalDateTime)))

;; ----------------------------------------------------------------
;; Custom Schema Sync Implementations
;; ----------------------------------------------------------------

;; List tables by executing the "SHOW TABLES" command.
(defmethod driver/describe-database :arrow-flight-sql
  [driver database]
  (let [spec (sql-jdbc.conn/connection-details->spec :arrow-flight-sql (:details database))]
    (with-open [conn (jdbc/get-connection spec)]
      (let [rows (jdbc/query {:connection conn}
                             ["SHOW TABLES"]
                             {:identifiers str/lower-case})
            formatted (->> rows
                           (filter #(not= (str/lower-case (:table_schema %)) "information_schema"))
                           (map (fn [row]
                                  {:name   (:table_name row)
                                   :schema (:table_schema row)})))]
        {:tables (into #{} formatted)})))) ;; Return a set of formatted table information

;; ----------------------------------------------------------------
;; Describe a specific table by executing a DESCRIBE query.
(defmethod driver/describe-table :arrow-flight-sql
  [_ driver database {:keys [name schema]}]
  (let [spec (sql-jdbc.conn/connection-details->spec :arrow-flight-sql (:details database))]
    (with-open [conn (jdbc/get-connection spec)]
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
         :fields fields}))))

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
(defmethod sql-jdbc.sync/describe-fields-sql :arrow-flight-sql
  [driver & {:keys [schema-names table-names details]}]
  (let [base-condition [:>= [:inline 1] [:inline 1]]
        schema-condition (when (seq schema-names)
                           [:in [:lower :table_schema]
                            (mapv (fn [s] [:inline (str/lower-case s)]) schema-names)])
        table-condition (when (seq table-names)
                          [:in [:lower :table_name]
                           (mapv (fn [t] [:inline (str/lower-case t)]) table-names)])
        where-clause (cond-> [base-condition]
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
