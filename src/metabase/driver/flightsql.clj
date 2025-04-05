(ns metabase.driver.flightsql
  "Arrow Flight SQL Driver for Metabase.

  This driver uses the Apache Arrow Flight SQL JDBC driver.
  The connection URI is built following the format:

      jdbc:arrow-flight-sql://HOST:PORT[/?param1=val1&param2=val2&...]

  Supported parameters include:

    - user: the user for authentication
    - password: the password for authentication
    - token: an optional token for authentication
    - useEncryption: whether to use TLS (default true)

  For more details see the Flight SQL JDBC Driver documentation."
  (:require
   [clojure.string :as str]
   [clojure.java.jdbc :as jdbc]
   [ring.util.codec :as codec]                        ;; Use Ring’s codec for URL encoding
   [metabase.driver :as driver]
   [metabase.driver.common :as driver.common]
   [metabase.driver.sql-jdbc :as sql-jdbc]
   [metabase.driver.sql-jdbc.common :as sql-jdbc.common]
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
   [metabase.util.log :as log]
   [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
   [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
   )
  )

;; Register this driver as a JDBC-based driver with parent :sql-jdbc.
(driver/register! :arrow-flight-sql, :parent #{:sql-jdbc})

;; Provide a human-friendly display name.
(defmethod driver/display-name :arrow-flight-sql [_]
  "Arrow Flight SQL")

;; Define supported features.
(doseq [[feature supported?]
        {:describe-fields           true
         :connection-impersonation  false
         :convert-timezone          true}]
  (defmethod driver/database-supports? [:arrow-flight-sql feature]
    [_driver _feature _db]
    supported?))

(defn non-blank [s default]
  (if (and (string? s) (not (str/blank? s)))
    s
    default))

(defmethod sql-jdbc.conn/connection-details->spec :arrow-flight-sql
  [_ details]
  (-> (merge
        ;; Default connection parameters for Flight SQL
       {:classname   "org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver"
        :subprotocol "arrow-flight-sql"
        :subname     (let [host (if (and (string? (:host details))
                                         (not (str/blank? (:host details))))
                                  (:host details)
                                  "localhost")
                           port (if (or (nil? (:port details))
                                        (and (string? (:port details))
                                             (str/blank? (:port details))))
                                  443
                                  (:port details))
                           query-params (->> {:user          (:user details)
                                              :password      (:password details)
                                              :token         (:token details)
                                              :useEncryption (if (contains? details :useEncryption)
                                                               (:useEncryption details)
                                                               true)}
                                             (filter (comp some? second))
                                             (map (fn [[k v]]
                                                    (str (name k) "=" (codec/url-encode (str v)))))
                                             (str/join "&"))]
                       (str "//" host ":" port
                            (when (not (str/blank? query-params))
                              (str "/?" query-params))))
         ;; Add casting logic to prevent Timestamp → LocalDateTime class cast errors
        :cast        (fn [col val]
                       (cond
                         (and (= (:base-type col) :type/DateTime)
                              (instance? java.sql.Timestamp val))
                         (.toLocalDateTime ^java.sql.Timestamp val)

                         :else val))}
        ;; Clean up any internal config keys
       (dissoc details :host :port))
      ;; Process other options
      (sql-jdbc.common/handle-additional-options details)))




;; Example implementation for testing the connection.
(defmethod driver/can-connect? :arrow-flight-sql
  [driver details]
  (try
    (sql-jdbc.conn/with-connection-spec-for-testing-connection [spec [driver details]]
      (jdbc/query spec "SELECT 1"))
    true
    (catch Exception e
      (log/error e "Flight SQL connection test failed.")
      false)))

;; Additional methods (describe-database, describe-table, etc.) can remain unchanged.


(defmethod sql-jdbc.sync/database-type->base-type :arrow-flight-sql
  [_driver base-type]
  (let [normalized (-> base-type
                       str
                       str/upper-case
                       keyword)]
    (case normalized
      ;; Boolean
      :BOOLEAN            :type/Boolean

      ;; Integers
      :INT8              :type/Integer
      :INT16             :type/Integer
      :INT32             :type/Integer
      :INT64             :type/BigInteger
      :UINT8             :type/Integer
      :UINT16            :type/Integer
      :UINT32            :type/BigInteger
      :UINT64            :type/BigInteger

      ;; Floating point
      :FLOAT16           :type/Float
      :FLOAT32           :type/Float
      :FLOAT64           :type/Float

      ;; Decimal
      :DECIMAL128        :type/Decimal
      :DECIMAL256        :type/Decimal

      ;; Time and date
      :DATE32            :type/Date
      :TIME32            :type/Time
      :TIME64            :type/Time
      :TIMESTAMP         :type/DateTime
      :TIMESTAMP_MILLISECOND :type/DateTime
      :TIMESTAMP_MICROSECOND :type/DateTime
      :TIMESTAMP_NANOSECOND  :type/DateTime

      ;; Strings and binary
      :UTF8              :type/Text
      :BINARY            :type/*
      :FIXED_SIZE_BINARY :type/*
      :INTERVAL          :type/*

      ;; Fallback
      :type/*)))



(defmethod sql-jdbc.execute/read-column-thunk [:arrow-flight-sql java.sql.Types/TIMESTAMP]
  [_driver ^java.sql.ResultSet rs _rsmeta ^Integer i]
  (fn []
    (some-> (.getTimestamp rs i)
            .toLocalDateTime)))
