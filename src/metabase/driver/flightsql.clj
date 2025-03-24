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
   [metabase.util.log :as log]))

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
       ;; Set default connection parameters for Flight SQL.
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
                              (str "/?" query-params))))}
       ;; Remove keys that we don’t want passed along as connection properties.
       (dissoc details :host :port))
      ;; Let Metabase process any additional options.
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
