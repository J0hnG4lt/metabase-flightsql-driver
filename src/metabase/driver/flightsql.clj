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
   [metabase.driver :as driver]
   [metabase.driver.common :as driver.common]
   [metabase.driver.sql-jdbc :as sql-jdbc]
   [metabase.driver.sql-jdbc.common :as sql-jdbc.common]
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
   [metabase.util :as u]
   [metabase.util.log :as log]))

;; Register this driver as a JDBC-based driver with parent :sql-jdbc.
(driver/register! :arrow-flight-sql, :parent #{:sql-jdbc})

;; Implement a multimethod to provide a human-friendly display name.
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

;; Build the connection spec from the provided details.
;; Expected details keys: :host, :port, :user, :password, :token, and optionally :useEncryption.
(defmethod sql-jdbc.conn/connection-details->spec :arrow-flight-sql
  [_ details]
  (let [host         (or (:host details) "localhost")
        port         (or (:port details) 443)
        ;; Build query parameters from available authentication options.
        query-params (->> {:user      (:user details)
                           :password      (:password details)
                           :token         (:token details)
                           :useEncryption (if (contains? details :useEncryption)
                                            (:useEncryption details)
                                            true)}
                          (filter (comp some? second))
                          (map (fn [[k v]]
                                 (str (name k) "=" (u/url-encode (str v)))))
                          (str/join "&"))
        conn-uri     (str "jdbc:arrow-flight-sql://" host ":" port
                          (when (not (str/blank? query-params))
                            (str "/?" query-params)))]
    {:classname   "org.apache.arrow.flight.jdbc.FlightSqlDriver"  ;; Adjust if the driver classname changes.
     :subprotocol "arrow-flight-sql"
     :subname     conn-uri}))

;; Provide a human-friendly connection error message.
(defmethod driver/humanize-connection-error-message :arrow-flight-sql
  [_ message]
  (if (re-find #"Authentication failed" message)
    "Authentication failed. Please check your Flight SQL credentials."
    message))

;; Test the connection by issuing a simple query.
(defmethod driver/can-connect? :arrow-flight-sql
  [driver details]
  (try
    (sql-jdbc.conn/with-connection-spec-for-testing-connection [spec [driver details]]
      ;; A simple test query – adjust if necessary.
      (jdbc/query spec "SELECT 1"))
    true
    (catch Exception e
      (log/error e "Flight SQL connection test failed.")
      false)))

;; Delegate database and table description to the SQL-JDBC implementation.
(defmethod driver/describe-database :arrow-flight-sql
  [driver database]
  (sql-jdbc/describe-database driver database))

(defmethod driver/describe-table :arrow-flight-sql
  [driver database table]
  (sql-jdbc/describe-table driver database table))

;; Optionally, you can add additional methods for current datetime, query processing,
;; or other Flight SQL–specific behavior as needed.
