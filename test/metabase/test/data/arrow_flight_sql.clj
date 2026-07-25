(ns ^:mb/driver-tests metabase.test.data.arrow-flight-sql
  "Test extensions for running Metabase's shared driver test suite against a
  GizmoSQL (DuckDB-backed) Flight SQL server.

  Modeled on the Vertica extensions: the server hosts a single database, so
  every test dataset loads into the DuckDB `main` schema with
  dataset-qualified table names (`test_data_venues`, ...), and a test-only
  wrapper around describe-database filters tables per dataset to fake
  database isolation (see metabase#40310 for the pattern)."
  (:require
   [clojure.java.jdbc :as jdbc]
   [metabase.driver :as driver]
   [metabase.driver.arrow-flight-sql]
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
   [metabase.test.data.impl :as data.impl]
   [metabase.test.data.interface :as tx]
   [metabase.test.data.sql :as sql.tx]
   [metabase.test.data.sql-jdbc :as sql-jdbc.tx]
   [metabase.test.data.sql-jdbc.execute :as execute]
   [metabase.test.data.sql-jdbc.load-data :as load-data]))

(set! *warn-on-reflection* true)

(comment metabase.driver.arrow-flight-sql/keep-me)

(sql-jdbc.tx/add-test-extensions! :arrow-flight-sql)

;; DuckDB DDL type for each base type the test datasets use
(doseq [[base-type sql-type] {:type/BigInteger     "BIGINT"
                              :type/Boolean        "BOOLEAN"
                              :type/Char           "VARCHAR"
                              :type/Date           "DATE"
                              :type/DateTime       "TIMESTAMP"
                              :type/DateTimeWithTZ "TIMESTAMP WITH TIME ZONE"
                              :type/Decimal        "DECIMAL(24,6)"
                              :type/Float          "DOUBLE"
                              :type/Integer        "INTEGER"
                              :type/Text           "VARCHAR"
                              :type/Time           "TIME"
                              :type/UUID           "UUID"}]
  (defmethod sql.tx/field-base-type->sql-type [:arrow-flight-sql base-type] [_ _] sql-type))

(defmethod sql.tx/pk-sql-type :arrow-flight-sql [& _] "INTEGER")

(defmethod tx/dbdef->connection-details :arrow-flight-sql
  [& _]
  {:host                           (tx/db-test-env-var-or-throw :arrow-flight-sql :host "localhost")
   :port                           (Integer/parseInt (tx/db-test-env-var-or-throw :arrow-flight-sql :port "31337"))
   :username                       (tx/db-test-env-var :arrow-flight-sql :user "gizmosql")
   :password                       (tx/db-test-env-var :arrow-flight-sql :password "gizmosql_password")
   :use-token                      false
   :useEncryption                  false
   :disableCertificateVerification true})

(defn- test-db-spec []
  (sql-jdbc.conn/connection-details->spec :arrow-flight-sql
                                          (tx/dbdef->connection-details :arrow-flight-sql :db nil)))

;; Single-database server: no CREATE/DROP DATABASE; datasets share the `main`
;; schema with dataset-qualified table names.
(defmethod sql.tx/create-db-sql         :arrow-flight-sql [& _] nil)
(defmethod sql.tx/drop-db-if-exists-sql :arrow-flight-sql [& _] nil)

(defmethod sql.tx/qualified-name-components :arrow-flight-sql
  ([_driver _db-name]
   ["memory"])
  ([_driver db-name table-name]
   ["main" (tx/db-qualified-table-name db-name table-name)])
  ([_driver db-name table-name field-name]
   ["main" (tx/db-qualified-table-name db-name table-name) field-name]))

;; Flight SQL executes one statement per request
(defmethod execute/execute-sql! :arrow-flight-sql [& args]
  (apply execute/sequentially-execute-sql! args))

;; DuckDB has no ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY support
;; ("Not implemented Error: No support for that ALTER TABLE option yet!"),
;; same situation as the SQLite test extensions -> skip FK DDL entirely.
(defmethod sql.tx/add-fk-sql :arrow-flight-sql [& _] nil)

;; DuckDB's INTEGER PRIMARY KEY does not auto-increment, so the generated
;; INSERTs must carry explicit id values (the ClickHouse pattern).
(defmethod load-data/row-xform :arrow-flight-sql
  [_driver _dbdef tabledef]
  (load-data/maybe-add-ids-xform tabledef))

;; DuckDB default ordering puts NULLs last for ascending sorts
(defmethod tx/sorts-nil-first? :arrow-flight-sql [_ _] false)

(defmethod tx/dataset-already-loaded? :arrow-flight-sql
  [_driver dbdef]
  (let [tabledef   (first (:table-definitions dbdef))
        table-name (tx/db-qualified-table-name (:database-name dbdef) (:table-name tabledef))]
    (boolean
     (seq (jdbc/query (test-db-spec)
                      ["SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' AND table_name = ?"
                       table-name])))))

(def ^:dynamic *override-describe-database-to-filter-by-db-name?*
  "Fake per-dataset database isolation during tests: sync only the tables whose
  names are qualified by the dataset name. Same pattern as the Vertica test
  extensions (metabase#40310)."
  true)

(defonce ^:private original-describe-database
  (get-method driver/describe-database :arrow-flight-sql))

(defmethod driver/describe-database :arrow-flight-sql
  [driver database]
  (if *override-describe-database-to-filter-by-db-name?*
    (let [result           (original-describe-database driver database)
          physical-db-name (data.impl/database-source-dataset-name database)]
      (update result :tables (fn [tables]
                               (into #{}
                                     (filter #(tx/qualified-by-db-name? physical-db-name (:name %)))
                                     tables))))
    (original-describe-database driver database)))

(defmethod tx/aggregate-column-info :arrow-flight-sql
  ([driver ag-type]
   (merge
    ((get-method tx/aggregate-column-info ::tx/test-extensions) driver ag-type)
    (when (#{:count :cum-count} ag-type)
      {:base_type :type/BigInteger})))

  ([driver ag-type field]
   (merge
    ((get-method tx/aggregate-column-info ::tx/test-extensions) driver ag-type field)
    (when (#{:count :cum-count} ag-type)
      {:base_type :type/BigInteger}))))
