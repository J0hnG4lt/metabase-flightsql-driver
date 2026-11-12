(ns metabase.driver.arrow-flight-sql-catalog-test
  "Catalog qualification regressions; no Flight SQL server required."
  (:require
   [clojure.java.jdbc :as jdbc]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [honey.sql :as sql]
   [metabase.driver :as driver]
   [metabase.driver-api.core :as driver-api]
   [metabase.driver.arrow-flight-sql]
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
   [metabase.driver.sql.query-processor :as sql.qp]
   [metabase.util.honey-sql-2 :as h2x]))

(defn- table-sql [catalog table]
  (with-redefs [driver-api/metadata-provider (constantly ::provider)
                driver-api/database (constantly {:details {:catalog catalog}})]
    (sql/format {:select [:*]
                 :from [[(sql.qp/->honeysql :arrow-flight-sql
                                         (merge {:lib/type :metadata/table
                                                 :schema "bronze" :name "stations"}
                                                table))]]}
                :quoted true)))

(deftest configured-catalog-qualifies-table-test
  (is (= ["SELECT * FROM \"lake\".\"bronze\".\"stations\""]
         (table-sql "lake" {})))
  (testing "an explicit table database takes precedence"
    (is (= ["SELECT * FROM \"other\".\"bronze\".\"stations\""]
           (table-sql "lake" {:db "other"}))))
  (testing "a blank explicit database falls back to the connection catalog"
    (is (= (table-sql "lake" {}) (table-sql "lake" {:db "  "})))))

(deftest blank-catalog-preserves-unqualified-table-test
  (doseq [catalog [nil "" "  "]]
    (is (= ["SELECT * FROM \"bronze\".\"stations\""]
           (table-sql catalog {}))))
  (is (= ["SELECT * FROM \"lake\".\"stations\""]
         (table-sql "lake" {:schema nil}))))

(deftest catalog-components-are-quoted-separately-test
  (is (= ["SELECT * FROM \"lake.prod\".\"bronze\"\"quoted\".\"weather.stations\""]
         (table-sql "lake.prod" {:schema "bronze\"quoted" :name "weather.stations"}))))

(deftest joined-tables-are-qualified-without-changing-aliases-test
  (with-redefs [driver-api/metadata-provider (constantly ::provider)
                driver-api/database (constantly {:details {:catalog "lake"}})]
    (let [physical-table (fn [name]
                           (sql.qp/->honeysql :arrow-flight-sql
                                             {:lib/type :metadata/table
                                              :schema "bronze" :name name}))
          station-id (h2x/identifier :field "Stations" "id")
          [query] (sql/format
                   {:select [[station-id]]
                    :from [[(physical-table "stations")
                            [(h2x/identifier :table-alias "Stations")]]]
                    :left-join [[(physical-table "sensors")
                                 [(h2x/identifier :table-alias "Sensors")]]
                                [:= station-id (h2x/identifier :field "Sensors" "station_id")]]}
                   :quoted true)]
      (is (= (str "SELECT \"Stations\".\"id\" FROM \"lake\".\"bronze\".\"stations\" AS \"Stations\""
                  " LEFT JOIN \"lake\".\"bronze\".\"sensors\" AS \"Sensors\""
                  " ON \"Stations\".\"id\" = \"Sensors\".\"station_id\"")
             query)))))

(deftest nested-table-is-qualified-test
  (with-redefs [driver-api/metadata-provider (constantly ::provider)
                driver-api/database (constantly {:details {:catalog "lake"}})]
    (is (= [(str "SELECT * FROM (SELECT * FROM \"lake\".\"bronze\".\"stations\") AS \"source\"")]
           (sql/format {:select [:*]
                        :from [[{:select [:*]
                                 :from [[(sql.qp/->honeysql :arrow-flight-sql
                                                         {:lib/type :metadata/table
                                                          :schema "bronze" :name "stations"})]]}
                                [(h2x/identifier :table-alias "source")]]]}
                       :quoted true)))))

(deftest describe-table-scopes-catalog-and-escapes-literals-test
  (doseq [catalog ["lake" "lake'prod" nil "" "  "]]
    (let [captured (atom nil)
          closed? (atom false)
          database {:details {:catalog catalog}}
          table {:schema "bronze'quoted" :name "station's"}]
      (with-redefs [sql-jdbc.conn/connection-details->spec (constantly {})
                    jdbc/get-connection (constantly (reify java.io.Closeable
                                                     (close [_] (reset! closed? true))))
                    jdbc/query (fn [_ sql+args _]
                                 (reset! captured sql+args)
                                 [{:column_name "id" :data_type "INTEGER"}])]
        (is (= #{"id"} (set (map :name (:fields (driver/describe-table :arrow-flight-sql database table))))))
        (let [[query & params] @captured]
          (is (empty? params) "metadata filters must not rely on unsupported bound parameters")
          (is (str/includes? query "'bronze''quoted'"))
          (is (str/includes? query "'station''s'"))
          (if (str/blank? catalog)
            (is (not (str/includes? query "table_catalog")))
            (do
              (is (str/includes? query "table_catalog"))
              (is (str/includes? query (str "'" (str/replace catalog "'" "''") "'"))))))
        (is @closed?)))))

(deftest catalog-is-not-sent-as-a-session-option-test
  (is (not (str/includes?
            (:subname (sql-jdbc.conn/connection-details->spec
                       :arrow-flight-sql {:host "localhost" :port 1 :catalog "lake"}))
            "catalog="))))
