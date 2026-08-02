(defproject metabase-flightsql-driver "0.1.0-SNAPSHOT"
  :description "A Clojure library that enables Metabase to connect to databases using the Apache Arrow Flight SQL JDBC driver, delivering enhanced performance and advanced SQL querying capabilities."
  :url "https://github.com/J0hnG4lt/metabase-flightsql-driver"
  :license {:name "Apache-2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0"}
  ;; Clojure 1.12.3 matches Metabase 0.63's runtime; Arrow Flight SQL JDBC
  ;; 19.0.0 is the latest apache/arrow-java release (see deps.edn for details).
  :dependencies [[org.clojure/clojure "1.12.3"]
                 [org.apache.arrow/flight-sql-jdbc-driver "19.0.0"]]
  :repl-options {:init-ns metabase.driver.arrow-flight-sql})
