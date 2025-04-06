(defproject metabase-flightsql-driver "0.1.0-SNAPSHOT"
  :description "A Clojure library that enables Metabase to connect to databases using the Apache Arrow Flight SQL JDBC driver, delivering enhanced performance and advanced SQL querying capabilities."
  :url "https://github.com/J0hnG4lt/metabase-flightsql-driver"
  :license {:name "Apache License 2.0 / EPL-2.0 or GPL-2.0-or-later with Classpath-exception-2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.apache.arrow/flight-sql-jdbc-driver "18.2.0"]]
  :repl-options {:init-ns metabase.driver.flightsql})
