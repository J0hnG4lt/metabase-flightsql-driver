(ns ^:mb/driver-tests metabase.driver.arrow-flight-sql-test
  "Unit tests (no server needed) for the connection spec builder and detail
  normalization, plus harness-driven integration tests against the GizmoSQL
  test server (MB_ARROW_FLIGHT_SQL_TEST_* env vars)."
  (:require
   [clojure.test :refer [deftest is testing]]
   [metabase.driver :as driver]
   [metabase.driver.arrow-flight-sql]
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
   [metabase.test :as mt]))

(set! *warn-on-reflection* true)

(comment metabase.driver.arrow-flight-sql/keep-me)

;;; ------------------------------ unit tests ------------------------------

(defn- subname [details]
  (:subname (sql-jdbc.conn/connection-details->spec :arrow-flight-sql details)))

(deftest connection-spec-auth-modes-test
  (testing "username/password handshake"
    (is (re-find #"user=alice&password=s3cret"
                 (subname {:host "h" :port 1 :use-token false
                           :username "alice" :password "s3cret"}))))
  (testing "blank username + password (Spice.ai API-key convention)"
    (is (re-find #"user=&password=apikey"
                 (subname {:host "h" :port 1 :use-token false :password "apikey"}))))
  (testing "username with EMPTY password (Doris/StarRocks root) still sends user"
    (let [sn (subname {:host "h" :port 1 :use-token false :username "root" :password ""})]
      (is (re-find #"user=root" sn))
      (is (re-find #"password=" sn))
      (is (not (re-find #"authorization" sn)))))
  (testing "bearer token"
    (is (re-find #"authorization=Bearer%20tok"
                 (subname {:host "h" :port 1 :use-token true :token "tok"}))))
  (testing "toggle off ignores a stale stored token"
    (let [sn (subname {:host "h" :port 1 :use-token false :token "stale"
                       :username "u" :password "p"})]
      (is (not (re-find #"authorization" sn)))
      (is (re-find #"user=u&password=p" sn))))
  (testing "legacy details without the toggle infer token mode"
    (is (re-find #"authorization=Bearer%20tok"
                 (subname {:host "h" :port 1 :token "tok"}))))
  (testing "anonymous emits no credentials"
    (let [sn (subname {:host "h" :port 1 :use-token false})]
      (is (not (re-find #"user=" sn)))
      (is (not (re-find #"authorization" sn))))))

(deftest connection-spec-options-test
  (testing "secure defaults"
    (let [sn (subname {:host "h" :port 1})]
      (is (re-find #"useEncryption=true" sn))
      (is (re-find #"disableCertificateVerification=false" sn))))
  (testing "additional options are appended verbatim"
    (is (re-find #"threadPoolSize=2&retainAuth=true"
                 (subname {:host "h" :port 1
                           :additional-options "threadPoolSize=2&retainAuth=true"}))))
  (testing "connect timeout"
    (is (re-find #"connectTimeoutMillis=5000"
                 (subname {:host "h" :port 1 :connect-timeout-millis 5000}))))
  (testing "credentials are URL-encoded"
    (is (re-find #"password=p%40ss%26word"
                 (subname {:host "h" :port 1 :username "u" :password "p@ss&word"})))))

(deftest normalize-db-details-backfill-test
  (let [normalize (fn [details]
                    (:details (driver/normalize-db-details :arrow-flight-sql
                                                           {:details details})))]
    (testing "token-shaped legacy details backfill use-token true"
      (is (true? (:use-token (normalize {:token "t"}))))
      (is (true? (:use-token (normalize {:token-id 5}))))
      (is (true? (:use-token (normalize {:token-value "t"})))))
    (testing "user/password legacy details backfill use-token false"
      (is (false? (:use-token (normalize {:username "u" :password "p"})))))
    (testing "already-normalized details are untouched"
      (is (= {:use-token false :username "u"}
             (normalize {:use-token false :username "u"}))))))

;;; --------------------------- integration tests --------------------------

(deftest ^:parallel basic-aggregation-test
  (mt/test-driver :arrow-flight-sql
    (testing "COUNT over the synced test dataset"
      (is (= [[100]]
             (mt/formatted-rows [int]
               (mt/run-mbql-query venues
                 {:aggregation [[:count]]})))))))

(deftest ^:parallel basic-select-test
  (mt/test-driver :arrow-flight-sql
    (testing "simple projection with limit and order-by"
      (is (= [[1 "Red Medicine"]]
             (mt/formatted-rows [int str]
               (mt/run-mbql-query venues
                 {:fields   [$id $name]
                  :order-by [[:asc $id]]
                  :limit    1})))))))

(deftest ^:parallel filter-and-breakout-test
  (mt/test-driver :arrow-flight-sql
    (testing "filter + breakout + aggregation compiles and runs"
      (is (= [[2 59] [3 13]]
             (mt/formatted-rows [int int]
               (mt/run-mbql-query venues
                 {:aggregation [[:count]]
                  :breakout    [$price]
                  :filter      [:between $price 2 3]
                  :order-by    [[:asc $price]]
                  :limit       2})))))))
