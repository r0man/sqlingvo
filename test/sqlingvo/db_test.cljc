(ns sqlingvo.db-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :refer [are deftest is]]
            [sqlingvo.compiler :as compiler]
            [sqlingvo.db :as db]
            [sqlingvo.util :as util]))

(deftest test-db-invalid
  (are [spec] (thrown? #?(:clj Exception :cljs js/Error) (db/db spec))
    nil "" "invalid" 1))

(deftest test-db-keyword
  (let [db (db/db :postgresql)]
    (is (s/valid? ::db/db db))
    (is (instance? sqlingvo.db.Database db))
    (is (= (:classname db) "org.postgresql.Driver"))
    (is (= (:eval-fn db) compiler/compile-stmt))
    (is (= (:scheme db) :postgresql))
    (is (= (:sql-name db) nil))
    (is (= (:sql-quote db) util/sql-quote-double-quote))))

(deftest test-db-map-scheme
  (is (= (db/db {:scheme :postgresql})
         (db/db {:scheme "postgresql"})
         (db/db :postgresql))))

(deftest test-db-url
  (let [db (db/db "postgresql://tiger:scotch@localhost/sqlingvo?a=1&b=2")]
    (is (instance? sqlingvo.db.Database db))
    (is (= (:classname db) "org.postgresql.Driver"))
    (is (= (:eval-fn db) compiler/compile-stmt))
    (is (= (:name db) "sqlingvo"))
    (is (= (:password db) "scotch"))
    (is (= (:query-params db) {:a "1" :b "2"}))
    (is (= (:scheme db) :postgresql))
    (is (= (:server-name db) "localhost" ))
    (is (= (:sql-name db) nil))
    (is (= (:sql-quote db) util/sql-quote-double-quote))
    (is (= (:username db) "tiger"))
    (is (nil? (:server-port db)))))

(deftest test-db-idempotent
  (let [db (db/db :postgresql {:eval-fn identity})]
    (is (= (:eval-fn (db/db db)) identity))))
