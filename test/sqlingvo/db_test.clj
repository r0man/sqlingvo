(ns sqlingvo.db-test
  (:require [clojure.test :refer :all]
            [sqlingvo.compiler :as compiler]
            [sqlingvo.db :refer :all]
            [sqlingvo.util :as util]))

(deftest test-db
  (is (= (db {:subprotocol :postgresql})
         (db {:subprotocol "postgresql"})
         (postgresql)))
  (is (thrown? Exception (db nil))))

(deftest test-postgresql
  (let [db (postgresql)]
    (is (instance? sqlingvo.db.Database db))
    (is (= (:classname db) "org.postgresql.Driver"))
    (is (= (:doc db) "The world's most advanced open source database."))
    (is (= (:eval-fn db) compiler/compile-stmt))
    (is (= (:subprotocol db) "postgresql"))
    (is (= (:sql-name db) nil))
    (is (= (:sql-name db) nil))
    (is (= (:sql-quote db) util/sql-quote-double-quote))))
