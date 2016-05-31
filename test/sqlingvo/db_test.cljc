(ns sqlingvo.db-test
  (:require #?(:clj [clojure.test :refer :all]
               :cljs [cljs.test :refer-macros [are deftest is]])
            [sqlingvo.compiler :as compiler]
            [sqlingvo.db :as db]
            [sqlingvo.util :as util]))

(deftest test-db
  (is (= (db/db {:subprotocol "postgresql"})
         (db/postgresql)))
  (is (thrown? #?(:clj Exception :cljs js/Error) (db/db nil))))

(deftest test-postgresql
  (let [db (db/postgresql)]
    (is (instance? sqlingvo.db.Database db))
    (is (= (:classname db) "org.postgresql.Driver"))
    (is (= (:doc db) "The world's most advanced open source database."))
    (is (= (:eval-fn db) compiler/compile-stmt))
    (is (= (:subprotocol db) "postgresql"))
    (is (= (:sql-name db) nil))
    (is (= (:sql-name db) nil))
    (is (= (:sql-quote db) util/sql-quote-double-quote))))
