(ns sqlingvo.vendor-test
  (:require [clojure.test :refer :all]
            [sqlingvo.vendor :refer :all]))

(deftest test-sql-name
  (are [x expected]
    (is (and (= expected (sql-name (->mysql {}) x))
             (= expected (sql-name (->postgresql {}) x))
             (= expected (sql-name (->vertica {}) x))))
    "" ""
    :a "a"
    :a-1 "a_1"))

(deftest test-sql-keyword
  (are [x expected]
    (is (and (= expected (sql-keyword (->mysql {}) x))
             (= expected (sql-keyword (->postgresql {}) x))
             (= expected (sql-keyword (->vertica {}) x))))
    "" (keyword "")
    :a :a
    :a-1 :a-1
    :a_1 :a-1))

(deftest test-sql-quote
  (are [vendor x expected]
    (is (= expected (sql-quote (vendor {}) x)))
    ->mysql "" "``"
    ->mysql :a "`a`"
    ->mysql :a-1 "`a_1`"
    ->postgresql "" "\"\""
    ->postgresql :a "\"a\""
    ->postgresql :a-1 "\"a_1\""
    ->vertica"" "\"\""
    ->vertica :a "\"a\""
    ->vertica :a-1 "\"a_1\""))
