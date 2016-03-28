(ns sqlingvo.util-test
  (:require [clojure.test :refer :all]
            [sqlingvo.db :as db]
            [sqlingvo.test :refer [db]]
            [sqlingvo.util :refer :all]))

(deftest test-sql-quote-backtick
  (are [x expected]
      (= expected (sql-quote-backtick x))
    nil nil
    :continents "`continents`"
    :continents.name "`continents`.`name`"
    :continents.* "`continents`.*"
    :EXCLUDED.dname "EXCLUDED.`dname`"))

(deftest test-sql-quote-double-quote
  (are [x expected]
      (= expected (sql-quote-double-quote x))
    nil nil
    :continents "\"continents\""
    :continents.name "\"continents\".\"name\""
    :continents.* "\"continents\".*"
    :EXCLUDED.dname "EXCLUDED.\"dname\""))

(deftest test-sql-name
  (are [x expected]
      (and (= expected (sql-name (db/mysql) x))
           (= expected (sql-name (db/postgresql) x))
           (= expected (sql-name (db/vertica) x)))
    nil nil
    "" ""
    :a "a"
    :a-1 "a-1"))

(deftest test-sql-keyword
  (are [x expected]
      (and (= expected (sql-keyword (db/mysql) x))
           (= expected (sql-keyword (db/postgresql) x))
           (= expected (sql-keyword (db/vertica) x)))
    nil nil
    "" (keyword "")
    :a :a
    :a-1 :a-1
    :a_1 :a_1))

(deftest test-sql-quote
  (are [db x expected]
      (= expected (sql-quote (db) x))
    db/mysql nil nil
    db/mysql "" "``"
    db/mysql :a "`a`"
    db/mysql :a-1 "`a-1`"
    db/postgresql "" "\"\""
    db/postgresql :a "\"a\""
    db/postgresql :a-1 "\"a-1\""
    db/postgresql :EXCLUDED.dname "EXCLUDED.\"dname\""
    db/vertica"" "\"\""
    db/vertica :a "\"a\""
    db/vertica :a-1 "\"a-1\""))

(deftest test-sql-placeholder-constant
  (let [placeholder (sql-placeholder-constant)]
    (is (= (placeholder) "?"))
    (is (= (placeholder) "?")))
  (let [placeholder (sql-placeholder-constant "$")]
    (is (= (placeholder) "$"))
    (is (= (placeholder) "$"))))

(deftest test-sql-placeholder-count
  (let [placeholder (sql-placeholder-count)]
    (is (= (placeholder) "$1"))
    (is (= (placeholder) "$2")))
  (let [placeholder (sql-placeholder-count "?")]
    (is (= (placeholder) "?1"))
    (is (= (placeholder) "?2"))))

(deftest test-sql-quote-fn
  (are [x expected]
      (= (sql-quote-fn db x) expected)
    "" "\"\""
    "_" "_"
    "x" "x"
    "1" "\"1\""
    "a b" "\"a b\""))
