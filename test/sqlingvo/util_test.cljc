(ns sqlingvo.util-test
  (:require [clojure.test :refer [are deftest is]]
            [sqlingvo.db :as db]
            [sqlingvo.test :refer [db]]
            [sqlingvo.util :as util]))

(deftest test-keyword-str
  (are [k expected] (= (util/keyword-str k) expected)
    nil nil
    :x "x"
    :x.y "x.y"
    :x/y "x/y"))

(deftest test-sql-type
  (are [type expected] (= (util/sql-type-name type) expected)
    nil nil
    :smallint "SMALLINT"
    :double-precision "DOUBLE PRECISION"))

(deftest test-sql-quote-backtick
  (are [x expected]
      (= expected (util/sql-quote-backtick x))
    nil nil
    :continents "`continents`"
    :continents.name "`continents`.`name`"
    :continents.* "`continents`.*"
    :EXCLUDED.dname "EXCLUDED.`dname`"))

(deftest test-sql-quote-double-quote
  (are [x expected]
      (= expected (util/sql-quote-double-quote x))
    nil nil
    :continents "\"continents\""
    :continents.name "\"continents\".\"name\""
    :continents.* "\"continents\".*"
    :EXCLUDED.dname "EXCLUDED.\"dname\""))

(deftest test-sql-name
  (are [x expected]
      (and (= expected (util/sql-name (db/db :mysql) x))
           (= expected (util/sql-name (db/db :postgresql) x))
           (= expected (util/sql-name (db/db :vertica) x)))
    nil nil
    "" ""
    "country/id" "country/id"
    :a "a"
    :a-1 "a-1"))

(deftest test-sql-keyword
  (are [x expected]
      (and (= expected (util/sql-keyword (db/db :mysql) x))
           (= expected (util/sql-keyword (db/db :postgresql) x))
           (= expected (util/sql-keyword (db/db :vertica) x)))
    nil nil
    "" (keyword "")
    "country/id" :country/id
    :a :a
    :a-1 :a-1
    :a_1 :a_1))

(deftest test-sql-quote
  (are [scheme x expected]
      (= expected (util/sql-quote (db/db scheme) x))
    :mysql nil nil
    :mysql "" "``"
    :mysql :a "`a`"
    :mysql :a-1 "`a-1`"
    :postgresql "" "\"\""
    :postgresql "country/id" "\"country/id\""
    :postgresql :a "\"a\""
    :postgresql :a-1 "\"a-1\""
    :postgresql :EXCLUDED.dname "EXCLUDED.\"dname\""
    :vertica"" "\"\""
    :vertica :a "\"a\""
    :vertica :a-1 "\"a-1\""))

(deftest test-sql-placeholder-constant
  (let [placeholder (util/sql-placeholder-constant)]
    (is (= (placeholder) "?"))
    (is (= (placeholder) "?")))
  (let [placeholder (util/sql-placeholder-constant "$")]
    (is (= (placeholder) "$"))
    (is (= (placeholder) "$"))))

(deftest test-sql-placeholder-count
  (let [placeholder (util/sql-placeholder-count)]
    (is (= (placeholder) "$1"))
    (is (= (placeholder) "$2")))
  (let [placeholder (util/sql-placeholder-count "?")]
    (is (= (placeholder) "?1"))
    (is (= (placeholder) "?2"))))

(deftest test-sql-quote-fn
  (are [x expected]
      (= (util/sql-quote-fn db x) expected)
    "" "\"\""
    "_" "_"
    "x" "x"
    "1" "\"1\""
    "a b" "\"a b\""))
