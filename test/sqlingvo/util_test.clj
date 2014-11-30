(ns sqlingvo.util-test
  (:require [clojure.test :refer :all]
            [sqlingvo.util :refer :all]))

(deftest test-sql-quote-backtick
  (are [x expected]
    (= expected (sql-quote-backtick x))
    nil ""
    :continents "`continents`"
    :continents.name "`continents`.`name`"
    :continents.* "`continents`.*"))

(deftest test-sql-quote-double-quote
  (are [x expected]
    (= expected (sql-quote-double-quote x))
    nil ""
    :continents "\"continents\""
    :continents.name "\"continents\".\"name\""
    :continents.* "\"continents\".*"))
