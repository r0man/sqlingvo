(ns sqlingvo.explain-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [sqlingvo.core :refer :all]
            [sqlingvo.test :refer [db sql=]]))

(deftest test-explain
  (sql= (explain db
          (select db [:*]
            (from :foo)))
        ["EXPLAIN SELECT * FROM \"foo\""]))

(deftest test-explain-boolean-options
  (doseq [option [:analyze :buffers :costs :timing :verbose]
          value [true false]]
    (sql= (explain db
            (select db [:*]
              (from :foo))
            {option value})
          [(format "EXPLAIN (%s %s) SELECT * FROM \"foo\""
                   (str/upper-case (name option))
                   (str/upper-case (str value)))])))

(deftest test-explain-multiple-options
  (sql= (explain db
          (select db [:*]
            (from :foo))
          {:analyze true
           :verbose true})
        ["EXPLAIN (ANALYZE TRUE, VERBOSE TRUE) SELECT * FROM \"foo\""]))

(deftest test-explain-format
  (doseq [value [:text :xml :json :yaml]]
    (sql= (explain db
            (select db [:*]
              (from :foo))
            {:format value})
          [(format "EXPLAIN (FORMAT %s) SELECT * FROM \"foo\""
                   (str/upper-case (name value)))])))
