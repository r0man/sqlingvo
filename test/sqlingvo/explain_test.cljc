(ns sqlingvo.explain-test
  (:require #?(:clj [sqlingvo.test :refer [db sql=]]
               :cljs [sqlingvo.test :refer [db] :refer-macros [sql=]])
            [clojure.test :refer [deftest is]]
            [clojure.string :as str]
            [sqlingvo.core :as sql]))

(deftest test-explain-keyword-db
  (sql= (sql/explain :postgresql
          (sql/select :postgresql [:*]
            (sql/from :foo)))
        ["EXPLAIN SELECT * FROM \"foo\""]))

(deftest test-explain
  (sql= (sql/explain db
          (sql/select db [:*]
            (sql/from :foo)))
        ["EXPLAIN SELECT * FROM \"foo\""]))

(deftest test-explain-boolean-options
  (doseq [option [:analyze :buffers :costs :timing :verbose]
          value [true false]]
    (sql= (sql/explain db
            (sql/select db [:*]
              (sql/from :foo))
            {option value})
          [(str "EXPLAIN ("
                (str/upper-case (name option))
                " "
                (str/upper-case (str value))
                ") SELECT * FROM \"foo\"")])))

(deftest test-explain-multiple-options
  (sql= (sql/explain db
          (sql/select db [:*]
            (sql/from :foo))
          {:analyze true
           :verbose true})
        ["EXPLAIN (ANALYZE TRUE, VERBOSE TRUE) SELECT * FROM \"foo\""]))

(deftest test-explain-format
  (doseq [value [:text :xml :json :yaml]]
    (sql= (sql/explain db
            (sql/select db [:*]
              (sql/from :foo))
            {:format value})
          [(str "EXPLAIN (FORMAT "
                (str/upper-case (name value))
                ") SELECT * FROM \"foo\"")])))
