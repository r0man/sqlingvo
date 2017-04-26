(ns sqlingvo.truncate-test
  (:require #?(:clj [sqlingvo.test :refer [db sql=]]
               :cljs [sqlingvo.test :refer [db] :refer-macros [sql=]])
            [clojure.test :refer [deftest is]]
            [sqlingvo.core :as sql]))

(deftest test-truncate-keyword-db
  (sql= (sql/truncate :postgresql [:continents])
        ["TRUNCATE TABLE \"continents\""]))

(deftest test-truncate-continents
  (sql= (sql/truncate db [:continents])
        ["TRUNCATE TABLE \"continents\""]))

(deftest test-truncate-continents-and-countries
  (sql= (sql/truncate db [:continents :countries])
        ["TRUNCATE TABLE \"continents\", \"countries\""]))

(deftest test-truncate-continents-restart-restrict
  (sql= (sql/truncate db [:continents]
          (sql/restart-identity true)
          (sql/restrict true))
        ["TRUNCATE TABLE \"continents\" RESTART IDENTITY RESTRICT"]))

(deftest test-truncate-continents-continue-cascade
  (sql= (sql/truncate db [:continents]
          (sql/continue-identity true)
          (sql/cascade true))
        ["TRUNCATE TABLE \"continents\" CONTINUE IDENTITY CASCADE"]))

(deftest test-truncate-continue-identity
  (sql= (sql/truncate db [:continents]
          (sql/continue-identity true))
        ["TRUNCATE TABLE \"continents\" CONTINUE IDENTITY"]))

(deftest test-truncate-continue-identity-false
  (sql= (sql/truncate db [:continents]
          (sql/continue-identity false))
        ["TRUNCATE TABLE \"continents\""]))

(deftest test-truncate-cascade-true
  (sql= (sql/truncate db [:continents]
          (sql/cascade true))
        ["TRUNCATE TABLE \"continents\" CASCADE"]))

(deftest test-truncate-cascade-false
  (sql= (sql/truncate db [:continents]
          (sql/cascade false))
        ["TRUNCATE TABLE \"continents\""]))

(deftest test-truncate-restart-identity
  (sql= (sql/truncate db [:continents]
          (sql/restart-identity true))
        ["TRUNCATE TABLE \"continents\" RESTART IDENTITY"]))

(deftest test-truncate-restart-identity-false
  (sql= (sql/truncate db [:continents]
          (sql/restart-identity false))
        ["TRUNCATE TABLE \"continents\""]))

(deftest test-truncate-restrict
  (sql= (sql/truncate db [:continents]
          (sql/restrict false))
        ["TRUNCATE TABLE \"continents\""]))

(deftest test-truncate-restrict-false
  (sql= (sql/truncate db [:continents]
          (sql/restrict false))
        ["TRUNCATE TABLE \"continents\""]))
