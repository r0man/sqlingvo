(ns sqlingvo.truncate-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.test :refer :all]
            [sqlingvo.core :refer :all]
            [sqlingvo.test :refer [db sql= with-stmt]]))

(deftest test-truncate-continents
  (sql= (truncate db [:continents])
        ["TRUNCATE TABLE \"continents\""]))

(deftest test-truncate-continents-and-countries
  (sql= (truncate db [:continents :countries])
        ["TRUNCATE TABLE \"continents\", \"countries\""]))

(deftest test-truncate-continents-restart-restrict
  (sql= (truncate db [:continents]
          (restart-identity true)
          (restrict true))
        ["TRUNCATE TABLE \"continents\" RESTART IDENTITY RESTRICT"]))

(deftest test-truncate-continents-continue-cascade
  (sql= (truncate db [:continents]
          (continue-identity true)
          (cascade true))
        ["TRUNCATE TABLE \"continents\" CONTINUE IDENTITY CASCADE"]))

(deftest test-truncate-continue-identity
  (sql= (truncate db [:continents]
          (continue-identity true))
        ["TRUNCATE TABLE \"continents\" CONTINUE IDENTITY"]))

(deftest test-truncate-continue-identity-false
  (sql= (truncate db [:continents]
          (continue-identity false))
        ["TRUNCATE TABLE \"continents\""]))

(deftest test-truncate-cascade-true
  (sql= (truncate db [:continents]
          (cascade true))
        ["TRUNCATE TABLE \"continents\" CASCADE"]))

(deftest test-truncate-cascade-false
  (sql= (truncate db [:continents]
          (cascade false))
        ["TRUNCATE TABLE \"continents\""]))

(deftest test-truncate-restart-identity
  (sql= (truncate db [:continents]
          (restart-identity true))
        ["TRUNCATE TABLE \"continents\" RESTART IDENTITY"]))

(deftest test-truncate-restart-identity-false
  (sql= (truncate db [:continents]
          (restart-identity false))
        ["TRUNCATE TABLE \"continents\""]))

(deftest test-truncate-restrict
  (sql= (truncate db [:continents]
          (restrict false))
        ["TRUNCATE TABLE \"continents\""]))

(deftest test-truncate-restrict-false
  (sql= (truncate db [:continents]
          (restrict false))
        ["TRUNCATE TABLE \"continents\""]))
