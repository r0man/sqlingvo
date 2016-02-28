(ns sqlingvo.drop-table-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.test :refer :all]
            [sqlingvo.core :refer :all]
            [sqlingvo.expr :refer :all]
            [sqlingvo.test :refer [db sql= with-stmt]]))

(deftest test-drop-continents
  (with-stmt
    ["DROP TABLE \"continents\""]
    (drop-table db [:continents])
    (is (= :drop-table (:op stmt)))
    (is (= [(parse-table :continents)] (:tables stmt)))
    (is (= [:tables] (:children stmt)))))

(deftest test-drop-continents-and-countries
  (with-stmt
    ["DROP TABLE \"continents\", \"countries\""]
    (drop-table db [:continents :countries])
    (is (= :drop-table (:op stmt)))
    (is (= (map parse-table [:continents :countries]) (:tables stmt)))
    (is (= [:tables] (:children stmt)))))

(deftest test-drop-continents-countries-if-exists-restrict
  (with-stmt
    ["DROP TABLE IF EXISTS \"continents\", \"countries\" RESTRICT"]
    (drop-table db [:continents :countries]
      (if-exists true)
      (restrict true))
    (is (= :drop-table (:op stmt)))
    (is (= {:op :if-exists} (:if-exists stmt)))
    (is (= (map parse-table [:continents :countries]) (:tables stmt)))
    (is (= {:op :restrict} (:restrict stmt)))
    (is (= [:tables] (:children stmt)))))

(deftest test-drop-continents-if-exists
  (with-stmt
    ["DROP TABLE IF EXISTS \"continents\""]
    (drop-table db [:continents]
      (if-exists true))
    (is (= (map parse-table [:continents]) (:tables stmt)))
    (is (= [:tables] (:children stmt)))))

(deftest test-drop-continents-if-exists-false
  (with-stmt
    ["DROP TABLE \"continents\""]
    (drop-table db [:continents]
      (if-exists false))
    (is (= (map parse-table [:continents]) (:tables stmt)))
    (is (= [:tables] (:children stmt)))))
