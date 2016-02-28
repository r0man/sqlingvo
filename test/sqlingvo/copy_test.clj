(ns sqlingvo.copy-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.java.io :refer [file]]
            [clojure.test :refer :all]
            [sqlingvo.core :refer :all]
            [sqlingvo.expr :refer :all]
            [sqlingvo.test :refer [db sql= with-stmt]]))

(deftest test-copy-stdin
  (with-stmt
    ["COPY \"country\" FROM STDIN"]
    (copy db :country []
      (from :stdin))
    (is (= :copy (:op stmt)))
    (is (= [:stdin] (:from stmt)))))

(deftest test-copy-country
  (with-stmt
    ["COPY \"country\" FROM ?" "/usr1/proj/bray/sql/country_data"]
    (copy db :country []
      (from "/usr1/proj/bray/sql/country_data"))
    (is (= :copy (:op stmt)))
    (is (= ["/usr1/proj/bray/sql/country_data"] (:from stmt)))))

(deftest test-copy-country-with-encoding
  (with-stmt
    ["COPY \"country\" FROM ? ENCODING ?" "/usr1/proj/bray/sql/country_data" "UTF-8"]
    (copy db :country []
      (from "/usr1/proj/bray/sql/country_data")
      (encoding "UTF-8"))
    (is (= :copy (:op stmt)))
    (is (= ["/usr1/proj/bray/sql/country_data"] (:from stmt)))))

(deftest test-copy-country-with-delimiter
  (with-stmt
    ["COPY \"country\" FROM ? DELIMITER ?" "/usr1/proj/bray/sql/country_data" " "]
    (copy db :country []
      (from "/usr1/proj/bray/sql/country_data")
      (delimiter " "))))

(deftest test-copy-country-columns
  (with-stmt
    ["COPY \"country\" (\"id\", \"name\") FROM ?" "/usr1/proj/bray/sql/country_data"]
    (copy db :country [:id :name]
      (from "/usr1/proj/bray/sql/country_data"))
    (is (= :copy (:op stmt)))
    (is (= ["/usr1/proj/bray/sql/country_data"] (:from stmt)))
    (is (= (map parse-column [:id :name]) (:columns stmt)))))

(deftest test-copy-from-expands-to-absolute-path
  (is (= ["COPY \"country\" FROM ?" (.getAbsolutePath (file "country_data"))]
         (sql (copy db :country [] (from (file "country_data")))))))
