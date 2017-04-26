(ns sqlingvo.copy-test
  (:require #?(:clj [sqlingvo.test :refer [db sql=]]
               :cljs [sqlingvo.test :refer [db] :refer-macros [sql=]])
            [clojure.test :refer [deftest is]]
            [sqlingvo.core :as sql]
            [sqlingvo.expr :as expr]))

(deftest test-copy-keyword-db
  (sql= (sql/copy :postgresql :country []
          (sql/from :stdin))
        ["COPY \"country\" FROM STDIN"]))

(deftest test-copy-stdin
  (sql= (sql/copy db :country []
          (sql/from :stdin))
        ["COPY \"country\" FROM STDIN"]))

(deftest test-copy-country
  (sql= (sql/copy db :country []
          (sql/from "/usr1/proj/bray/sql/country_data"))
        ["COPY \"country\" FROM ?"
         "/usr1/proj/bray/sql/country_data"]))

(deftest test-copy-country-with-encoding
  (sql= (sql/copy db :country []
          (sql/from "/usr1/proj/bray/sql/country_data")
          (sql/encoding "UTF-8"))
        ["COPY \"country\" FROM ? ENCODING ?"
         "/usr1/proj/bray/sql/country_data" "UTF-8"]))

(deftest test-copy-country-with-delimiter
  (sql= (sql/copy db :country []
          (sql/from "/usr1/proj/bray/sql/country_data")
          (sql/delimiter " "))
        ["COPY \"country\" FROM ? DELIMITER ?"
         "/usr1/proj/bray/sql/country_data" " "]))

(deftest test-copy-country-columns
  (sql= (sql/copy db :country [:id :name]
          (sql/from "/usr1/proj/bray/sql/country_data"))
        ["COPY \"country\" (\"id\", \"name\") FROM ?"
         "/usr1/proj/bray/sql/country_data"]))

#?(:clj (deftest test-copy-from-expands-to-absolute-path
          (sql= (sql/copy db :country []
                  (sql/from (java.io.File. "country_data")))
                ["COPY \"country\" FROM ?"
                 (.getAbsolutePath (java.io.File. "country_data"))])))
