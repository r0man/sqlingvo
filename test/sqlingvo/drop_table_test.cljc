(ns sqlingvo.drop-table-test
  (:require #?(:clj [sqlingvo.test :refer [db sql=]]
               :cljs [sqlingvo.test :refer [db] :refer-macros [sql=]])
            [clojure.test :refer [deftest is]]
            [sqlingvo.core :as sql]))

(deftest test-drop-table-keyword-db
  (sql= (sql/drop-table :postgresql [:continents])
        ["DROP TABLE \"continents\""]))

(deftest test-drop-continents
  (sql= (sql/drop-table db [:continents])
        ["DROP TABLE \"continents\""]))

(deftest test-drop-continents-and-countries
  (sql= (sql/drop-table db [:continents :countries])
        ["DROP TABLE \"continents\", \"countries\""]))

(deftest test-drop-continents-countries-if-exists-restrict
  (sql= (sql/drop-table db [:continents :countries]
          (sql/if-exists true)
          (sql/restrict true))
        ["DROP TABLE IF EXISTS \"continents\", \"countries\" RESTRICT"]))

(deftest test-drop-continents-if-exists
  (sql= (sql/drop-table db [:continents]
          (sql/if-exists true))
        ["DROP TABLE IF EXISTS \"continents\""]))

(deftest test-drop-continents-if-exists-false
  (sql= (sql/drop-table db [:continents]
          (sql/if-exists false))
        ["DROP TABLE \"continents\""]))
