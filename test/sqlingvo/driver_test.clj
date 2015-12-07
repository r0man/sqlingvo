(ns sqlingvo.driver-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.test :refer :all]
            [sqlingvo.core :refer :all]
            [sqlingvo.driver :as driver]
            [sqlingvo.test :refer [with-backends]]))

(defn create-countries
  "Create the countries table."
  [db]
  @(create-table db :countries
     (column :id :int)
     (column :name :text)))

(defn insert-countries
  "Insert countries into the table."
  [db]
  @(insert db :countries [:id :name]
     (values [{:id 1 :name "Spain"}
              {:id 2 :name "Germany"}
              {:id 3 :name "France"}
              {:id 4 :name "Portugal"}])))

(defn setup-countries [db]
  @(drop-table db [:countries]
     (if-exists true))
  (create-countries db)
  (insert-countries db))

(deftest test-select-countries
  (with-backends [db]
    (setup-countries db)
    (is (= @(select db [:id :name]
              (from :countries)
              (order-by :id))
           [{:id 1 :name "Spain"}
            {:id 2 :name "Germany"}
            {:id 3 :name "France"}
            {:id 4 :name "Portugal"}]))))

(deftest test-select-countries-by-name
  (with-backends [db]
    (setup-countries db)
    (is (= @(select db [:id :name]
              (from :countries)
              (where '(or (= :name "Spain")
                          (= :name "Portugal")))
              (order-by :id))
           [{:id 1 :name "Spain"}
            {:id 4 :name "Portugal"}]))))
