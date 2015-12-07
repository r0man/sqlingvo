(ns sqlingvo.driver-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.test :refer :all]
            [sqlingvo.core :refer :all]
            [sqlingvo.driver :as driver]
            [sqlingvo.test :refer [with-backends]]))

(def countries
  [{:id 1 :name "Spain"}
   {:id 2 :name "Germany"}
   {:id 3 :name "France"}
   {:id 4 :name "Portugal"}])

(defn drop-countries
  "Drop the countries table."
  [db]
  @(drop-table db [:countries]
     (if-exists true)))

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
     (values countries)))

(defn setup-countries [db]
  (drop-countries db)
  (create-countries db)
  (insert-countries db))

(deftest test-select
  (with-backends [db]
    (setup-countries db)
    (is (= @(select db [:*]
              (from :countries))
           countries))))

(deftest test-select-columns
  (with-backends [db]
    (setup-countries db)
    (is (= @(select db [:id]
              (from :countries))
           (map #(select-keys % [:id]) countries)))))

(deftest test-select-where-equals
  (with-backends [db]
    (setup-countries db)
    (is (= @(select db [:id :name]
              (from :countries)
              (where '(= :name "Spain")))
           [{:id 1 :name "Spain"}]))))

(deftest test-insert-returning
  (with-backends [db]
    (drop-countries db)
    (create-countries db)
    (is (= @(insert db :countries []
              (values countries)
              (returning :*))
           countries))))

(deftest test-insert-returning-column
  (with-backends [db]
    (drop-countries db)
    (create-countries db)
    (is (= @(insert db :countries []
              (values countries)
              (returning :id))
           (map #(select-keys % [:id]) countries)))))

(deftest test-except
  (with-backends [db]
    (is (= @(except
             (select db [(as '(generate_series 1 3) :x)])
             (select db [(as '(generate_series 3 4) :x)]))
           [{:x 1} {:x 2}]))))

(deftest test-union
  (with-backends [db]
    (is (= @(union
             (select db [(as 1 :x)])
             (select db [(as 1 :x)]))
           [{:x 1}]))))

(deftest test-union-all
  (with-backends [db]
    (is (= @(union
             {:all true}
             (select db [(as 1 :x)])
             (select db [(as 1 :x)]))
           [{:x 1} {:x 1}]))))

(deftest test-intersect
  (with-backends [db]
    (is (= @(intersect
             (select db [(as '(generate_series 1 2) :x)])
             (select db [(as '(generate_series 2 3) :x)]))
           [{:x 2}]))))
