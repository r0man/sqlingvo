(ns sqlingvo.core-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.test :refer :all]
            [sqlingvo.core :refer :all]
            [sqlingvo.expr :refer :all]
            [sqlingvo.util :refer :all]
            [sqlingvo.test :refer :all]))

(deftest test-column
  (are [column expected]
      (= (ast column) expected)
    (column :id :serial :primary-key? true)
    {:columns [:id]
     :column
     {:id
      {:schema nil
       :table nil
       :primary-key? true
       :default nil
       :name :id
       :type :serial
       :op :column}}}))

(deftest test-from
  (let [[from stmt] ((from :continents) {})]
    (is (= [{:op :table
             :children [:name]
             :name :continents
             :form :continents}]
           from))
    (is (= {:from [{:op :table
                    :children [:name]
                    :name :continents
                    :form :continents}]}
           stmt))))

;; COMPOSE

(deftest test-compose
  (with-stmt
    ["SELECT \"id\", \"name\" FROM \"continents\" WHERE (\"id\" = 1) ORDER BY \"name\""]
    (compose (select db [:id :name]
               (from :continents))
             (where '(= :id 1))
             (order-by :name))))

(deftest test-compose-where-clause-using-and
  (with-stmt
    ["SELECT \"color\", \"num-sides\" FROM \"shapes\" WHERE ((\"num-sides\" = 3) and (\"color\" = ?))" "green"]
    (let [triangles (compose (select db [:color :num-sides] (from :shapes))
                             (where '(= :num-sides 3)))]
      (compose triangles (where '(= :color "green") :and)))))

(deftest test-compose-selects
  (with-stmt
    ["SELECT 3, 2, 1"]
    (compose (select db [1 2 3])
             (select db [3 2 1]))))

;; AS

(deftest test-as
  (are [args expected] (= (apply as args) expected)
    [:id :other]
    {:op :alias
     :children [:expr :name]
     :expr {:children [:name]
            :name :id
            :op :column
            :form :id}
     :name :other
     :columns []}
    [:continents [:id :name]]
    {:op :alias
     :children [:expr :name]
     :expr
     {:children [:name]
      :name :continents
      :op :column
      :form :continents}
     :name [:id :name]
     :columns []}
    [:public.continents [:id :name]]
    {:op :alias
     :children [:expr :name]
     :expr
     {:children [:table :name]
      :table :public
      :name :continents
      :op :column
      :form :public.continents}
     :name [:id :name]
     :columns []}
    ['(count *) :count]
    {:op :alias
     :children [:expr :name]
     :expr
     {:args
      [{:val '*
        :type :symbol
        :op :constant
        :literal? true
        :form '*}]
      :children [:args]
      :name "count"
      :op :fn}
     :name :count
     :columns []}))

;; CAST

(deftest test-cast-int-as-text
  (with-stmt
    ["SELECT CAST(1 AS text)"]
    (select db [`(cast 1 :text)])))

(deftest test-cast-text-as-int
  (with-stmt
    ["SELECT CAST(? AS int)" "1"]
    (select db [`(cast "1" :int)])))

(deftest test-cast-with-alias
  (with-stmt
    ["SELECT CAST(? AS int) AS \"numeric-id\"" "1"]
    (select db [(as `(cast "1" :int) :numeric-id)])))

(deftest test-sql-placeholder-constant
  (let [db (assoc db :sql-placeholder sql-placeholder-constant)]
    (sql= (select db  [:*]
            (from :distributors)
            (where '(and (= :dname "Anvil Distribution")
                         (= :zipcode "21201"))))
          ["SELECT * FROM \"distributors\" WHERE ((\"dname\" = ?) and (\"zipcode\" = ?))"
           "Anvil Distribution" "21201"])))

(deftest test-sql-placeholder-count
  (let [db (assoc db :sql-placeholder sql-placeholder-count)]
    (sql= (select db  [:*]
            (from :distributors)
            (where '(and (= :dname "Anvil Distribution")
                         (= :zipcode "21201"))))
          ["SELECT * FROM \"distributors\" WHERE ((\"dname\" = $1) and (\"zipcode\" = $2))"
           "Anvil Distribution" "21201"])))

(deftest test-sql-placeholder-count-subselect
  (let [db (assoc db :sql-placeholder sql-placeholder-count)]
    (sql= (select db ["a" "b" :*]
            (from (as (select db ["c" "d"]) :x)))
          ["SELECT $1, $2, * FROM (SELECT $3, $4) AS \"x\"" "a" "b" "c" "d"])))
