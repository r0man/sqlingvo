(ns sqlingvo.core-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.test :refer :all]
            [sqlingvo.core :refer :all]
            [sqlingvo.expr :refer :all]
            [sqlingvo.util :refer :all]
            [sqlingvo.test :refer [db sql= with-stmt]]))

(deftest test-from
  (let [[from stmt] ((from :continents) {})]
    (is (= [{:op :table :children [:name] :name :continents}] from))
    (is (= {:from [{:op :table, :children [:name] :name :continents}]} stmt))))

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
  (are [args expected]
      (is (= expected (apply as args)))
    [:id :other]
    (assoc (parse-expr :id) :as :other)
    [:continents [:id :name]]
    [(assoc (parse-expr :continents.id) :as :continents-id)
     (assoc (parse-expr :continents.name) :as :continents-name)]
    [:public.continents [:id :name]]
    [(assoc (parse-expr :public.continents.id) :as :public-continents-id)
     (assoc (parse-expr :public.continents.name) :as :public-continents-name)]
    ['(count *) :count]
    (assoc (parse-expr '(count *)) :as :count)))

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
