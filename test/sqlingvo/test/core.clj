(ns sqlingvo.test.core
  (:refer-clojure :exclude [group-by replace])
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        sqlingvo.core))

(deftest test-group-by
  (let [node (:group-by (group-by {} :name :created-at))]
    (is (= :group-by (:op node)))
    (let [exprs (:exprs node)]
      (let [node (first (:children exprs))]
        (is (= :column (:op node)))
        (is (= :name (:name node))))
      (let [node (second (:children exprs))]
        (is (= :column (:op node)))
        (is (= :created-at (:name node)))))))

(deftest test-limit
  (is (= {:limit {:op :limit :count 1}} (limit {} 1))))

(deftest test-drop-table
  (are [stmt expected]
       (is (= expected (sql stmt)))
       (drop-table :continents)
       ["DROP TABLE continents"]
       (drop-table [:continents :countries])
       ["DROP TABLE continents, countries"]
       (drop-table :continents :if-exists true :restrict true)
       ["DROP TABLE IF EXISTS continents RESTRICT"]
       (drop-table [:continents :countries] :if-exists true :restrict true)
       ["DROP TABLE IF EXISTS continents, countries RESTRICT"]))

(deftest test-from
  (let [node (:from (from {} :continents))]
    (is (= :from (:op node)))
    (let [node (first (:from node))]
      (is (= :table (:op node)))
      (is (= :continents (:name node)))))
  (let [node (:from (from {} :continents :countries))]
    (is (= :from (:op node)))
    (let [node (first (:from node))]
      (is (= :table (:op node)))
      (is (= :continents (:name node))))
    (let [node (second (:from node))]
      (is (= :table (:op node)))
      (is (= :countries (:name node)))))
  ;; (let [node (:from (from (select 1 2 3)))]
  ;;   (println node)
  ;;   (is (= :from (:op node)))
  ;;   (is (= (select [1 2 3]) (first (:from node)))))

  )

(deftest test-offset
  (is (= {:offset {:op :offset :start 1}} (offset {} 1))))

(deftest test-order-by
  (let [node (:order-by (order-by {} :created-at))]
    (is (= :order-by (:op node)))
    (let [node (:exprs node)]
      (is (= :exprs (:op node)))
      (is (= [{:op :column :schema nil :table nil :name :created-at :as nil}] (:children node)))))
  (let [node (:order-by (order-by {} [:name :created-at] :direction :desc :nulls :first))]
    (is (= :order-by (:op node)))
    (is (= :desc (:direction node)))
    (is (= :first (:nulls node)))
    (let [node (:exprs node)]
      (is (= :exprs (:op node)))
      (is (= [{:op :column :schema nil :table nil :name :name :as nil}
              {:op :column :schema nil :table nil :name :created-at :as nil}] (:children node))))))

(deftest test-select
  (are [stmt expected]
       (is (= expected (sql stmt)))
       (select 1)
       ["SELECT 1"]
       (select (as 1 :n))
       ["SELECT 1 AS n"]
       (select (as "s" :s))
       ["SELECT ? AS s" "s"]
       (select 1 2 3)
       ["SELECT 1, 2, 3"]
       (select (as 1 :a) (as 2 :b) (as 3 :c))
       ["SELECT 1 AS a, 2 AS b, 3 AS c"]
       (-> (select) (from :continents))
       ["SELECT * FROM continents"]
       (-> (select *) (from :continents))
       ["SELECT * FROM continents"]
       (-> (select *) (from :continents/c))
       ["SELECT * FROM continents AS c"]
       (-> (select * 1 "x") (from :continents))
       ["SELECT *, 1, ? FROM continents" "x"]
       (-> (select :created-at) (from :continents))
       ["SELECT created-at FROM continents"]
       (-> (select :created-at/c) (from :continents))
       ["SELECT created-at AS c FROM continents"]
       (-> (select :name :created-at) (from :continents))
       ["SELECT name, created-at FROM continents"]
       (-> (select :name '(max :created-at)) (from :continents))
       ["SELECT name, max(created-at) FROM continents"]
       (select '(greatest 1 2) '(lower "X"))
       ["SELECT greatest(1, 2), lower(?)" "X"]
       (select '(+ 1 (greatest 2 3)))
       ["SELECT (1 + greatest(2, 3))"]
       (-> (select (as '(max :created-at) :m)) (from :continents))
       ["SELECT max(created-at) AS m FROM continents"]
       (-> (select *) (from :continents) (limit 1))
       ["SELECT * FROM continents LIMIT 1"]
       (-> (-> (select *)) (from :continents) (offset 1))
       ["SELECT * FROM continents OFFSET 1"]
       (-> (-> (select *)) (from :continents) (limit 1) (offset 2))
       ["SELECT * FROM continents LIMIT 1 OFFSET 2"]
       (-> (select *) (from :continents) (order-by :created-at))
       ["SELECT * FROM continents ORDER BY created-at"]
       (-> (select *) (from :continents) (order-by :created-at :direction :asc))
       ["SELECT * FROM continents ORDER BY created-at ASC"]
       (-> (select *) (from :continents) (order-by :created-at :direction :desc))
       ["SELECT * FROM continents ORDER BY created-at DESC"]
       (-> (select *) (from :continents) (order-by :created-at :nulls :first))
       ["SELECT * FROM continents ORDER BY created-at NULLS FIRST"]
       (-> (select *) (from :continents) (order-by :created-at :nulls :last))
       ["SELECT * FROM continents ORDER BY created-at NULLS LAST"]
       (-> (select *) (from :continents) (order-by [:name :created-at] :direction :asc))
       ["SELECT * FROM continents ORDER BY name, created-at ASC"]
       (-> (select *)
           (from (as (select 1 2 3) :x)))
       ["SELECT * FROM (SELECT 1, 2, 3) AS x"]
       (-> (select *)
           (from (as (select 1) :x) (as (select 2) :y)))
       ["SELECT * FROM (SELECT 1) AS x, (SELECT 2) AS y"]
       (-> (select *) (from :continents) (group-by :created-at))
       ["SELECT * FROM continents GROUP BY created-at"]
       (-> (select *) (from :continents) (group-by :name :created-at))
       ["SELECT * FROM continents GROUP BY name, created-at"]
       (-> (select 1) (where '(= 1 1)))
       ["SELECT 1 WHERE (1 = 1)"]
       (-> (select 1) (where '(= 1 2 3)))
       ["SELECT 1 WHERE (1 = 2) AND (2 = 3)"]
       (-> (select 1) (where '(< 1 2)))
       ["SELECT 1 WHERE (1 < 2)"]
       (-> (select 1) (where '(< 1 2 3)))
       ["SELECT 1 WHERE (1 < 2) AND (2 < 3)"]
       (select (select 1))
       ["SELECT (SELECT 1)"]
       (select (select 1) (select "x"))
       ["SELECT (SELECT 1), (SELECT ?)" "x"]
       (union (select 1) (select 2))
       ["SELECT 1 UNION (SELECT 2)"]
       (union (select 1) (select 2) :all true)
       ["SELECT 1 UNION ALL (SELECT 2)"]
       (intersect (select 1) (select 2))
       ["SELECT 1 INTERSECT (SELECT 2)"]
       (intersect (select 1) (select 2) :all true)
       ["SELECT 1 INTERSECT ALL (SELECT 2)"]
       (except (select 1) (select 2))
       ["SELECT 1 EXCEPT (SELECT 2)"]
       (except (select 1) (select 2) :all true)
       ["SELECT 1 EXCEPT ALL (SELECT 2)"]))

(deftest test-truncate
  (are [stmt expected]
       (is (= expected (sql stmt)))
       (truncate :continents)
       ["TRUNCATE TABLE continents"]
       (truncate [:continents :countries])
       ["TRUNCATE TABLE continents, countries"]
       (truncate :continents :cascade true :continue-identity true :restart-identity true :restrict true)
       ["TRUNCATE TABLE continents RESTART IDENTITY CONTINUE IDENTITY CASCADE RESTRICT"]
       (truncate [:continents :countries] :cascade true :continue-identity true :restart-identity true :restrict true)
       ["TRUNCATE TABLE continents, countries RESTART IDENTITY CONTINUE IDENTITY CASCADE RESTRICT"]))
