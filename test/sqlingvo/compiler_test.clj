(ns sqlingvo.compiler-test
  (:require [clojure.test :refer :all]
            [sqlingvo.compiler :refer :all]
            [sqlingvo.db :refer [postgresql]]))

(def db (postgresql))

(deftest test-compile-column
  (are [ast expected]
    (is (= expected (compile-stmt db ast)))
    {:op :column :name :*}
    ["*"]
    {:op :column :table :continents :name :*}
    ["\"continents\".*"]
    {:op :column :name :created-at}
    ["\"created_at\""]
    {:op :column :table :continents :name :created-at}
    ["\"continents\".\"created_at\""]
    {:op :column :schema :public :table :continents :name :created-at}
    ["\"public\".\"continents\".\"created_at\""]
    {:op :column :schema :public :table :continents :name :created-at :as :c}
    ["\"public\".\"continents\".\"created_at\" AS \"c\""]))

(deftest test-compile-constant
  (are [ast expected]
    (is (= expected (compile-stmt db ast)))
    {:op :constant :form 1}
    ["1"]
    {:op :constant :form 3.14}
    ["3.14"]
    {:op :constant :form "x"}
    ["?" "x"]))

(deftest test-compile-sql
  (are [ast expected]
    (is (= expected (compile-sql db ast)))
    {:op :nil}
    ["NULL"]
    {:op :constant :form 1}
    ["1"]
    {:op :keyword :form :continents.created-at}
    ["\"continents\".\"created_at\""]
    {:op :fn :name 'max :args [{:op :keyword :form :created-at}]}
    ["max(\"created_at\")"]
    {:op :fn :name 'greatest :args [{:op :constant :form 1} {:op :constant :form 2}]}
    ["greatest(1, 2)"]
    {:op :fn :name 'ST_AsText :args [{:op :fn :name 'ST_Centroid :args [{:op :constant :form "MULTIPOINT(-1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6)"}]}]}
    ["ST_AsText(ST_Centroid(?))" "MULTIPOINT(-1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6)"]))

(deftest test-compile-drop-table
  (are [ast expected]
    (is (= expected (compile-sql db ast)))
    {:op :drop-table :tables [{:op :table :name :continents}]}
    ["DROP TABLE \"continents\""]
    {:op :drop-table :tables [{:op :table :name :continents}] :cascade {:op :cascade :cascade true}}
    ["DROP TABLE \"continents\" CASCADE"]
    {:op :drop-table :tables [{:op :table :name :continents}] :restrict {:op :restrict :restrict true}}
    ["DROP TABLE \"continents\" RESTRICT"]
    {:op :drop-table :tables [{:op :table :name :continents}] :if-exists {:op :if-exists :if-exists true}}
    ["DROP TABLE IF EXISTS \"continents\""]
    {:op :drop-table :tables [{:op :table :name :continents}]
     :cascade {:op :cascade :cascade true}
     :restrict {:op :restrict :restrict true}
     :if-exists {:op :if-exists :if-exists true}}
    ["DROP TABLE IF EXISTS \"continents\" CASCADE RESTRICT"]))

(deftest test-compile-limit
  (are [ast expected]
    (is (= expected (compile-sql db ast)))
    {:op :limit :count 1}
    ["LIMIT 1"]
    {:op :limit :count nil}
    ["LIMIT ALL"]))

(deftest test-compile-offset
  (are [ast expected]
    (is (= expected (compile-sql db ast)))
    {:op :offset :start 1}
    ["OFFSET 1"]
    {:op :offset :start nil}
    ["OFFSET 0"]))

(deftest test-compile-table
  (are [ast expected]
    (is (= expected (compile-sql db ast)))
    {:op :table :name :continents}
    ["\"continents\""]
    {:op :table :schema :public :name :continents}
    ["\"public\".\"continents\""]
    {:op :table :schema :public :name :continents :as :c}
    ["\"public\".\"continents\" \"c\""]))

(deftest test-wrap-stmt
  (are [stmt expected]
    (is (= expected (wrap-stmt stmt)))
    ["SELECT 1"]
    ["(SELECT 1)"]
    ["SELECT ?" "x"]
    ["(SELECT ?)" "x"]))

(deftest test-unwrap-stmt
  (are [stmt expected]
    (is (= expected (unwrap-stmt stmt)))
    ["(SELECT 1)"]
    ["SELECT 1"]
    ["(SELECT ?)" "x"]
    ["SELECT ?" "x"]))
