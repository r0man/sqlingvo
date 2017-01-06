(ns sqlingvo.compiler-test
  (:require [clojure.test :refer [are deftest is]]
            [sqlingvo.compiler :as compiler :refer [compile-sql]]
            [sqlingvo.core :as sql]
            [sqlingvo.expr :as expr]
            [sqlingvo.test :refer [db]]))

(deftest test-compile-column
  (are [ast expected]
      (= expected (compile-sql db ast))
    {:op :column :name :*}
    ["*"]
    {:op :column
     :table :continents
     :name :*}
    ["\"continents\".*"]
    {:op :column
     :name :created-at}
    ["\"created-at\""]
    {:op :column
     :table :continents
     :name :created-at}
    ["\"continents\".\"created-at\""]
    {:op :column
     :schema :public
     :table :continents
     :name :created-at}
    ["\"public\".\"continents\".\"created-at\""]
    {:op :alias
     :name :c
     :expr
     {:op :column
      :schema :public
      :table :continents
      :name :created-at}}
    ["\"public\".\"continents\".\"created-at\" AS \"c\""]))

(deftest test-compile-constant
  (are [ast expected]
      (= expected (compile-sql db ast))
    {:op :constant
     :form 1
     :type :number
     :val 1}
    ["1"]
    {:op :constant
     :form 3.14
     :type :number
     :val 3.14}
    ["3.14"]
    {:op :constant
     :form "x"
     :type :string
     :val "x"}
    ["?" "x"]))

(deftest test-compile-sql
  (are [ast expected]
      (= (compile-sql db (expr/parse-expr ast)) expected)
    nil
    ["NULL"]
    1
    ["1"]
    :continents.created-at
    ["\"continents\".\"created-at\""]
    '(max :created-at)
    ["max(\"created-at\")"]
    '(greatest 1 2)
    ["greatest(1, 2)"]
    '(st_astext (st_centroid "MULTIPOINT(-1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6)"))
    ["st_astext(st_centroid(?))" "MULTIPOINT(-1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6)"]))

(deftest test-compile-drop-table
  (are [ast expected]
      (= expected (compile-sql db ast))
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

(deftest test-compile-table
  (are [ast expected]
      (= expected (compile-sql db ast))
    {:op :table :name :continents}
    ["\"continents\""]
    {:op :table
     :schema :public
     :name :continents}
    ["\"public\".\"continents\""]
    {:op :alias
     :expr {:op :table :schema :public :name :continents}
     :name :c}
    ["\"public\".\"continents\" \"c\""]))

(deftest test-wrap-stmt
  (are [stmt expected]
      (= (compiler/wrap-stmt stmt) expected)
    ["SELECT 1"]
    ["(SELECT 1)"]
    ["SELECT ?" "x"]
    ["(SELECT ?)" "x"]))

(deftest test-unwrap-stmt
  (are [stmt expected]
      (= (compiler/unwrap-stmt stmt) expected)
    ["(SELECT 1)"]
    ["SELECT 1"]
    ["(SELECT ?)" "x"]
    ["SELECT ?" "x"]))

(deftest test-compile-stmt-returns-vector
  (let [sql (compiler/compile-stmt (sql/ast (sql/select db [1])))]
    (is (vector? sql))
    (is (= sql ["SELECT 1"]))))
