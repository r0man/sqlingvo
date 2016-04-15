(ns sqlingvo.compiler-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.test :refer :all]
            [sqlingvo.compiler :refer :all]
            [sqlingvo.core :refer :all]
            [sqlingvo.db :as db]))

(def db (db/postgresql))

(deftest test-compile-column
  (are [ast expected]
      (= expected (compile-sql db ast))
    {:op :column :name :*}
    ["*"]
    {:op :column :table :continents :name :*}
    ["\"continents\".*"]
    {:op :column :name :created-at}
    ["\"created-at\""]
    {:op :column :table :continents :name :created-at}
    ["\"continents\".\"created-at\""]
    {:op :column :schema :public :table :continents :name :created-at}
    ["\"public\".\"continents\".\"created-at\""]
    {:op :column :schema :public :table :continents :name :created-at :as :c}
    ["\"public\".\"continents\".\"created-at\" AS \"c\""]))

(deftest test-compile-constant
  (are [ast expected]
      (= expected (compile-sql db ast))
    {:op :constant
     :form 1
     :literal? true
     :type :number
     :val 1}
    ["1"]
    {:op :constant
     :form 3.14
     :literal? true
     :type :number
     :val 3.14}
    ["3.14"]
    {:op :constant
     :form "x"
     :literal? true
     :type :string
     :val "x"}
    ["?" "x"]))

(deftest test-compile-sql
  (are [ast expected]
      (= expected (compile-sql db ast))
    {:op :nil}
    ["NULL"]
    {:op :constant
     :form 1
     :literal? true
     :type :number
     :val 1}
    ["1"]
    {:op :keyword :form :continents.created-at}
    ["\"continents\".\"created-at\""]
    {:op :fn :name 'max :args [{:op :keyword :form :created-at}]}
    ["max(\"created-at\")"]
    {:op :fn
     :name 'greatest
     :args [{:op :constant
             :form 1
             :literal? true
             :type :number
             :val 1}
            {:op :constant
             :form 2
             :literal? true
             :type :number
             :val 2}]}
    ["greatest(1, 2)"]
    {:op :fn :name 'st_astext :args [{:op :fn :name 'st_centroid :args [{:op :constant :form "MULTIPOINT(-1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6)"}]}]}
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

(deftest test-compile-limit
  (are [ast expected]
      (= expected (compile-sql db ast))
    {:op :limit :count 1}
    ["LIMIT 1"]
    {:op :limit :count nil}
    []))

(deftest test-compile-offset
  (are [ast expected]
      (= expected (compile-sql db ast))
    {:op :offset :start 1}
    ["OFFSET 1"]
    {:op :offset :start nil}
    ["OFFSET 0"]))

(deftest test-compile-table
  (are [ast expected]
      (= expected (compile-sql db ast))
    {:op :table :name :continents}
    ["\"continents\""]
    {:op :table :schema :public :name :continents}
    ["\"public\".\"continents\""]
    {:op :table :schema :public :name :continents :as :c}
    ["\"public\".\"continents\" \"c\""]))

(deftest test-wrap-stmt
  (are [stmt expected]
      (= expected (wrap-stmt stmt))
    ["SELECT 1"]
    ["(SELECT 1)"]
    ["SELECT ?" "x"]
    ["(SELECT ?)" "x"]))

(deftest test-unwrap-stmt
  (are [stmt expected]
      (= expected (unwrap-stmt stmt))
    ["(SELECT 1)"]
    ["SELECT 1"]
    ["(SELECT ?)" "x"]
    ["SELECT ?" "x"]))

(deftest test-compile-stmt-returns-vector
  (let [sql (compile-stmt (ast (select db [1])))]
    (is (vector? sql))
    (is (= sql ["SELECT 1"]))))

(deftest test-compile-values-maps
  (let [{:keys [values]}
        (ast (values
              [{:code "B6717"
                :title "Tampopo"
                :did 110
                :date-prod "1985-02-10"
                :kind "Comedy"}
               {:code "HG120"
                :title "The Dinner Game"
                :did 140
                :date-prod "1985-02-10"
                :kind "Comedy"}]))]
    (is (= (compile-sql db values)
           ["VALUES (?, ?, 110, ?, ?), (?, ?, 140, ?, ?)"
            "B6717"
            "1985-02-10"
            "Comedy"
            "Tampopo"
            "HG120"
            "1985-02-10"
            "Comedy"
            "The Dinner Game"]))))

(deftest test-compile-values-with-default
  (let [{:keys [values]} (ast (values :default))]
    (is (= (compile-sql db values)
           ["DEFAULT VALUES"]))))

(deftest test-compile-values-with-exprs
  (let [{:keys [values]}
        (ast (values ['(cast "192.168.0.1" :inet)
                      "192.168.0.10"
                      "192.168.1.43"]))]
    (is (= (compile-sql db values)
           ["VALUES (CAST(? AS inet)), (?), (?)"
            "192.168.0.1"
            "192.168.0.10"
            "192.168.1.43"]))))
