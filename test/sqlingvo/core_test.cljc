(ns sqlingvo.core-test
  (:require #?(:clj [sqlingvo.test :refer [db sql=]]
               :cljs [sqlingvo.test :refer [db] :refer-macros [sql=]])
            [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [clojure.test :refer [are deftest is]]
            [sqlingvo.core :as sql]
            [sqlingvo.util :as util]
            [clojure.spec.test.alpha :as stest]))

(stest/instrument)

(deftest test-excluded-keyword
  (are [arg expected]
      (= (sql/excluded-keyword arg) expected)
    nil nil
    :a :EXCLUDED.a))

(deftest test-excluded-kw-map
  (are [arg expected]
      (= (sql/excluded-kw-map arg) expected)
    nil
    nil
    {:a 1}
    {:a :EXCLUDED.a}
    [:a]
    {:a :EXCLUDED.a}))

(deftest test-column
  (are [column expected]
      (= (sql/ast column) expected)
    (sql/column :id :serial :primary-key? true)
    {:columns [:id],
     :column
     {:id
      {:schema nil,
       :children [:name],
       :table nil,
       :primary-key? true,
       :default nil,
       :name :id,
       :val :id,
       :type :serial,
       :op :column,
       :form :id}}}))

(deftest test-from
  (let [[from stmt] ((sql/from :continents) {})]
    (is (= [{:op :table
             :children [:name]
             :name :continents
             :form :continents
             :val :continents}]
           from))
    (is (= {:from
            [{:op :table
              :children [:name]
              :name :continents
              :form :continents
              :val :continents}]}
           stmt))))

;; COMPOSE

(deftest test-compose
  (sql= (sql/compose
         (sql/select db [:id :name]
           (sql/from :continents))
         (sql/where '(= :id 1))
         (sql/order-by :name))
        [(str "SELECT \"id\", \"name\" "
              "FROM \"continents\" "
              "WHERE (\"id\" = 1) "
              "ORDER BY \"name\"")]))

(deftest test-compose-where-clause-using-and
  (sql= (sql/compose
         (sql/compose
          (sql/select db [:color :num-sides]
            (sql/from :shapes))
          (sql/where '(= :num-sides 3)))
         (sql/where '(= :color "green") :and))
        [(str "SELECT \"color\", \"num-sides\" "
              "FROM \"shapes\" "
              "WHERE ((\"num-sides\" = 3) and (\"color\" = ?))")
         "green"]))

(deftest test-compose-selects
  (sql= (sql/compose
         (sql/select db [1 2 3])
         (sql/select db [3 2 1]))
        ["SELECT 3, 2, 1"]))

;; AS

(deftest test-as
  (are [args expected] (= (apply sql/as args) expected)
    [:id :other]
    {:op :alias
     :children [:expr :name]
     :expr {:children [:name]
            :name :id
            :op :column
            :form :id
            :val :id}
     :name :other
     :columns []}
    ['(count :*) :count]
    {:op :alias
     :children [:expr :name]
     :columns []
     :expr
     {:op :list
      :children
      [{:form 'count
        :op :constant
        :type :symbol
        :val 'count}
       {:children [:name]
        :name :*
        :val :*
        :op :column
        :form :*}]}
     :name :count}))

;; CAST

(deftest test-cast-int-as-double-precision
  (sql= (sql/select db [`(cast 1 :double-precision)])
        ["SELECT CAST(1 AS DOUBLE PRECISION)"]))

(deftest test-cast-int-as-text
  (sql= (sql/select db [`(cast 1 :text)])
        ["SELECT CAST(1 AS TEXT)"]))

(deftest test-cast-text-as-int
  (sql= (sql/select db [`(cast "1" :int)])
        ["SELECT CAST(? AS INT)" "1"]))

(deftest test-cast-with-alias
  (sql= (sql/select db [(sql/as `(cast "1" :int) :numeric-id)])
        ["SELECT CAST(? AS INT) AS \"numeric-id\"" "1"]))

(deftest test-sql-placeholder-constant
  (let [db (assoc db :sql-placeholder util/sql-placeholder-constant)]
    (sql= (sql/select db  [:*]
            (sql/from :distributors)
            (sql/where '(and (= :dname "Anvil Distribution")
                             (= :zipcode "21201"))))
          [(str "SELECT * FROM \"distributors\" "
                "WHERE ((\"dname\" = ?) and (\"zipcode\" = ?))")
           "Anvil Distribution" "21201"])))

(deftest test-sql-placeholder-count
  (let [db (assoc db :sql-placeholder util/sql-placeholder-count)]
    (sql= (sql/select db  [:*]
            (sql/from :distributors)
            (sql/where '(and (= :dname "Anvil Distribution")
                             (= :zipcode "21201"))))
          [(str "SELECT * FROM \"distributors\" "
                "WHERE ((\"dname\" = $1) and (\"zipcode\" = $2))")
           "Anvil Distribution" "21201"])))

(deftest test-sql-placeholder-count-subselect
  (let [db (assoc db :sql-placeholder util/sql-placeholder-count)]
    (sql= (sql/select db ["a" "b" :*]
            (sql/from (sql/as (sql/select db ["c" "d"]) :x)))
          ["SELECT $1, $2, * FROM (SELECT $3, $4) AS \"x\"" "a" "b" "c" "d"])))

(deftest test-pprint
  (is (= (with-out-str (pprint (sql/select db [1])))
         "[\"SELECT 1\"]\n")))

(deftest test-table
  (is (= (sql/table :my-table
           (sql/column :a :serial :primary-key? true)
           (sql/column :b :text))
         {:children [:name],
          :columns [:a :b],
          :name :my-table,
          :val :my-table,
          :op :table,
          :column
          {:a
           {:schema nil,
            :children [:name],
            :table :my-table,
            :primary-key? true,
            :default nil,
            :name :a,
            :val :a,
            :type :serial,
            :op :column,
            :form :a},
           :b
           {:children [:name],
            :name :b,
            :val :b,
            :op :column,
            :form :b,
            :type :text,
            :default nil,
            :schema nil,
            :table :my-table}},
          :form :my-table})))
