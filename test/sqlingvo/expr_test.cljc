(ns sqlingvo.expr-test
  (:require [clojure.test :refer [are deftest is]]
            [sqlingvo.core :refer [as select values]]
            [sqlingvo.expr :as expr]
            [sqlingvo.test :refer [db]]))

(deftest test-parse-column
  (are [column expected] (= (expr/parse-column column) expected)
    :id
    {:op :column
     :children [:name]
     :name :id
     :form :id
     :sqlingvo/op :column
     :sqlingvo.column/name "id"
     :val :id}

    :continents.id
    {:op :column
     :children [:table :name]
     :table :continents
     :name :id
     :sqlingvo/op :column
     :sqlingvo.column/table "continents"
     :sqlingvo.column/name "id"
     :form :continents.id
     :val :continents.id}

    :public.continents.id
    {:op :column
     :children [:schema :table :name]
     :schema :public
     :table :continents
     :sqlingvo/op :column
     :sqlingvo.column/schema "public"
     :sqlingvo.column/table "continents"
     :sqlingvo.column/name "id"
     :name :id
     :form :public.continents.id
     :val :public.continents.id}

    :continent/id
    {:op :column
     :children [:name]
     :name :id
     :ns "continent"
     :form :continent/id
     :sqlingvo/op :column
     :sqlingvo.column/name "id"
     :val :continent/id}

    (expr/parse-column :continents.id)
    (expr/parse-column :continents.id)))

(deftest test-parse-table
  (are [table expected] (= (expr/parse-table table) expected)
    :continents
    {:op :table
     :children [:name]
     :name :continents
     :form :continents
     :sqlingvo/op :table
     :sqlingvo.table/name "continents"
     :val :continents}

    :public.continents
    {:op :table
     :children [:schema :name]
     :schema :public
     :name :continents
     :form :public.continents
     :sqlingvo/op :table
     :sqlingvo.table/schema "public"
     :sqlingvo.table/name "continents"
     :val :public.continents}

    :sqlingvo/continents
    {:op :table
     :children [:name]
     :name :continents
     :ns "sqlingvo"
     :form :sqlingvo/continents
     :sqlingvo/op :table
     :sqlingvo.table/name "continents"
     :val :sqlingvo/continents}

    (expr/parse-table :public.continents)
    (expr/parse-table :public.continents)))

(deftest test-parse-expr
  (are [expr expected] (= (expr/parse-expr expr) expected)
    :*
    {:children [:name]
     :name :*
     :op :column
     :sqlingvo/op :column
     :sqlingvo.column/name "*"
     :val :*
     :form :*}
    1
    {:op :constant
     :form 1
     :type :number
     :val 1}
    1.0
    {:op :constant
     :form 1.0
     :type :number
     :val 1.0}
    "x"
    {:op :constant
     :form "x"
     :type :string
     :val "x"}
    '(= 1 1)
    {:op :list
     :children
     [(expr/parse-expr '=)
      (expr/parse-expr 1)
      (expr/parse-expr 1)]}
    `(= 1 1)
    {:op :list
     :children
     [(expr/parse-expr `=)
      (expr/parse-expr 1)
      (expr/parse-expr 1)]}
    '(= :name "Europe")
    {:op :list
     :children
     [(expr/parse-expr '=)
      (expr/parse-expr :name)
      (expr/parse-expr "Europe")]}
    '(max 1 2)
    {:op :list
     :children
     [(expr/parse-expr 'max)
      (expr/parse-expr 1)
      (expr/parse-expr 2)]}
    '(max 1 (max 2 3))
    {:op :list
     :children
     [(expr/parse-expr 'max)
      (expr/parse-expr 1)
      (expr/parse-expr '(max 2 3))]}
    '(now)
    {:op :list
     :children [(expr/parse-expr 'now)]}
    '(in 1 (1 2 3))
    {:op :list
     :children
     [(expr/parse-expr 'in)
      (expr/parse-expr 1)
      (expr/parse-expr '(1 2 3))]}
    '(.-val :x)
    {:op :attr
     :children [:arg]
     :name :val
     :arg (expr/parse-expr :x)}
    '(.-val (new-emp))
    {:children [:arg]
     :name :val
     :op :attr
     :arg
     {:op :list
      :children
      [{:form 'new-emp
        :op :constant
        :type :symbol
        :val 'new-emp}]}}))

(deftest test-parse-expr-object
  (let [obj #?(:clj (Object.) :cljs (js/Object.))]
    (is (= (expr/parse-expr obj)
           {:form obj
            :op :constant
            :type :object
            :val obj}))))

(deftest test-parse-expr-select
  (is (= (expr/parse-expr (select db [1]))
         {:op :select
          :children [:exprs]
          :db db
          :exprs
          [{:val 1
            :type :number
            :op :constant
            :form 1}]})))

(deftest test-parse-expr-values
  (is (= (expr/parse-expr
          (values db [["MGM" "Horror"]
                      ["UA" "Sci-Fi"]]))
         {:op :values
          :db db
          :columns nil
          :type :exprs
          :values
          [[{:val "MGM"
             :type :string
             :op :constant
             :form "MGM"}
            {:val "Horror"
             :type :string
             :op :constant
             :form "Horror"}]
           [{:val "UA"
             :type :string
             :op :constant
             :form "UA"}
            {:val "Sci-Fi"
             :type :string
             :op :constant
             :form "Sci-Fi"}]]})))

(deftest test-parse-expr-list
  (is (= (expr/parse-expr '((lag :close) over (partition by :company-id order by :date desc)))
         '{:op :expr-list,
           :children
           [{:op :list,
             :children
             [{:form lag, :op :constant, :type :symbol, :val lag}
              {:children [:name],
               :name :close,
               :val :close,
               :op :column,
               :sqlingvo/op :column
               :sqlingvo.column/name "close"
               :form :close}]}
            {:form over, :op :constant, :type :symbol, :val over}
            {:op :list,
             :children
             [{:form partition, :op :constant, :type :symbol, :val partition}
              {:form by, :op :constant, :type :symbol, :val by}
              {:children [:name],
               :name :company-id,
               :val :company-id,
               :op :column,
               :sqlingvo/op :column
               :sqlingvo.column/name "company-id"
               :form :company-id}
              {:form order, :op :constant, :type :symbol, :val order}
              {:form by, :op :constant, :type :symbol, :val by}
              {:children [:name],
               :name :date,
               :val :date,
               :op :column,
               :form :date
               :sqlingvo/op :column
               :sqlingvo.column/name "date"}
              {:form desc, :op :constant, :type :symbol, :val desc}]}],
           :as nil})))

(deftest test-parse-expr-backquote
  (is (= (expr/parse-expr `(count :*))
         {:op :list
          :children
          [{:form #?(:clj 'clojure.core/count :cljs 'cljs.core/count)
            :op :constant
            :type :symbol
            :val 'count}
           {:children [:name]
            :name :*
            :val :*
            :op :column
            :sqlingvo/op :column
            :sqlingvo.column/name "*"
            :form :*}]}))
  (is (= (expr/parse-expr `((~'lag (~'count :*) 1)))
         (expr/parse-expr '((lag (count :*) 1)))))
  (is (= (expr/parse-expr
          `((~'lag (~'count :*) 1) ~'over
            (~'partition ~'by :quote-id ~'order ~'by (~'date :tweets.created-at))))
         (expr/parse-expr
          '((lag (count :*) 1) over
            (partition by :quote-id order by (date :tweets.created-at)))))))

(deftest test-parse-condition-backquote
  (is (= (expr/parse-condition `(in 1 (1 2 3)))
         {:op :condition,
          :condition (expr/parse-expr `(in 1 (1 2 3)))})))

(deftest test-parse-from
  (are [from expected]
      (= (expr/parse-from from) expected)
    "continents"
    {:children [:name]
     :form "continents"
     :name :continents
     :op :table
     :sqlingvo/op :table
     :sqlingvo.table/name "continents"
     :val "continents"}

    :continents
    {:children [:name]
     :form :continents
     :name :continents
     :op :table
     :sqlingvo/op :table
     :sqlingvo.table/name "continents"
     :val :continents}

    '(generate_series 0 10)
    {:op :list
     :children
     [(expr/parse-expr 'generate_series)
      (expr/parse-expr 0)
      (expr/parse-expr 10)]}

    (as :countries :c)
    {:op :alias
     :children [:expr :name]
     :expr
     {:children [:name]
      :name :countries
      :op :table
      :form :countries
      :sqlingvo/op :table
      :sqlingvo.table/name "countries"
      :val :countries}
     :name :c
     :columns []}))

(deftest test-parse-expr-array
  (is (= (expr/parse-expr [1 2])
         {:op :array
          :children
          [(expr/parse-expr 1)
           (expr/parse-expr 2)]})))

(deftest test-deref-statement
  (is (= @(select db [1])
         ["SELECT 1"])))

(deftest test-unintern-name
  (are [k expected]
      (= (expr/unintern-name k) expected)
    :a "a"
    :a/b "b"
    'a "a"
    'a/b "b"
    `a "a"
    `a "a"
    "a/b" "a/b"))
