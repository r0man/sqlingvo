(ns sqlingvo.expr-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [sqlingvo.expr :refer :all]
            [sqlingvo.core :refer :all]
            [sqlingvo.test :refer :all]
            [clojure.test :refer :all]))

(deftest test-parse-column
  (are [table expected] (= (parse-column table) expected)
    :id
    {:op :column
     :children [:name]
     :name :id
     :form :id}
    :continents.id
    {:op :column
     :children [:table :name]
     :table :continents
     :name :id
     :form :continents.id}
    :continents.id/i
    {:op :column
     :children [:table :name :as]
     :table :continents
     :name :id
     :as :i
     :form :continents.id/i}
    :public.continents.id
    {:op :column
     :children [:schema :table :name]
     :schema :public
     :table :continents
     :name :id
     :form :public.continents.id}
    :public.continents.id/i
    {:op :column
     :children [:schema :table :name :as]
     :schema :public
     :table :continents
     :name :id
     :as :i
     :form :public.continents.id/i})
  (is (= (parse-column :continents.id)
         (parse-column (parse-column :continents.id)))))

(deftest test-parse-table
  (are [table expected] (= (parse-table table) expected)
    :continents
    {:op :table
     :children [:name]
     :name :continents
     :form :continents}
    :continents/c
    {:op :table
     :children [:name :as]
     :name :continents
     :as :c
     :form :continents/c}
    :public.continents
    {:op :table
     :children [:schema :name]
     :schema :public
     :name :continents
     :form :public.continents}
    :public.continents/c
    {:op :table
     :children [:schema :name :as]
     :schema :public
     :name :continents
     :as :c
     :form :public.continents/c})
  (is (= (parse-table :public.continents/c)
         (parse-table (parse-table :public.continents/c)))))

(deftest test-parse-expr
  (are [expr expected] (= (parse-expr expr) expected)
    *
    {:children [:name]
     :name :*
     :op :column
     :form :*}
    1
    {:op :constant
     :form 1
     :literal? true
     :type :number
     :val 1}
    1.0
    {:op :constant
     :form 1.0
     :literal? true
     :type :number
     :val 1.0}
    "x"
    {:op :constant
     :form "x"
     :literal? true
     :type :string
     :val "x"}
    `(= 1 1)
    {:op :fn
     :children [:args]
     :name "="
     :args [(parse-expr 1) (parse-expr 1)]}
    '(= :name "Europe")
    {:op :fn
     :children [:args]
     :name "="
     :args [(parse-expr :name) (parse-expr "Europe")]}
    '(max 1 2)
    {:op :fn
     :children [:args]
     :name "max"
     :args [(parse-expr 1) (parse-expr 2)]}
    '(max 1 (max 2 3))
    {:op :fn
     :children [:args]
     :name "max"
     :args [(parse-expr 1)
            {:op :fn
             :children [:args]
             :name "max"
             :args [(parse-expr 2) (parse-expr 3)]}]}
    '(now)
    {:op :fn
     :children [:args]
     :name "now"
     :args []}
    '(in 1 (1 2 3))
    {:op :fn
     :children [:args]
     :name "in"
     :args [(parse-expr 1)
            {:op :list
             :children [(parse-expr 1)
                        (parse-expr 2)
                        (parse-expr 3)]
             :as nil}]}
    '(.-val :x)
    {:op :attr
     :children [:arg]
     :name :val
     :arg (parse-expr :x)}
    '(.-val (new-emp))
    {:op :attr
     :children [:arg]
     :name :val
     :arg {:op :fn
           :children [:args]
           :name "new-emp"
           :args []}}))

(deftest test-parse-expr-select
  (is (= (parse-expr (select db [1]))
         {:op :select
          :children [:exprs]
          :db db
          :exprs
          [{:val 1
            :type :number
            :op :constant
            :literal? true
            :form 1}]})))

(deftest test-parse-expr-values
  (is (= (parse-expr
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
             :literal? true
             :form "MGM"}
            {:val "Horror"
             :type :string
             :op :constant
             :literal? true
             :form "Horror"}]
           [{:val "UA"
             :type :string
             :op :constant
             :literal? true
             :form "UA"}
            {:val "Sci-Fi"
             :type :string
             :op :constant
             :literal? true
             :form "Sci-Fi"}]]})))

(deftest test-parse-expr-list
  (is (= (parse-expr '((lag :close) over (partition by :company-id order by :date desc)))
         '{:op :expr-list
           :as nil
           :children
           [{:args [{:children [:name]
                     :name :close
                     :op :column
                     :form :close}]
             :children [:args]
             :name "lag"
             :op :fn}
            {:val over
             :type :symbol
             :op :constant
             :literal? true
             :form over}
            {:args
             [{:val by
               :type :symbol
               :op :constant
               :literal? true
               :form by}
              {:children [:name]
               :name :company-id
               :op :column
               :form :company-id}
              {:val order
               :type :symbol
               :op :constant
               :literal? true
               :form order}
              {:val by
               :type :symbol
               :op :constant
               :literal? true
               :form by}
              {:children [:name]
               :name :date
               :op :column
               :form :date}
              {:val desc
               :type :symbol
               :op :constant
               :literal? true
               :form desc}]
             :children [:args]
             :name "partition"
             :op :fn}]})))

(deftest test-parse-expr-backquote
  (is (= (parse-expr `(count :*))
         (parse-expr '(count :*))))
  (is (= (parse-expr `((~'lag (~'count :*) 1)))
         (parse-expr '((lag (count :*) 1)))))
  (is (= (parse-expr `((~'lag (~'count :*) 1) ~'over (~'partition ~'by :quote-id ~'order ~'by (~'date :tweets.created-at))))
         (parse-expr '((lag (count :*) 1) over (partition by :quote-id order by (date :tweets.created-at)))))))

(deftest test-parse-condition-backquote
  (is (= (parse-condition '(in 1 (1 2 3)))
         (parse-condition `(in 1 (1 2 3))))))

(deftest test-parse-from
  (are [from expected]
      (is (= expected (parse-from from)))
    "continents"
    {:op :table
     :children [:name]
     :form "continents"
     :name :continents}
    :continents
    {:op :table
     :children [:name]
     :form :continents
     :name :continents}
    '(generate_series 0 10)
    {:op :fn
     :children [:args]
     :name "generate_series"
     :args [(parse-expr 0) (parse-expr 10)]}
    (as :countries :c)
    {:op :alias,
     :children [:expr :name],
     :expr
     {:children [:name]
      :name :countries
      :op :table
      :form :countries},
     :name :c
     :columns []}))

(deftest test-qualified-name
  (are [arg expected]
      (is (= expected (qualified-name arg)))
    nil ""
    "" ""
    "continents" "continents"
    :continents "continents"
    :public.continents "public.continents"))

(deftest test-type-keyword
  (are [x expected]
      (= expected (type-keyword x))
    1 :number
    "1" :string
    ))

(deftest test-unintern-name
  (are [k expected]
      (= (unintern-name k) expected)
    :x "x"
    (keyword "m/s->km/h") "m/s->km/h"
    (keyword "sqlingvo.expr-test/m/s->km/h") "m/s->km/h"))
