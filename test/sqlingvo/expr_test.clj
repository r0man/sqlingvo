(ns sqlingvo.expr-test
  (:require [sqlingvo.expr :refer :all]
            [clojure.test :refer :all]))

(deftest test-parse-column
  (are [table expected]
    (do (is (= expected (parse-column table)))
        (is (= expected (parse-column table))))
    :id
    {:op :column :children [:name] :name :id}
    :continents.id
    {:op :column :children [:table :name] :table :continents :name :id}
    :continents.id/i
    {:op :column :children [:table :name :as] :table :continents :name :id :as :i}
    :public.continents.id
    {:op :column :children [:schema :table :name] :schema :public :table :continents :name :id}
    :public.continents.id/i
    {:op :column :children [:schema :table :name :as] :schema :public :table :continents :name :id :as :i})
  (is (= (parse-column :continents.id)
         (parse-column (parse-column :continents.id)))))

(deftest test-parse-table
  (are [table expected]
    (do (is (= expected (parse-table table)))
        (is (= expected (parse-table (qualified-name table)))))
    :continents
    {:op :table :children [:name] :name :continents}
    :continents/c
    {:op :table :children [:name :as] :name :continents :as :c}
    :public.continents
    {:op :table :children [:schema :name] :schema :public :name :continents}
    :public.continents/c
    {:op :table :children [:schema :name :as] :schema :public :name :continents :as :c})
  (is (= (parse-table :public.continents/c)
         (parse-table (parse-table :public.continents/c)))))

(deftest test-parse-expr
  (are [expr expected]
    (is (= expected (parse-expr expr)))
    *
    {:children [:name] :name :* :op :column}
    1
    {:op :constant, :form 1, :literal? true, :type :number, :val 1}
    1.0
    {:op :constant, :form 1.0, :literal? true, :type :number, :val 1.0}
    "x"
    {:op :constant, :form "x", :literal? true, :type :string, :val "x"}
    `(= 1 1)
    {:op :fn
     :children [:args]
     :name :=
     :args [(parse-expr 1) (parse-expr 1)]}
    '(= :name "Europe")
    {:op :fn
     :children [:args]
     :name :=
     :args [(parse-expr :name) (parse-expr "Europe")]}
    '(max 1 2)
    {:op :fn
     :children [:args]
     :name :max
     :args [(parse-expr 1) (parse-expr 2)]}
    '(max 1 (max 2 3))
    {:op :fn
     :children [:args]
     :name :max
     :args [(parse-expr 1)
            {:op :fn
             :children [:args]
             :name :max
             :args [(parse-expr 2) (parse-expr 3)]}]}
    '(now)
    {:op :fn
     :children [:args]
     :name :now
     :args []}
    '(in 1 (1 2 3))
    {:op :fn
     :children [:args]
     :name :in
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
     :arg {:op :fn,
           :children [:args]
           :name :new-emp
           :args []}}))

(deftest test-parse-expr-list
  (is (= (parse-expr '((lag :close) over (partition by :company-id order by :date desc)))
         '{:op :expr-list
           :children
           [{:args [{:children [:name] :name :close :op :column}]
             :children [:args]
             :name :lag
             :op :fn}
            {:val over
             :type :symbol
             :op :constant
             :literal? true
             :form over}
            {:args
             [{:val by :type :symbol :op :constant :literal? true :form by}
              {:children [:name] :name :company-id :op :column}
              {:val order :type :symbol :op :constant :literal? true :form order}
              {:val by :type :symbol :op :constant :literal? true :form by}
              {:children [:name] :name :date :op :column}
              {:val desc :type :symbol :op :constant :literal? true :form desc}]
             :children [:args] :name :partition :op :fn}]
           :as nil})))

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
     :name :continents}
    :continents
    {:op :table
     :children [:name]
     :name :continents}
    '(generate_series 0 10)
    {:op :fn
     :children [:args]
     :name :generate_series
     :args [(parse-expr 0) (parse-expr 10)]}))

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
