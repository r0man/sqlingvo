(ns sqlingvo.util-test
  (:require [clojure.test :refer :all]
            [sqlingvo.util :refer :all]))

(deftest test-parse-column
  (are [table expected]
    (do (is (= expected (parse-column table)))
        (is (= expected (parse-column table))))
    :id
    {:op :column :schema nil :table nil :name :id :as nil}
    :continents.id
    {:op :column :schema nil :table :continents :name :id :as nil}
    :continents.id/i
    {:op :column :schema nil :table :continents :name :id :as :i}
    :public.continents.id
    {:op :column :schema :public :table :continents :name :id :as nil}
    :public.continents.id/i
    {:op :column :schema :public :table :continents :name :id :as :i})
  (is (= (parse-column :continents.id)
         (parse-column (parse-column :continents.id)))))

(deftest test-parse-table
  (are [table expected]
    (do (is (= expected (parse-table table)))
        (is (= expected (parse-table (qualified-name table)))))
    :continents
    {:op :table :schema nil :name :continents :as nil}
    :continents/c
    {:op :table :schema nil :name :continents :as :c}
    :public.continents
    {:op :table :schema :public :name :continents :as nil}
    :public.continents/c
    {:op :table :schema :public :name :continents :as :c})
  (is (= (parse-table :public.continents/c)
         (parse-table (parse-table :public.continents/c)))))

(deftest test-parse-expr
  (are [expr expected]
    (is (= expected (parse-expr expr)))
    *
    {:op :constant :form '*}
    1
    {:op :constant :form 1}
    1.0
    {:op :constant :form 1.0}
    "x"
    {:op :constant :form "x"}
    `(= 1 1)
    {:op :fn :name := :args [{:op :constant :form 1} {:op :constant :form 1}]}
    '(= :name "Europe")
    {:op :fn :name := :args [{:op :column :schema nil :table nil :name :name :as nil} {:op :constant :form "Europe"}]}
    '(max 1 2)
    {:op :fn :name :max :args [{:op :constant :form 1} {:op :constant :form 2}]}
    '(max 1 (max 2 3))
    {:op :fn :name :max :args [{:op :constant :form 1}
                               {:op :fn :name :max
                                :args [{:op :constant :form 2}{:op :constant :form 3} ]}]}
    '(now)
    {:op :fn :name :now :args []}
    '(in 1 (1 2 3))
    {:op :fn
     :name :in
     :args [{:op :constant :form 1}
            {:op :list
             :children [{:op :constant :form 1}
                        {:op :constant :form 2}
                        {:op :constant :form 3}] :as nil}]}
    '((lag :close) over (partition by :company-id order by :date desc))
    '{:op :expr-list
      :as nil
      :children
      [{:op :fn
        :name :lag
        :args [{:op :column :schema nil :table nil :name :close :as nil}]}
       {:op :constant :form over}
       {:op :fn
        :name :partition
        :args
        [{:op :constant :form by}
         {:op :column :schema nil :table nil :name :company-id :as nil}
         {:op :constant :form order}
         {:op :constant :form by}
         {:op :column :schema nil :table nil :name :date :as nil}
         {:op :constant :form desc}]}]}))

(deftest test-parse-condition-backquote
  (is (= (parse-condition '(in 1 (1 2 3)))
         (parse-condition `(in 1 (1 2 3))))))

(deftest test-parse-from
  (are [from expected]
    (is (= expected (parse-from from)))
    "continents"
    {:op :table, :schema nil, :name :continents, :as nil}
    :continents
    {:op :table, :schema nil, :name :continents, :as nil}
    '(generate_series 0 10)
    {:op :fn, :name :generate_series, :args [{:op :constant, :form 0} {:op :constant, :form 10}]}))

(deftest test-qualified-name
  (are [arg expected]
    (is (= expected (qualified-name arg)))
    nil ""
    "" ""
    "continents" "continents"
    :continents "continents"
    :public.continents "public.continents"))
