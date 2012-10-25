(ns sqlingvo.test.util
  (:use clojure.test
        sqlingvo.util))

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
       {:op :column :schema :public :table :continents :name :id :as :i}))

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
       {:op :table :schema :public :name :continents :as :c}))

(deftest test-parse-expr
  (are [expr expected]
       (is (= expected (parse-expr expr)))
       '(= 1 1)
       {:op :fn :name := :args [{:op :constant :form 1} {:op :constant :form 1}]}
       `(= 1 1)
       {:op :fn :name := :args [{:op :constant :form 1} {:op :constant :form 1}]}
       '(= :name "Europe")
       {:op :fn :name := :args [{:op :column :schema nil :table nil :name :name :as nil} {:op :constant :form "Europe"}]}))

(deftest test-qualified-name
  (are [arg expected]
       (is (= expected (qualified-name arg)))
       nil ""
       "" ""
       "continents" "continents"
       :continents "continents"
       :public.continents "public.continents"))