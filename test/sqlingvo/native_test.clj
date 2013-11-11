(ns sqlingvo.native-test
  (:refer-clojure :exclude [distinct group-by])
  (:require [sqlingvo.compiler :refer [compile-stmt]])
  (:use clojure.test
        sqlingvo.native))

(deftest test-select
  (let [[exprs state] ((select [1]) {})]
    (is (= [{:op :constant, :form 1}] exprs))
    (is (= {:exprs [{:op :constant, :form 1}], :op :select} state))))

(deftest test-from
  (let [[from state] ((from :continents) {})]
    (is (= [{:op :table, :schema nil, :name :continents, :as nil}] from))
    (is (= {:from [{:op :table, :schema nil, :name :continents, :as nil}]} state))))

(deftest test-select-from
  (let [[exprs state] ((select [:*] (from :continents)) {})]
    (is (= [{:op :column, :schema nil, :table nil, :name :*, :as nil}] exprs))
    (is (= {:op :select,
            :exprs [{:op :column, :schema nil, :table nil, :name :*, :as nil}],
            :from [{:op :table, :schema nil, :name :continents, :as nil}]}))))
