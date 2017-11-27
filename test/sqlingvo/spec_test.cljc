(ns sqlingvo.spec-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :refer [deftest is]]
            [sqlingvo.spec :as spec]))

(deftest test-column
  (is (s/exercise :sqlingvo/column)))

(deftest test-column-identifier
  (is (s/exercise :sqlingvo.column/identifier)))

(deftest test-table
  (is (s/exercise :sqlingvo/table)))

(deftest test-table-identifier
  (is (s/exercise :sqlingvo.table/identifier)))
