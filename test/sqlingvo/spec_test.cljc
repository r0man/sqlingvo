(ns sqlingvo.spec-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :refer [deftest]]
            [sqlingvo.spec :as spec]))

(deftest test-column
  (s/exercise :sqlingvo/column))

(deftest test-table
  (s/exercise :sqlingvo/table))
