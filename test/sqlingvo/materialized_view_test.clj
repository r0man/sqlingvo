(ns sqlingvo.materialized-view-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.test :refer :all]
            [sqlingvo.core :refer :all]
            [sqlingvo.expr :refer :all]
            [sqlingvo.test :refer [db sql= with-stmt]]))

(deftest test-refresh-materialized-view
  (sql= (refresh-materialized-view db :order-summary)
        ["REFRESH MATERIALIZED VIEW \"order-summary\""])
  (sql= (refresh-materialized-view db :order-summary
          (concurrently true))
        ["REFRESH MATERIALIZED VIEW CONCURRENTLY \"order-summary\""])
  (sql= (refresh-materialized-view db :order-summary
          (with-data true))
        ["REFRESH MATERIALIZED VIEW \"order-summary\" WITH DATA"])
  (sql= (refresh-materialized-view db :order-summary
          (with-data false))
        ["REFRESH MATERIALIZED VIEW \"order-summary\" WITH NO DATA"])
  (sql= (refresh-materialized-view db :order-summary
          (concurrently true)
          (with-data false))
        ["REFRESH MATERIALIZED VIEW CONCURRENTLY \"order-summary\" WITH NO DATA"]))

(deftest test-drop-materialized-view
  (sql= (drop-materialized-view db :order-summary)
        ["DROP MATERIALIZED VIEW \"order-summary\""])
  (sql= (drop-materialized-view db :order-summary
          (if-exists true))
        ["DROP MATERIALIZED VIEW IF EXISTS \"order-summary\""])
  (sql= (drop-materialized-view db :order-summary
          (cascade true))
        ["DROP MATERIALIZED VIEW \"order-summary\" CASCADE"])
  (sql= (drop-materialized-view db :order-summary
          (restrict true))
        ["DROP MATERIALIZED VIEW \"order-summary\" RESTRICT"])
  (sql= (drop-materialized-view db :order-summary
          (if-exists true)
          (cascade true))
        ["DROP MATERIALIZED VIEW IF EXISTS \"order-summary\" CASCADE"]))
