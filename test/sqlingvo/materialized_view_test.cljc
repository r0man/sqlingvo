(ns sqlingvo.materialized-view-test
  (:require #?(:clj [sqlingvo.test :refer [db sql=]]
               :cljs [sqlingvo.test :refer [db] :refer-macros [sql=]])
            [clojure.test :refer [deftest is]]
            [sqlingvo.core :as sql]))

(deftest test-refresh-materialized-view
  (sql= (sql/refresh-materialized-view :postgresql :order-summary)
        ["REFRESH MATERIALIZED VIEW \"order-summary\""])
  (sql= (sql/refresh-materialized-view db :order-summary)
        ["REFRESH MATERIALIZED VIEW \"order-summary\""])
  (sql= (sql/refresh-materialized-view db :order-summary
          (sql/concurrently true))
        ["REFRESH MATERIALIZED VIEW CONCURRENTLY \"order-summary\""])
  (sql= (sql/refresh-materialized-view db :order-summary
          (sql/with-data true))
        ["REFRESH MATERIALIZED VIEW \"order-summary\" WITH DATA"])
  (sql= (sql/refresh-materialized-view db :order-summary
          (sql/with-data false))
        ["REFRESH MATERIALIZED VIEW \"order-summary\" WITH NO DATA"])
  (sql= (sql/refresh-materialized-view db :order-summary
          (sql/concurrently true)
          (sql/with-data false))
        ["REFRESH MATERIALIZED VIEW CONCURRENTLY \"order-summary\" WITH NO DATA"]))

(deftest test-drop-materialized-view
  (sql= (sql/drop-materialized-view :postgresql :order-summary)
        ["DROP MATERIALIZED VIEW \"order-summary\""])
  (sql= (sql/drop-materialized-view db :order-summary)
        ["DROP MATERIALIZED VIEW \"order-summary\""])
  (sql= (sql/drop-materialized-view db :order-summary
          (sql/if-exists true))
        ["DROP MATERIALIZED VIEW IF EXISTS \"order-summary\""])
  (sql= (sql/drop-materialized-view db :order-summary
          (sql/cascade true))
        ["DROP MATERIALIZED VIEW \"order-summary\" CASCADE"])
  (sql= (sql/drop-materialized-view db :order-summary
          (sql/restrict true))
        ["DROP MATERIALIZED VIEW \"order-summary\" RESTRICT"])
  (sql= (sql/drop-materialized-view db :order-summary
          (sql/if-exists true)
          (sql/cascade true))
        ["DROP MATERIALIZED VIEW IF EXISTS \"order-summary\" CASCADE"]))
