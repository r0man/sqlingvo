(ns sqlingvo.materialized-view-test
  (:require #?(:clj [sqlingvo.test :refer [db sql=]]
               :cljs [sqlingvo.test :refer [db] :refer-macros [sql=]])
            [clojure.test :refer [deftest is]]
            [sqlingvo.core :as sql]))

(deftest test-create-materialized-view-as-values
  (sql= (sql/create-materialized-view db :pseudo-source [:key :value]
          (sql/values [["a" 1] ["a" 2] ["a" 3] ["a" 4] ["b" 5] ["c" 6] ["c" 7]]))
        ["CREATE MATERIALIZED VIEW \"pseudo-source\" (\"key\", \"value\") AS VALUES (?, 1), (?, 2), (?, 3), (?, 4), (?, 5), (?, 6), (?, 7)"
         "a" "a" "a" "a" "b" "c" "c"]))

(deftest test-create-materialized-view-as-select
  (sql= (sql/create-materialized-view db :key_sums []
          (sql/select db [:key '(sum :value)]
            (sql/from :pseudo_source)
            (sql/group-by :key)))
        ["CREATE MATERIALIZED VIEW \"key_sums\" AS SELECT \"key\", sum(\"value\") FROM \"pseudo_source\" GROUP BY \"key\""]))

(deftest test-create-materialized-view-as-select-if-exists
  (sql= (sql/create-materialized-view db :pseudo-source [:key :value]
          (sql/values [["a" 1]])
          (sql/if-not-exists true))
        ["CREATE MATERIALIZED VIEW IF NOT EXISTS \"pseudo-source\" (\"key\", \"value\") AS VALUES (?, 1)" "a"]))

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
