(ns sqlingvo.view-test
  (:require #?(:clj [sqlingvo.test :refer [db sql=]]
               :cljs [sqlingvo.test :refer [db] :refer-macros [sql=]])
            [clojure.test :refer [deftest is]]
            [sqlingvo.core :as sql]))

(deftest test-drop-view
  (sql= (sql/drop-view :postgresql :order-summary)
        ["DROP VIEW \"order-summary\""])
  (sql= (sql/drop-view db :order-summary)
        ["DROP VIEW \"order-summary\""])
  (sql= (sql/drop-view db :order-summary
          (sql/if-exists true))
        ["DROP VIEW IF EXISTS \"order-summary\""])
  (sql= (sql/drop-view db :order-summary
          (sql/cascade true))
        ["DROP VIEW \"order-summary\" CASCADE"])
  (sql= (sql/drop-view db :order-summary
          (sql/restrict true))
        ["DROP VIEW \"order-summary\" RESTRICT"])
  (sql= (sql/drop-view db :order-summary
          (sql/if-exists true)
          (sql/cascade true))
        ["DROP VIEW IF EXISTS \"order-summary\" CASCADE"]))
