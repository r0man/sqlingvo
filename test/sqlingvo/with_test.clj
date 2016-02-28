(ns sqlingvo.with-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.test :refer :all]
            [sqlingvo.core :refer :all]
            [sqlingvo.test :refer [db sql= with-stmt]]))

(deftest test-with-query
  (sql= (with db [:regional-sales
                  (select db [:region (as '(sum :amount) :total-sales)]
                    (from :orders)
                    (group-by :region))
                  :top-regions
                  (select db [:region]
                    (from :regional-sales)
                    (where `(> :total-sales
                               ~(select db ['(/ (sum :total-sales) 10)]
                                  (from :regional-sales)))))]
          (select db [:region :product
                      (as '(sum :quantity) :product-units)
                      (as '(sum :amount) :product-sales)]
            (from :orders)
            (where `(in :region ~(select db [:region]
                                   (from :top-regions))))
            (group-by :region :product)))
        [(str "WITH \"regional-sales\" AS ("
              "SELECT \"region\", \"sum\"(\"amount\") AS \"total-sales\" "
              "FROM \"orders\" GROUP BY \"region\"), "
              "\"top-regions\" AS ("
              "SELECT \"region\" "
              "FROM \"regional-sales\" "
              "WHERE (\"total-sales\" > (SELECT (\"sum\"(\"total-sales\") / 10) FROM \"regional-sales\"))) "
              "SELECT \"region\", \"product\", \"sum\"(\"quantity\") AS \"product-units\", \"sum\"(\"amount\") AS \"product-sales\" "
              "FROM \"orders\" "
              "WHERE \"region\" IN (SELECT \"region\" "
              "FROM \"top-regions\") "
              "GROUP BY \"region\", \"product\"")]))

(deftest test-with-modify-data
  (sql= (with db [:moved-rows
                  (delete db :products
                    (where '(and (>= :date "2010-10-01")
                                 (< :date "2010-11-01")))
                    (returning :*))]
          (insert db :product-logs []
            (select db [:*] (from :moved-rows))))
        [(str "WITH \"moved-rows\" AS ("
              "DELETE FROM \"products\" "
              "WHERE ((\"date\" >= ?) and (\"date\" < ?)) "
              "RETURNING *) "
              "INSERT INTO \"product-logs\" SELECT * FROM \"moved-rows\"")
         "2010-10-01" "2010-11-01"]))

(deftest test-with-counter-update
  (sql= (with db [:upsert (update db :counter-table
                            '((= counter counter+1))
                            (where '(= :id "counter-name"))
                            (returning *))]
          (insert db :counter-table [:id :counter]
            (select db ["counter-name" 1])
            (where `(not-exists ~(select db [*] (from :upsert))))))
        [(str "WITH \"upsert\" AS ("
              "UPDATE \"counter-table\" SET counter = counter+1 "
              "WHERE (\"id\" = ?) RETURNING *) "
              "INSERT INTO \"counter-table\" (\"id\", \"counter\") "
              "SELECT ?, 1 "
              "WHERE (NOT EXISTS (SELECT * FROM \"upsert\"))")
         "counter-name" "counter-name"]))

(deftest test-with-delete
  (sql= (with db [:t (delete db :foo)]
          (delete db :bar))
        ["WITH \"t\" AS (DELETE FROM \"foo\") DELETE FROM \"bar\""]))

(deftest test-with-compose
  (sql= (compose (with db [:a (select db [:*] (from :b))]
                   (select db [:*] (from :a)))
                 (where '(= 1 1)))
        ["WITH \"a\" AS (SELECT * FROM \"b\") SELECT * FROM \"a\" WHERE (1 = 1)"]))
