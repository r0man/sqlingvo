(ns sqlingvo.with-test
  (:require #?(:clj [sqlingvo.test :refer [db sql=]]
               :cljs [sqlingvo.test :refer [db] :refer-macros [sql=]])
            [clojure.test :refer [deftest is]]
            [sqlingvo.core :as sql]))

(deftest test-with-keyword-db
  (sql= (sql/with :postgresql [:a (sql/select :postgresql [:*] (sql/from :b))]
          (sql/select :postgresql [:*]
            (sql/from :a)
            (sql/where '(= 1 1))))
        ["WITH \"a\" AS (SELECT * FROM \"b\") SELECT * FROM \"a\" WHERE (1 = 1)"]))

(deftest test-with-query
  (sql= (sql/with db [:regional-sales
                      (sql/select db [:region (sql/as '(sum :amount) :total-sales)]
                        (sql/from :orders)
                        (sql/group-by :region))
                      :top-regions
                      (sql/select db [:region]
                        (sql/from :regional-sales)
                        (sql/where `(> :total-sales
                                       ~(sql/select db ['(/ (sum :total-sales) 10)]
                                          (sql/from :regional-sales)))))]
          (sql/select db [:region :product
                          (sql/as '(sum :quantity) :product-units)
                          (sql/as '(sum :amount) :product-sales)]
            (sql/from :orders)
            (sql/where `(in :region
                            ~(sql/select db [:region]
                               (sql/from :top-regions))))
            (sql/group-by :region :product)))
        [(str "WITH \"regional-sales\" AS ("
              "SELECT \"region\", sum(\"amount\") AS \"total-sales\" "
              "FROM \"orders\" GROUP BY \"region\"), "
              "\"top-regions\" AS ("
              "SELECT \"region\" "
              "FROM \"regional-sales\" "
              "WHERE (\"total-sales\" > (SELECT (sum(\"total-sales\") / 10) FROM \"regional-sales\"))) "
              "SELECT \"region\", \"product\", sum(\"quantity\") AS \"product-units\", sum(\"amount\") AS \"product-sales\" "
              "FROM \"orders\" "
              "WHERE \"region\" IN (SELECT \"region\" "
              "FROM \"top-regions\") "
              "GROUP BY \"region\", \"product\"")]))

(deftest test-with-modify-data
  (sql= (sql/with db [:moved-rows
                      (sql/delete db :products
                        (sql/where '(and (>= :date "2010-10-01")
                                         (< :date "2010-11-01")))
                        (sql/returning :*))]
          (sql/insert db :product-logs []
            (sql/select db [:*] (sql/from :moved-rows))))
        [(str "WITH \"moved-rows\" AS ("
              "DELETE FROM \"products\" "
              "WHERE ((\"date\" >= ?) and (\"date\" < ?)) "
              "RETURNING *) "
              "INSERT INTO \"product-logs\" SELECT * FROM \"moved-rows\"")
         "2010-10-01" "2010-11-01"]))

(deftest test-with-counter-update
  (sql= (sql/with db [:upsert (sql/update db :counter-table
                                '((= counter counter+1))
                                (sql/where '(= :id "counter-name"))
                                (sql/returning :*))]
          (sql/insert db :counter-table [:id :counter]
            (sql/select db ["counter-name" 1])
            (sql/where `(not-exists
                         ~(sql/select db [:*]
                            (sql/from :upsert))))))
        [(str "WITH \"upsert\" AS ("
              "UPDATE \"counter-table\" SET counter = counter+1 "
              "WHERE (\"id\" = ?) RETURNING *) "
              "INSERT INTO \"counter-table\" (\"id\", \"counter\") "
              "SELECT ?, 1 "
              "WHERE (NOT EXISTS (SELECT * FROM \"upsert\"))")
         "counter-name" "counter-name"]))

(deftest test-with-delete
  (sql= (sql/with db [:t (sql/delete db :foo)]
          (sql/delete db :bar))
        ["WITH \"t\" AS (DELETE FROM \"foo\") DELETE FROM \"bar\""]))

(deftest test-with-compose
  (sql= (sql/with db [:a (sql/select db [:*] (sql/from :b))]
          (sql/compose
           (sql/select db [:*] (sql/from :a))
           (sql/where '(= 1 1))))
        ["WITH \"a\" AS (SELECT * FROM \"b\") SELECT * FROM \"a\" WHERE (1 = 1)"]))

(deftest test-insert-from-with
  (sql= (sql/insert db :c [:id]
          (sql/with db [:a (sql/select db [:a.id]
                             (sql/from :a))]
            (sql/select db [:b.id]
              (sql/from :b)
              (sql/where `(in :b.id
                              ~(sql/select db [:a.id]
                                 (sql/from :a)))))))
        [(str "INSERT INTO \"c\" (\"id\") "
              "WITH \"a\" AS (SELECT \"a\".\"id\" FROM \"a\") "
              "SELECT \"b\".\"id\" FROM \"b\" WHERE \"b\".\"id\" "
              "IN (SELECT \"a\".\"id\" FROM \"a\")")]))
