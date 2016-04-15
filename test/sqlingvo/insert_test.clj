(ns sqlingvo.insert-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.test :refer :all]
            [sqlingvo.core :refer :all]
            [sqlingvo.expr :refer :all]
            [sqlingvo.test :refer [db sql= with-stmt]]))

(deftest test-insert-default-values
  (sql= (insert db :films []
          (values :default))
        ["INSERT INTO \"films\" DEFAULT VALUES"]))

(deftest test-insert-single-row-as-seq
  (sql= (insert db :films []
          (values [{:code "T-601" :title "Yojimbo" :did 106 :date-prod "1961-06-16" :kind "Drama"}]))
        ["INSERT INTO \"films\" (\"code\", \"date-prod\", \"did\", \"kind\", \"title\") VALUES (?, ?, 106, ?, ?)"
         "T-601" "1961-06-16" "Drama" "Yojimbo"]))

(deftest test-insert-multi-row
  (sql= (insert db :films []
          (values [{:code "B6717" :title "Tampopo" :did 110 :date-prod "1985-02-10" :kind "Comedy"},
                   {:code "HG120" :title "The Dinner Game" :did 140 :date-prod "1985-02-10":kind "Comedy"}]))

        ["INSERT INTO \"films\" (\"code\", \"date-prod\", \"did\", \"kind\", \"title\") VALUES (?, ?, 110, ?, ?), (?, ?, 140, ?, ?)"
         "B6717" "1985-02-10" "Comedy" "Tampopo"
         "HG120" "1985-02-10" "Comedy" "The Dinner Game"]))

(deftest test-insert-returning
  (sql= (insert db :distributors []
          (values [{:did 106 :dname "XYZ Widgets"}])
          (returning :*))
        ["INSERT INTO \"distributors\" (\"did\", \"dname\") VALUES (106, ?) RETURNING *" "XYZ Widgets"]))

(deftest test-insert-subselect
  (sql= (insert db :films []
          (select db [*]
            (from :tmp-films)
            (where '(< :date-prod "2004-05-07"))))
        ["INSERT INTO \"films\" SELECT * FROM \"tmp-films\" WHERE (\"date-prod\" < ?)" "2004-05-07"]))

(deftest test-insert-airports
  (with-stmt
    [(str "INSERT INTO \"airports\" (\"country-id\", \"name\", \"gps-code\", \"iata-code\", \"wikipedia-url\", \"location\") "
          "SELECT DISTINCT ON (\"a\".\"iata-code\") \"c\".\"id\", \"a\".\"name\", \"a\".\"gps-code\", \"a\".\"iata-code\", \"a\".\"wikipedia\", \"a\".\"geom\" "
          "FROM \"natural-earth\".\"airports\" \"a\" JOIN \"countries\" \"c\" ON (\"c\".\"geography\" && \"a\".\"geom\") "
          "LEFT JOIN \"airports\" ON (\"airports\".\"iata-code\" = \"a\".\"iata-code\") "
          "WHERE ((\"a\".\"gps-code\" IS NOT NULL) and (\"a\".\"iata-code\" IS NOT NULL) and (\"airports\".\"iata-code\" IS NULL))")]
    (insert db :airports [:country-id, :name :gps-code :iata-code :wikipedia-url :location]
      (select db (distinct [:c.id :a.name :a.gps-code :a.iata-code :a.wikipedia :a.geom] :on [:a.iata-code])
        (from (as :natural-earth.airports :a))
        (join (as :countries :c) '(on (:&& :c.geography :a.geom)))
        (join :airports '(on (= :airports.iata-code :a.iata-code)) :type :left)
        (where '(and (is-not-null :a.gps-code)
                     (is-not-null :a.iata-code)
                     (is-null :airports.iata-code)))))))

(deftest test-insert-only-columns
  (sql= (insert db :x [:a :b] (values [{:a 1 :b 2 :c 3}]))
        ["INSERT INTO \"x\" (\"a\", \"b\") VALUES (1, 2)"]))

(deftest test-insert-values-with-fn-call
  (sql= (insert db :x [:a :b]
          (values [{:a 1 :b '(lower "B")}
                   {:a 2 :b "b"}]))
        ["INSERT INTO \"x\" (\"a\", \"b\") VALUES (1, lower(?)), (2, ?)" "B" "b"]))

(deftest test-insert-fixed-columns-mixed-values
  (sql= (insert db :table [:a :b]
          (values [{:a 1 :b 2} {:b 3} {:c 3}]))
        [(str "INSERT INTO \"table\" (\"a\", \"b\") VALUES (1, 2), "
              "(NULL, 3), (NULL, NULL)")]))

(deftest test-insert-fixed-columns-mixed-values-2
  (sql= (insert db :quotes [:id :exchange-id :company-id
                            :symbol :created-at :updated-at]
          (values [{:updated-at #inst "2012-11-02T18:22:59.688-00:00"
                    :created-at #inst "2012-11-02T18:22:59.688-00:00"
                    :symbol "MSFT"
                    :exchange-id 2
                    :company-id 5
                    :id 5}
                   {:updated-at #inst "2012-11-02T18:22:59.688-00:00"
                    :created-at #inst "2012-11-02T18:22:59.688-00:00"
                    :symbol "SPY"
                    :exchange-id 2
                    :id 6}]))
        [(str "INSERT INTO \"quotes\" (\"id\", \"exchange-id\", "
              "\"company-id\", \"symbol\", \"created-at\", \"updated-at\") "
              "VALUES (5, 2, 5, ?, ?, ?), (6, 2, NULL, ?, ?, ?)")
         "MSFT"
         #inst "2012-11-02T18:22:59.688-00:00"
         #inst "2012-11-02T18:22:59.688-00:00"
         "SPY"
         #inst "2012-11-02T18:22:59.688-00:00"
         #inst "2012-11-02T18:22:59.688-00:00"]))

(deftest test-insert-array
  (sql= (insert db :test [:x] (values [{:x ["1" 2]}]))
        ["INSERT INTO \"test\" (\"x\") VALUES (ARRAY[?, 2])" "1"]))

(deftest test-insert-on-conflict-do-update
  (sql= (insert db :distributors [:did :dname]
          (values [{:did 5 :dname "Gizmo Transglobal"}
                   {:did 6 :dname "Associated Computing, Inc"}])
          (on-conflict [:did]
            (do-update {:dname :EXCLUDED.dname})))
        [(str "INSERT INTO \"distributors\" (\"did\", \"dname\") "
              "VALUES (5, ?), (6, ?) "
              "ON CONFLICT (\"did\") "
              "DO UPDATE SET \"dname\" = EXCLUDED.\"dname\"")
         "Gizmo Transglobal"
         "Associated Computing, Inc"]))

(deftest test-insert-on-conflict-do-nothing
  (sql= (insert db :distributors [:did :dname]
          (values [{:did 7 :dname "Redline GmbH"}])
          (on-conflict [:did]
            (do-nothing)))
        [(str "INSERT INTO \"distributors\" (\"did\", \"dname\") "
              "VALUES (7, ?) "
              "ON CONFLICT (\"did\") "
              "DO NOTHING")
         "Redline GmbH"]))

(deftest test-insert-on-conflict-do-nothing-returning
  (sql= (insert db :distributors [:did :dname]
          (values [{:did 7 :dname "Redline GmbH"}])
          (on-conflict [:did]
            (do-nothing))
          (returning :*))
        [(str "INSERT INTO \"distributors\" (\"did\", \"dname\") "
              "VALUES (7, ?) "
              "ON CONFLICT (\"did\") "
              "DO NOTHING "
              "RETURNING *")
         "Redline GmbH"]))

(deftest test-insert-on-conflict-do-update-where
  (sql= (insert db (as :distributors :d) [:did :dname]
          (values [{:did 8 :dname "Anvil Distribution"}])
          (on-conflict [:did]
            (do-update {:dname '(:|| :EXCLUDED.dname " (formerly " :d.dname ")")})
            (where '(:<> :d.zipcode "21201"))))
        [(str "INSERT INTO \"distributors\" AS \"d\" (\"did\", \"dname\") "
              "VALUES (8, ?) "
              "ON CONFLICT (\"did\") "
              "DO UPDATE SET \"dname\" = (EXCLUDED.\"dname\" || ? || \"d\".\"dname\" || ?) "
              "WHERE (\"d\".\"zipcode\" <> ?)")
         "Anvil Distribution" " (formerly " ")" "21201"]))

(deftest test-insert-on-conflict-where-do-nothing
  (sql= (insert db :distributors [:did :dname]
          (values [{:did 10 :dname "Conrad International"}])
          (on-conflict [:did]
            (where '(= :is-active true))
            (do-nothing)))
        [(str "INSERT INTO \"distributors\" (\"did\", \"dname\") "
              "VALUES (10, ?) "
              "ON CONFLICT (\"did\") "
              "WHERE (\"is-active\" = ?) DO NOTHING")
         "Conrad International" true]))

(deftest test-insert-on-conflict-on-constraint-do-nothing
  (sql= (insert db :distributors [:did :dname]
          (values [{:did 9 :dname "Antwerp Design"}])
          (on-conflict-on-constraint :distributors_pkey
            (do-nothing)))
        [(str "INSERT INTO \"distributors\" (\"did\", \"dname\") "
              "VALUES (9, ?) "
              "ON CONFLICT ON CONSTRAINT \"distributors_pkey\" "
              "DO NOTHING")
         "Antwerp Design"]))

(deftest test-insert-expression-row
  (sql= (insert db :films [:code :title :did :date-prod :kind]
          (values [["T_601" "Yojimbo" 106 "1961-06-16" "Drama"]]))
        [(str "INSERT INTO \"films\" (\"code\", \"title\", \"did\", "
              "\"date-prod\", \"kind\") VALUES (?, ?, 106, ?, ?)")
         "T_601" "Yojimbo" "1961-06-16" "Drama"]))

(deftest test-insert-expression-rows
  (sql= (insert db :films []
          (values [["UA502" "Bananas" 105 :DEFAULT "Comedy" "82 minutes"]
                   ["T_601" "Yojimbo" 106 :DEFAULT "Drama" :DEFAULT]]))
        [(str "INSERT INTO \"films\" VALUES "
              "(?, ?, 105, DEFAULT, ?, ?), "
              "(?, ?, 106, DEFAULT, ?, DEFAULT)")
         "UA502" "Bananas" "Comedy" "82 minutes"
         "T_601" "Yojimbo" "Drama"]))
