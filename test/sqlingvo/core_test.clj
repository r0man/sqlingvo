(ns sqlingvo.core-test
  (:import java.util.Date)
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.java.io :refer [file]]
            [clojure.test :refer :all]
            [sqlingvo.core :refer :all]
            [sqlingvo.db :as db]
            [sqlingvo.expr :refer :all]
            [sqlingvo.util :refer :all]
            [clojure.string :as str]
            [sqlingvo.test :refer [db sql= with-stmt]]))

(deftest test-from
  (let [[from stmt] ((from :continents) {})]
    (is (= [{:op :table :children [:name] :name :continents}] from))
    (is (= {:from [{:op :table, :children [:name] :name :continents}]} stmt))))

;; COMPOSE

(deftest test-compose
  (with-stmt
    ["SELECT \"id\", \"name\" FROM \"continents\" WHERE (\"id\" = 1) ORDER BY \"name\""]
    (compose (select db [:id :name]
               (from :continents))
             (where '(= :id 1))
             (order-by :name))))

(deftest test-compose-where-clause-using-and
  (with-stmt
    ["SELECT \"color\", \"num-sides\" FROM \"shapes\" WHERE ((\"num-sides\" = 3) and (\"color\" = ?))" "green"]
    (let [triangles (compose (select db [:color :num-sides] (from :shapes))
                             (where '(= :num-sides 3)))]
      (compose triangles (where '(= :color "green") :and)))))

(deftest test-compose-selects
  (with-stmt
    ["SELECT 3, 2, 1"]
    (compose (select db [1 2 3])
             (select db [3 2 1]))))

;; AS

(deftest test-as
  (are [args expected]
      (is (= expected (apply as args)))
    [:id :other]
    (assoc (parse-expr :id) :as :other)
    [:continents [:id :name]]
    [(assoc (parse-expr :continents.id) :as :continents-id)
     (assoc (parse-expr :continents.name) :as :continents-name)]
    [:public.continents [:id :name]]
    [(assoc (parse-expr :public.continents.id) :as :public-continents-id)
     (assoc (parse-expr :public.continents.name) :as :public-continents-name)]
    ['(count *) :count]
    (assoc (parse-expr '(count *)) :as :count)))

;; CAST

(deftest test-cast-int-as-text
  (with-stmt
    ["SELECT CAST(1 AS text)"]
    (select db [`(cast 1 :text)])))

(deftest test-cast-text-as-int
  (with-stmt
    ["SELECT CAST(? AS int)" "1"]
    (select db [`(cast "1" :int)])))

(deftest test-cast-with-alias
  (with-stmt
    ["SELECT CAST(? AS int) AS \"numeric-id\"" "1"]
    (select db [(as `(cast "1" :int) :numeric-id)])))

;; COPY

(deftest test-copy-stdin
  (with-stmt
    ["COPY \"country\" FROM STDIN"]
    (copy db :country []
      (from :stdin))
    (is (= :copy (:op stmt)))
    (is (= [:stdin] (:from stmt)))))

(deftest test-copy-country
  (with-stmt
    ["COPY \"country\" FROM ?" "/usr1/proj/bray/sql/country_data"]
    (copy db :country []
      (from "/usr1/proj/bray/sql/country_data"))
    (is (= :copy (:op stmt)))
    (is (= ["/usr1/proj/bray/sql/country_data"] (:from stmt)))))

(deftest test-copy-country-with-encoding
  (with-stmt
    ["COPY \"country\" FROM ? ENCODING ?" "/usr1/proj/bray/sql/country_data" "UTF-8"]
    (copy db :country []
      (from "/usr1/proj/bray/sql/country_data")
      (encoding "UTF-8"))
    (is (= :copy (:op stmt)))
    (is (= ["/usr1/proj/bray/sql/country_data"] (:from stmt)))))

(deftest test-copy-country-with-delimiter
  (with-stmt
    ["COPY \"country\" FROM ? DELIMITER ?" "/usr1/proj/bray/sql/country_data" " "]
    (copy db :country []
      (from "/usr1/proj/bray/sql/country_data")
      (delimiter " "))))

(deftest test-copy-country-columns
  (with-stmt
    ["COPY \"country\" (\"id\", \"name\") FROM ?" "/usr1/proj/bray/sql/country_data"]
    (copy db :country [:id :name]
      (from "/usr1/proj/bray/sql/country_data"))
    (is (= :copy (:op stmt)))
    (is (= ["/usr1/proj/bray/sql/country_data"] (:from stmt)))
    (is (= (map parse-column [:id :name]) (:columns stmt)))))

(deftest test-copy-from-expands-to-absolute-path
  (is (= ["COPY \"country\" FROM ?" (.getAbsolutePath (file "country_data"))]
         (sql (copy db :country [] (from (file "country_data")))))))

(deftest test-select-from
  (with-stmt
    ["SELECT * FROM \"continents\""]
    (select db [:*]
      (from :continents))))

(deftest test-select-in-list
  (with-stmt
    ["SELECT * FROM \"continents\" WHERE 1 IN (1, 2, 3)"]
    (select db [:*]
      (from :continents)
      (where '(in 1 (1 2 3))))))

(deftest test-select-in-empty-list
  (with-stmt
    ["SELECT * FROM \"continents\" WHERE 1 IN (NULL)"]
    (select db [:*]
      (from :continents)
      (where '(in 1 ())))))

;; DELETE

(deftest test-delete-films
  (with-stmt
    ["DELETE FROM \"films\""]
    (delete db :films)
    (is (= :delete (:op stmt)))
    (is (= (parse-table :films) (:table stmt)))))

(deftest test-delete-all-films-but-musicals
  (with-stmt
    ["DELETE FROM \"films\" WHERE (\"kind\" <> ?)" "Musical"]
    (delete db :films
      (where '(<> :kind "Musical")))
    (is (= :delete (:op stmt)))
    (is (= (parse-table :films) (:table stmt)))
    (is (= (parse-condition '(<> :kind "Musical")) (:where stmt)))))

(deftest test-delete-completed-tasks-returning-all
  (with-stmt
    ["DELETE FROM \"tasks\" WHERE (\"status\" = ?) RETURNING *" "DONE"]
    (delete db :tasks
      (where '(= :status "DONE"))
      (returning *))
    (is (= :delete (:op stmt)))
    (is (= (parse-table :tasks) (:table stmt)))
    (is (= (parse-condition '(= :status "DONE")) (:where stmt)))
    (is (= [(parse-expr *)] (:returning stmt)))))

(deftest test-delete-films-by-producer-name
  (with-stmt
    ["DELETE FROM \"films\" WHERE \"producer-id\" IN (SELECT \"id\" FROM \"producers\" WHERE (\"name\" = ?))" "foo"]
    (delete db :films
      (where `(in :producer-id
                  ~(select db [:id]
                     (from :producers)
                     (where '(= :name "foo"))))))
    (is (= :delete (:op stmt)))
    (is (= (parse-table :films) (:table stmt)))
    (is (= (parse-condition `(in :producer-id
                                 ~(select db [:id]
                                    (from :producers)
                                    (where '(= :name "foo")))))
           (:where stmt)))))

(deftest test-delete-quotes
  (with-stmt
    [(str "DELETE FROM \"quotes\" WHERE ((\"company-id\" = 1) and (\"date\" > (SELECT \"min\"(\"date\") FROM \"import\")) and "
          "(\"date\" > (SELECT \"max\"(\"date\") FROM \"import\")))")]
    (delete db :quotes
      (where `(and (= :company-id 1)
                   (> :date ~(select db ['(min :date)] (from :import)))
                   (> :date ~(select db ['(max :date)] (from :import))))))
    (is (= :delete (:op stmt)))
    (is (= (parse-table :quotes) (:table stmt)))
    (is (= (parse-condition `(and (= :company-id 1)
                                  (> :date ~(select db ['(min :date)] (from :import)))
                                  (> :date ~(select db ['(max :date)] (from :import)))))
           (:where stmt)))))

;; DROP TABLE

(deftest test-drop-continents
  (with-stmt
    ["DROP TABLE \"continents\""]
    (drop-table db [:continents])
    (is (= :drop-table (:op stmt)))
    (is (= [(parse-table :continents)] (:tables stmt)))
    (is (= [:tables] (:children stmt)))))

(deftest test-drop-continents-and-countries
  (with-stmt
    ["DROP TABLE \"continents\", \"countries\""]
    (drop-table db [:continents :countries])
    (is (= :drop-table (:op stmt)))
    (is (= (map parse-table [:continents :countries]) (:tables stmt)))
    (is (= [:tables] (:children stmt)))))

(deftest test-drop-continents-countries-if-exists-restrict
  (with-stmt
    ["DROP TABLE IF EXISTS \"continents\", \"countries\" RESTRICT"]
    (drop-table db [:continents :countries]
      (if-exists true)
      (restrict true))
    (is (= :drop-table (:op stmt)))
    (is (= {:op :if-exists} (:if-exists stmt)))
    (is (= (map parse-table [:continents :countries]) (:tables stmt)))
    (is (= {:op :restrict} (:restrict stmt)))
    (is (= [:tables] (:children stmt)))))

(deftest test-drop-continents-if-exists
  (with-stmt
    ["DROP TABLE IF EXISTS \"continents\""]
    (drop-table db [:continents]
      (if-exists true))
    (is (= (map parse-table [:continents]) (:tables stmt)))
    (is (= [:tables] (:children stmt)))))

(deftest test-drop-continents-if-exists-false
  (with-stmt
    ["DROP TABLE \"continents\""]
    (drop-table db [:continents]
      (if-exists false))
    (is (= (map parse-table [:continents]) (:tables stmt)))
    (is (= [:tables] (:children stmt)))))

;; INSERT

(deftest test-insert-default-values
  (with-stmt
    ["INSERT INTO \"films\" DEFAULT VALUES"]
    (insert db :films []
      (values :default))
    (is (= :insert (:op stmt)))
    (is (= [] (:columns stmt)))
    (is (= true (:default-values stmt)))
    (is (= (parse-table :films) (:table stmt)))))

(deftest test-insert-single-row-as-map
  (with-stmt
    ["INSERT INTO \"films\" (\"code\", \"title\", \"did\", \"date-prod\", \"kind\") VALUES (?, ?, 106, ?, ?)"
     "T-601" "Yojimbo" "1961-06-16" "Drama"]
    (insert db :films []
      (values {:code "T-601" :title "Yojimbo" :did 106 :date-prod "1961-06-16" :kind "Drama"}))
    (is (= :insert (:op stmt)))
    (is (= [] (:columns stmt)))
    (is (= [(parse-map-expr {:code "T-601" :title "Yojimbo" :did 106 :date-prod "1961-06-16" :kind "Drama"})]
           (:values stmt)))
    (is (= (parse-table :films) (:table stmt)))))

(deftest test-insert-single-row-as-seq
  (with-stmt
    ["INSERT INTO \"films\" (\"code\", \"title\", \"did\", \"date-prod\", \"kind\") VALUES (?, ?, 106, ?, ?)"
     "T-601" "Yojimbo" "1961-06-16" "Drama"]
    (insert db :films []
      (values [{:code "T-601" :title "Yojimbo" :did 106 :date-prod "1961-06-16" :kind "Drama"}]))
    (is (= :insert (:op stmt)))
    (is (= [] (:columns stmt)))
    (is (= [(parse-map-expr {:code "T-601" :title "Yojimbo" :did 106 :date-prod "1961-06-16" :kind "Drama"})]
           (:values stmt)))
    (is (= (parse-table :films) (:table stmt)))))

(deftest test-insert-multi-row
  (with-stmt
    ["INSERT INTO \"films\" (\"code\", \"title\", \"did\", \"date-prod\", \"kind\") VALUES (?, ?, 110, ?, ?), (?, ?, 140, ?, ?)"
     "B6717" "Tampopo" "1985-02-10" "Comedy" "HG120" "The Dinner Game" "1985-02-10" "Comedy"]
    (insert db :films []
      (values [{:code "B6717" :title "Tampopo" :did 110 :date-prod "1985-02-10" :kind "Comedy"},
               {:code "HG120" :title "The Dinner Game" :did 140 :date-prod "1985-02-10":kind "Comedy"}]))
    (is (= :insert (:op stmt)))
    (is (= [] (:columns stmt)))
    (is (= (map parse-map-expr
                [{:code "B6717" :title "Tampopo" :did 110 :date-prod "1985-02-10" :kind "Comedy"},
                 {:code "HG120" :title "The Dinner Game" :did 140 :date-prod "1985-02-10":kind "Comedy"}])
           (:values stmt)))
    (is (= (parse-table :films) (:table stmt)))))

(deftest test-insert-returning
  (with-stmt
    ["INSERT INTO \"distributors\" (\"did\", \"dname\") VALUES (106, ?) RETURNING *" "XYZ Widgets"]
    (insert db :distributors []
      (values [{:did 106 :dname "XYZ Widgets"}])
      (returning *))
    (is (= :insert (:op stmt)))
    (is (= [] (:columns stmt)))
    (is (= (parse-table :distributors) (:table stmt)))
    (is (= [(parse-expr *)] (:returning stmt)))))

(deftest test-insert-subselect
  (with-stmt
    ["INSERT INTO \"films\" SELECT * FROM \"tmp-films\" WHERE (\"date-prod\" < ?)" "2004-05-07"]
    (insert db :films []
      (select db [*]
        (from :tmp-films)
        (where '(< :date-prod "2004-05-07"))))
    (is (= :insert (:op stmt)))
    (is (= [] (:columns stmt)))
    (is (= (parse-table :films) (:table stmt)))
    (is (= (ast (select db [*]
                  (from :tmp-films)
                  (where '(< :date-prod "2004-05-07"))))
           (:select stmt)))))

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
  (with-stmt
    ["INSERT INTO \"x\" (\"a\", \"b\") VALUES (1, 2)"]
    (insert db :x [:a :b] (values [{:a 1 :b 2 :c 3}]))))

(deftest test-insert-values-with-fn-call
  (with-stmt
    ["INSERT INTO \"x\" (\"a\", \"b\") VALUES (1, \"lower\"(?)), (2, ?)" "B" "b"]
    (insert db :x [:a :b]
      (values [{:a 1 :b '(lower "B")}
               {:a 2 :b "b"}]))))

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

;; SELECT

(deftest test-select-1
  (with-stmt
    ["SELECT 1"]
    (select db [1])
    (is (= :select (:op stmt)))
    (is (= [(parse-expr 1)] (:exprs stmt)))))

(deftest test-select-1-as
  (with-stmt
    ["SELECT 1 AS \"n\""]
    (select db [(as 1 :n)])
    (is (= :select (:op stmt)))
    (is (= [(parse-expr (as 1 :n))] (:exprs stmt)))))

(deftest test-select-x-as-x
  (with-stmt
    ["SELECT ? AS \"x\"" "x"]
    (select db [(as "x" :x)])
    (is (= :select (:op stmt)))
    (is (= [(parse-expr (as "x" :x))] (:exprs stmt)))))

(deftest test-select-1-2-3
  (with-stmt
    ["SELECT 1, 2, 3"]
    (select db [1 2 3])
    (is (= :select (:op stmt)))
    (is (= (map parse-expr [1 2 3]) (:exprs stmt)))))

(deftest test-select-1-2-3-as
  (with-stmt
    ["SELECT 1 AS \"a\", 2 AS \"b\", 3 AS \"c\""]
    (select db [(as 1 :a) (as 2 :b) (as 3 :c)])
    (is (= :select (:op stmt)))
    (is (= (map parse-expr [(as 1 :a) (as 2 :b) (as 3 :c)])
           (:exprs stmt)))))

(deftest test-select-count-as
  (with-stmt
    ["SELECT count(*) AS \"count\" FROM \"tweets\""]
    (select db [(as '(count :*) :count)]
      (from :tweets))))

(deftest test-select-count-distinct
  (with-stmt
    ["SELECT count(DISTINCT \"user-id\") FROM \"tweets\""]
    (select db ['(count distinct :user-id)]
      (from :tweets))))

(deftest test-select-select-1
  (with-stmt
    ["SELECT (SELECT 1)"]
    (select db [(select db [1])])
    (is (= :select (:op stmt)))
    (is (= [(ast (select db [1]))] (:exprs stmt)))))

(deftest test-select-1-in-1-2-3
  (with-stmt
    ["SELECT 1 WHERE 1 IN (1, 2, 3)"]
    (select db [1]
      (where '(in 1 (1 2 3))))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr 1)] (:exprs stmt)))
    (is (= (parse-condition '(in 1 (1 2 3))) (:where stmt)))))

(deftest test-select-1-in-1-2-3-backquote
  (with-stmt
    ["SELECT 1 WHERE 1 IN (1, 2, 3)"]
    (select db [1]
      (where `(in 1 (1 2 3))))))

(deftest test-select-select-1-select-x
  (with-stmt
    ["SELECT (SELECT 1), (SELECT ?)" "x"]
    (select db [(select db [1]) (select db ["x"])])
    (is (= :select (:op stmt)))
    (is (= [(ast (select db [1])) (ast (select db ["x"]))] (:exprs stmt)))))

(deftest test-select-string
  (with-stmt
    ["SELECT * FROM \"continents\" WHERE (\"name\" = ?)" "Europe"]
    (select db [*]
      (from :continents)
      (where '(= :name "Europe")))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr *)] (:exprs stmt)))
    (is (= (parse-condition '(= :name "Europe")) (:where stmt)))))

(deftest test-select-where-single-arg-and
  (with-stmt
    ["SELECT 1 WHERE (1 = 1)"]
    (select db [1]
      (where '(and (= 1 1))))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr 1)] (:exprs stmt)))
    (is (= (parse-condition '(and (= 1 1))) (:where stmt)))))

(deftest test-select-less-2-arity
  (with-stmt
    ["SELECT 1 WHERE (1 < 2)"]
    (select db [1]
      (where '(< 1 2)))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr 1)] (:exprs stmt)))
    (is (= (parse-condition '(< 1 2)) (:where stmt)))))

(deftest test-select-less-3-arity
  (with-stmt
    ["SELECT 1 WHERE (1 < 2) AND (2 < 3)"]
    (select db [1]
      (where '(< 1 2 3)))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr 1)] (:exprs stmt)))
    (is (= (parse-condition '(< 1 2 3)) (:where stmt)))))

(deftest test-select-like
  (with-stmt
    ["SELECT * FROM \"films\" WHERE (\"title\" like ?)" "%Zombie%"]
    (select db [*]
      (from :films)
      (where '(like :title "%Zombie%")))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr *)] (:exprs stmt)))
    (is (= [(parse-from :films)] (:from stmt)))
    (is (= (parse-condition '(like :title "%Zombie%")) (:where stmt)))))

(deftest test-select-not-like
  (with-stmt
    ["SELECT * FROM \"films\" WHERE (\"title\" NOT LIKE ?)" "%Zombie%"]
    (select db [*]
      (from :films)
      (where '(not-like :title "%Zombie%")))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr *)] (:exprs stmt)))
    (is (= [(parse-from :films)] (:from stmt)))
    (is (= (parse-condition '(not-like :title "%Zombie%")) (:where stmt)))))

(deftest test-select-continents
  (with-stmt
    ["SELECT * FROM \"continents\""]
    (select db [*]
      (from :continents))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr *)] (:exprs stmt)))
    (is (= [(parse-from :continents)] (:from stmt)))))

(deftest test-select-continents-qualified
  (with-stmt
    ["SELECT \"continents\".* FROM \"continents\""]
    (select db [:continents.*]
      (from :continents))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr :continents.*)] (:exprs stmt)))
    (is (= [(parse-from :continents)] (:from stmt)))))

(deftest test-select-films
  (with-stmt
    ["SELECT * FROM \"films\""]
    (select db [*] (from :films))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr *)] (:exprs stmt)))
    (is (= [(parse-from :films)] (:from stmt)))))

(deftest test-select-comedy-films
  (with-stmt
    ["SELECT * FROM \"films\" WHERE (\"kind\" = ?)" "Comedy"]
    (select db [*]
      (from :films)
      (where '(= :kind "Comedy")))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr *)] (:exprs stmt)))
    (is (= (parse-condition '(= :kind "Comedy")) (:where stmt)))
    (is (= [(parse-from :films)] (:from stmt)))))

(deftest test-select-is-null
  (with-stmt
    ["SELECT 1 WHERE (NULL IS NULL)"]
    (select db [1]
      (where '(is-null nil)))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr 1)] (:exprs stmt)))
    (is (= (parse-condition '(is-null nil)) (:where stmt)))))

(deftest test-select-is-not-null
  (with-stmt
    ["SELECT 1 WHERE (NULL IS NOT NULL)"]
    (select db [1]
      (where '(is-not-null nil)))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr 1)] (:exprs stmt)))
    (is (= (parse-condition '(is-not-null nil)) (:where stmt)))))

(deftest test-select-backquote-date
  (with-stmt
    ["SELECT * FROM \"countries\" WHERE (\"created-at\" > ?)" (Date. 0)]
    (select db [*]
      (from :countries)
      (where `(> :created-at ~(Date. 0))))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr *)] (:exprs stmt)))
    (is (= (parse-condition `(> :created-at ~(Date. 0))) (:where stmt)))))

(deftest test-select-star-number-string
  (with-stmt
    ["SELECT *, 1, ?" "x"]
    (select db [* 1 "x"])
    (is (= :select (:op stmt)))
    (is (= (map parse-expr [* 1 "x"]) (:exprs stmt)))))

(deftest test-select-column
  (with-stmt
    ["SELECT \"created-at\" FROM \"continents\""]
    (select db [:created-at]
      (from :continents))
    (is (= :select (:op stmt)))
    (is (= [(parse-table :continents)] (:from stmt)))
    (is (= [(parse-expr :created-at)] (:exprs stmt)))))

(deftest test-select-columns
  (with-stmt
    ["SELECT \"name\", \"created-at\" FROM \"continents\""]
    (select db [:name :created-at]
      (from :continents))
    (is (= :select (:op stmt)))
    (is (= [(parse-table :continents)] (:from stmt)))
    (is (= (map parse-expr [:name :created-at]) (:exprs stmt)))))

(deftest test-select-column-alias
  (with-stmt
    ["SELECT \"created-at\" AS \"c\" FROM \"continents\""]
    (select db [(as :created-at :c)]
      (from :continents))
    (is (= :select (:op stmt)))
    (is (= [(parse-table :continents)] (:from stmt)))
    (is (= [(parse-expr (as :created-at :c))] (:exprs stmt)))))

(deftest test-select-multiple-fns
  (with-stmt
    ["SELECT \"greatest\"(1, 2), \"lower\"(?)" "X"]
    (select db ['(greatest 1 2) '(lower "X")])
    (is (= :select (:op stmt)))
    (is (= (map parse-expr ['(greatest 1 2) '(lower "X")]) (:exprs stmt)))))

(deftest test-select-nested-fns
  (with-stmt
    ["SELECT (1 + \"greatest\"(2, 3))"]
    (select db ['(+ 1 (greatest 2 3))])
    (is (= :select (:op stmt)))
    (is (= [(parse-expr '(+ 1 (greatest 2 3)))] (:exprs stmt)))))

(deftest test-select-fn-alias
  (with-stmt
    ["SELECT \"max\"(\"created-at\") AS \"m\" FROM \"continents\""]
    (select db [(as '(max :created-at) :m)]
      (from :continents))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr (as '(max :created-at) :m))] (:exprs stmt)))
    (is (= [(parse-table :continents)] (:from stmt)))))

(deftest test-select-limit
  (with-stmt
    ["SELECT * FROM \"continents\" LIMIT 10"]
    (select db [*]
      (from :continents)
      (limit 10))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr *)] (:exprs stmt)))
    (is (= [(parse-table :continents)] (:from stmt)))
    (is (= {:op :limit :count 10} (:limit stmt)))))

(deftest test-select-limit-nil
  (with-stmt
    ["SELECT * FROM \"continents\""]
    (select db [*]
      (from :continents)
      (limit nil))))

(deftest test-select-offset
  (with-stmt
    ["SELECT * FROM \"continents\" OFFSET 15"]
    (select db [*]
      (from :continents)
      (offset 15))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr *)] (:exprs stmt)))
    (is (= [(parse-table :continents)] (:from stmt)))
    (is (= {:op :offset :start 15} (:offset stmt)))))

(deftest test-select-limit-offset
  (with-stmt
    ["SELECT * FROM \"continents\" LIMIT 10 OFFSET 20"]
    (select db [*]
      (from :continents)
      (limit 10)
      (offset 20))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr *)] (:exprs stmt)))
    (is (= [(parse-table :continents)] (:from stmt)))
    (is (= {:op :limit :count 10} (:limit stmt)))
    (is (= {:op :offset :start 20} (:offset stmt)))))

(deftest test-select-column-max
  (with-stmt
    ["SELECT \"max\"(\"created-at\") FROM \"continents\""]
    (select db ['(max :created-at)]
      (from :continents))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr '(max :created-at))] (:exprs stmt)))
    (is (= [(parse-table :continents)] (:from stmt)))))

(deftest test-select-distinct-subquery-alias
  (with-stmt
    ["SELECT DISTINCT \"x\".\"a\", \"x\".\"b\" FROM (SELECT 1 AS \"a\", 2 AS \"b\") AS \"x\""]
    (select db (distinct [:x.a :x.b])
      (from (as (select db [(as 1 :a) (as 2 :b)]) :x)))
    (is (= :select (:op stmt)))
    (let [distinct (:distinct stmt)]
      (is (= :distinct (:op distinct)))
      (is (= (map parse-expr [:x.a :x.b]) (:exprs distinct)))
      (is (= [] (:on distinct))))
    (let [from (first (:from stmt))]
      (is (= :select (:op from)))
      (is (= :x (:as from)))
      (is (= (map parse-expr [(as 1 :a) (as 2 :b)]) (:exprs from))))))

(deftest test-select-distinct-on-subquery-alias
  (with-stmt
    ["SELECT DISTINCT ON (\"x\".\"a\", \"x\".\"b\") \"x\".\"a\", \"x\".\"b\" FROM (SELECT 1 AS \"a\", 2 AS \"b\") AS \"x\""]
    (select db (distinct [:x.a :x.b] :on [:x.a :x.b])
      (from (as (select db [(as 1 :a) (as 2 :b)]) :x)))
    (is (= :select (:op stmt)))
    (let [distinct (:distinct stmt)]
      (is (= :distinct (:op distinct)))
      (is (= (map parse-expr [:x.a :x.b]) (:exprs distinct)))
      (is (= (map parse-expr [:x.a :x.b]) (:on distinct))))
    (let [from (first (:from stmt))]
      (is (= :select (:op from)))
      (is (= :x (:as from)))
      (is (= (map parse-expr [(as 1 :a) (as 2 :b)]) (:exprs from))))))

(deftest test-select-most-recent-weather-report
  (with-stmt
    ["SELECT DISTINCT ON (\"location\") \"location\", \"time\", \"report\" FROM \"weather-reports\" ORDER BY \"location\", \"time\" DESC"]
    (select db (distinct [:location :time :report] :on [:location])
      (from :weather-reports)
      (order-by :location (desc :time)))
    (is (= :select (:op stmt)))
    (let [distinct (:distinct stmt)]
      (is (= :distinct (:op distinct)))
      (is (= (map parse-expr [:location :time :report]) (:exprs distinct)))
      (is (= [(parse-expr :location)] (:on distinct))))
    (is (= [(parse-from :weather-reports)] (:from stmt)))
    (is (= [(parse-expr :location) (desc :time)] (:order-by stmt)))))

(deftest test-select-order-by-asc
  (with-stmt
    ["SELECT * FROM \"continents\" ORDER BY \"created-at\" ASC"]
    (select db [*]
      (from :continents)
      (order-by (asc :created-at)))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr *)] (:exprs stmt)))
    (is (= [(parse-from :continents)] (:from stmt)))
    (is (= [(parse-expr (asc :created-at))] (:order-by stmt)))))

(deftest test-select-order-by-asc-expr
  (with-stmt
    ["SELECT * FROM \"weather\".\"datasets\" ORDER BY \"abs\"((\"st_scalex\"(\"rast\") * \"st_scaley\"(\"rast\"))) DESC"]
    (select db [*]
      (from :weather.datasets)
      (order-by (desc '(abs (* (st_scalex :rast) (st_scaley :rast))))))))

(deftest test-select-order-by-desc
  (with-stmt
    ["SELECT * FROM \"continents\" ORDER BY \"created-at\" DESC"]
    (select db [*]
      (from :continents)
      (order-by (desc :created-at)))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr *)] (:exprs stmt)))
    (is (= [(parse-from :continents)] (:from stmt)))
    (is (= [(parse-expr (desc :created-at))] (:order-by stmt)))))

(deftest test-select-order-by-nulls-first
  (with-stmt
    ["SELECT * FROM \"continents\" ORDER BY \"created-at\" NULLS FIRST"]
    (select db [*]
      (from :continents)
      (order-by (nulls :created-at :first)))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr *)] (:exprs stmt)))
    (is (= [(parse-from :continents)] (:from stmt)))
    (is (= [(parse-expr (nulls :created-at :first))] (:order-by stmt)))))

(deftest test-select-order-by-nulls-last
  (with-stmt
    ["SELECT * FROM \"continents\" ORDER BY \"created-at\" NULLS LAST"]
    (select db [*]
      (from :continents)
      (order-by (nulls :created-at :last)))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr *)] (:exprs stmt)))
    (is (= [(parse-from :continents)] (:from stmt)))
    (is (= [(parse-expr (nulls :created-at :last))] (:order-by stmt)))))

(deftest test-select-order-by-if-true
  (with-stmt
    ["SELECT * FROM \"continents\" ORDER BY \"name\""]
    (let [opts {:order-by :name}]
      (select db [*]
        (from :continents)
        (if (:order-by opts)
          (order-by (:order-by opts)))))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr *)] (:exprs stmt)))
    (is (= [(parse-from :continents)] (:from stmt)))
    (is (= [(parse-expr :name)] (:order-by stmt)))))

(deftest test-select-order-by-if-false
  (with-stmt
    ["SELECT * FROM \"continents\""]
    (let [opts {}]
      (select db [*]
        (from :continents)
        (if (:order-by opts)
          (order-by (:order-by opts)))))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr *)] (:exprs stmt)))
    (is (= [(parse-from :continents)] (:from stmt)))
    (is (nil? (:order-by stmt)))))

(deftest test-select-order-by-nil
  (with-stmt
    ["SELECT * FROM \"continents\""]
    (select db [*]
      (from :continents)
      (order-by nil))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr *)] (:exprs stmt)))
    (is (= [(parse-from :continents)] (:from stmt)))
    (is (nil? (:order-by stmt)))))

(deftest test-select-1-where-1-is-1
  (with-stmt
    ["SELECT 1 WHERE (1 = 1)"]
    (select db [1]
      (where '(= 1 1)))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr 1)] (:exprs stmt)))
    (is (= (parse-condition '(= 1 1)) (:where stmt)))))

(deftest test-select-1-where-1-is-2-is-3
  (with-stmt
    ["SELECT 1 WHERE (1 = 2) AND (2 = 3)"]
    (select db [1]
      (where '(= 1 2 3)))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr 1)] (:exprs stmt)))
    (is (= (parse-condition '(= 1 2 3)) (:where stmt)))))

(deftest test-select-subquery-alias
  (with-stmt
    ["SELECT * FROM (SELECT 1, 2, 3) AS \"x\""]
    (select db [*]
      (from (as (select db [1 2 3]) :x)))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr *)] (:exprs stmt)))
    (let [from (first (:from stmt))]
      (is (= :select (:op from)))
      (is (= :x (:as from)))
      (is (= (map parse-expr [1 2 3]) (:exprs from))))))

(deftest test-select-subqueries-alias
  (with-stmt
    ["SELECT * FROM (SELECT 1) AS \"x\", (SELECT 2) AS \"y\""]
    (select db [*]
      (from (as (select db [1]) :x)
            (as (select db [2]) :y)))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr *)] (:exprs stmt)))
    (let [from (first (:from stmt))]
      (is (= :select (:op from)))
      (is (= :x (:as from)))
      (is (= [(parse-expr 1)] (:exprs from))))
    (let [from (second (:from stmt))]
      (is (= :select (:op from)))
      (is (= :y (:as from)))
      (is (= [(parse-expr 2)] (:exprs from))))))

(deftest test-select-parition-by
  (with-stmt
    ["SELECT \"id\", \"lag\"(\"close\") over (partition by \"company-id\" order by \"date\" desc) FROM \"quotes\""]
    (select db [:id '((lag :close) over (partition by :company-id order by :date desc))]
      (from :quotes))
    (is (= :select (:op stmt)))
    (is (= (map parse-expr [:id '((lag :close) over (partition by :company-id order by :date desc))])
           (:exprs stmt)))
    (is (= [(parse-from :quotes)] (:from stmt)))))

(deftest test-select-total-return
  (with-stmt
    ["SELECT \"id\", (\"close\" / (\"lag\"(\"close\") over (partition by \"company-id\" order by \"date\" desc) - 1)) FROM \"quotes\""]
    (select db [:id '(/ :close (- ((lag :close) over (partition by :company-id order by :date desc)) 1))]
      (from :quotes))
    (is (= :select (:op stmt)))
    (is (= (map parse-expr [:id '(/ :close (- ((lag :close) over (partition by :company-id order by :date desc)) 1))])
           (:exprs stmt)))
    (is (= [(parse-from :quotes)] (:from stmt)))))

(deftest test-select-total-return-alias
  (with-stmt
    ["SELECT \"id\", (\"close\" / (\"lag\"(\"close\") over (partition by \"company-id\" order by \"date\" desc) - 1)) AS \"daily-return\" FROM \"quotes\""]
    (select db [:id (as '(/ :close (- ((lag :close) over (partition by :company-id order by :date desc)) 1)) :daily-return)]
      (from :quotes))
    (is (= :select (:op stmt)))
    (is (= (map parse-expr [:id (as '(/ :close (- ((lag :close) over (partition by :company-id order by :date desc)) 1)) :daily-return)])
           (:exprs stmt)))
    (is (= [(parse-from :quotes)] (:from stmt)))))

(deftest test-select-group-by-a-order-by-1
  (with-stmt
    ["SELECT \"a\", \"max\"(\"b\") FROM \"table-1\" GROUP BY \"a\" ORDER BY 1"]
    (select db [:a '(max :b)]
      (from :table-1)
      (group-by :a)
      (order-by 1))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr :a) (parse-expr '(max :b))] (:exprs stmt)))
    (is (= [(parse-from :table-1)] (:from stmt)))
    (is (= [(parse-expr 1)] (:order-by stmt)))))

(deftest test-select-order-by-query-select
  (with-stmt
    ["SELECT \"a\", \"b\" FROM \"table-1\" ORDER BY (\"a\" + \"b\"), \"c\""]
    (select db [:a :b]
      (from :table-1)
      (order-by '(+ :a :b) :c))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr :a) (parse-expr :b)] (:exprs stmt)))
    (is (= [(parse-from :table-1)] (:from stmt)))
    (is (= [(parse-expr '(+ :a :b)) (parse-expr :c)] (:order-by stmt)))))

(deftest test-select-order-by-sum
  (with-stmt
    ["SELECT (\"a\" + \"b\") AS \"sum\", \"c\" FROM \"table-1\" ORDER BY \"sum\""]
    (select db [(as '(+ :a :b) :sum) :c]
      (from :table-1)
      (order-by :sum))
    (is (= :select (:op stmt)))
    (is (= [(parse-expr (as '(+ :a :b) :sum)) (parse-expr :c)] (:exprs stmt)))
    (is (= [(parse-from :table-1)] (:from stmt)))
    (is (= [(parse-expr :sum)] (:order-by stmt)))))

(deftest test-select-setval
  (with-stmt
    ["SELECT \"setval\"(\"continent-id-seq\", (SELECT \"max\"(\"id\") FROM \"continents\"))"]
    (select db [`(setval :continent-id-seq ~(select db [`(max :id)] (from :continents)))])
    (is (= :select (:op stmt)))
    (is (= (map parse-expr [`(setval :continent-id-seq ~(select db [`(max :id)] (from :continents)))])
           (:exprs stmt)))))

(deftest test-select-regex-match
  (with-stmt
    ["SELECT \"id\", \"symbol\", \"quote\" FROM \"quotes\" WHERE (? ~ \"concat\"(?, \"symbol\", ?))" "$AAPL" "(^|\\s)\\$" "($|\\s)"]
    (select db [:id :symbol :quote]
      (from :quotes)
      (where `(~(symbol "~") "$AAPL" (concat "(^|\\s)\\$" :symbol "($|\\s)"))))
    (is (= :select (:op stmt)))
    (is (= [(parse-from :quotes)] (:from stmt)))
    (is (= (map parse-expr [:id :symbol :quote]) (:exprs stmt)))
    (is (= (parse-condition `(~(symbol "~") "$AAPL" (concat "(^|\\s)\\$" :symbol "($|\\s)"))) (:where stmt)))  ))

(deftest test-select-join-on-columns
  (with-stmt
    ["SELECT * FROM \"countries\" JOIN \"continents\" ON (\"continents\".\"id\" = \"countries\".\"continent-id\")"]
    (select db [*]
      (from :countries)
      (join :continents '(on (= :continents.id :countries.continent-id))))
    (is (= :select (:op stmt)))
    (is (= [(parse-from :countries)] (:from stmt)))
    (is (= [(parse-expr *)] (:exprs stmt)))
    (let [join (first (:joins stmt))]
      (is (= :join (:op join)))
      (is (= (parse-from :continents) (:from join)))
      (is (= (parse-expr '(= :continents.id :countries.continent-id)) (:on join))))))

(deftest test-select-join-with-keywords
  (with-stmt
    ["SELECT * FROM \"continents\" JOIN \"countries\" ON (\"countries\".\"continent-id\" = \"continents\".\"id\")"]
    (select db [*]
      (from :continents)
      (join :countries.continent-id :continents.id))
    (is (= :select (:op stmt)))
    (is (= [(parse-from :continents)] (:from stmt)))
    (is (= [(parse-expr *)] (:exprs stmt)))
    (let [join (first (:joins stmt))]
      (is (= :join (:op join)))
      (is (= (parse-from :countries) (:from join)))
      (is (= (parse-expr '(= :countries.continent-id :continents.id)) (:on join))))))

(deftest test-select-join-on-columns-alias
  (with-stmt
    ["SELECT * FROM \"countries\" \"c\" JOIN \"continents\" ON (\"continents\".\"id\" = \"c\".\"continent-id\")"]
    (select db [*]
      (from (as :countries :c))
      (join :continents '(on (= :continents.id :c.continent-id))))
    (is (= :select (:op stmt)))
    (is (= [(parse-from (as :countries :c))] (:from stmt)))
    (is (= [(parse-expr *)] (:exprs stmt)))
    (let [join (first (:joins stmt))]
      (is (= :join (:op join)))
      (is (= (parse-from :continents) (:from join)))
      (is (= (parse-expr '(= :continents.id :c.continent-id)) (:on join))))))

(deftest test-select-join-using-column
  (with-stmt
    ["SELECT * FROM \"countries\" JOIN \"continents\" USING (\"id\")"]
    (select db [*]
      (from :countries)
      (join :continents '(using :id)))
    (is (= :select (:op stmt)))
    (is (= [(parse-from :countries)] (:from stmt)))
    (is (= [(parse-expr *)] (:exprs stmt)))
    (let [join (first (:joins stmt))]
      (is (= :join (:op join)))
      (is (= (parse-from :continents) (:from join)))
      (is (= (map parse-expr [:id]) (:using join))))))

(deftest test-select-join-using-columns
  (with-stmt
    ["SELECT * FROM \"countries\" JOIN \"continents\" USING (\"id\", \"created-at\")"]
    (select db [*]
      (from :countries)
      (join :continents '(using :id :created-at)))
    (is (= :select (:op stmt)))
    (is (= [(parse-from :countries)] (:from stmt)))
    (is (= [(parse-expr *)] (:exprs stmt)))
    (let [join (first (:joins stmt))]
      (is (= :join (:op join)))
      (is (= (parse-from :continents) (:from join)))
      (is (= (map parse-expr [:id :created-at]) (:using join))))))

(deftest test-select-join-alias
  (with-stmt
    ["SELECT * FROM \"countries\" \"c\" JOIN \"continents\" ON (\"continents\".\"id\" = \"c\".\"continent-id\")"]
    (select db [*]
      (from (as :countries :c))
      (join :continents '(on (= :continents.id :c.continent-id))))))

(deftest test-select-join-syntax-quote
  (with-stmt
    ["SELECT * FROM \"countries\" \"c\" JOIN \"continents\" ON (\"continents\".\"id\" = \"c\".\"continent-id\")"]
    (select db [*]
      (from (as :countries :c))
      (join :continents `(on (= :continents.id :c.continent-id))))))

(deftest test-select-join-subselect-alias
  (with-stmt
    [(str "SELECT \"quotes\".*, \"start-date\" FROM \"quotes\" JOIN (SELECT \"company-id\", \"min\"(\"date\") AS \"start-date\" "
          "FROM \"quotes\" GROUP BY \"company-id\") AS \"start-dates\" ON ((\"quotes\".\"company-id\" = \"start-dates\".\"company-id\") and (\"quotes\".\"date\" = \"start-dates\".\"start-date\"))")]
    (select db [:quotes.* :start-date]
      (from :quotes)
      (join (as (select db [:company-id (as '(min :date) :start-date)]
                  (from :quotes)
                  (group-by :company-id))
                :start-dates)
            '(on (and (= :quotes.company-id :start-dates.company-id)
                      (= :quotes.date :start-dates.start-date)))))))

(deftest test-select-except
  (with-stmt
    ["SELECT 1 EXCEPT SELECT 2"]
    (except
     (select db [1])
     (select db [2]))))

(deftest test-select-except-all
  (with-stmt
    ["SELECT 1 EXCEPT ALL SELECT 2"]
    (except
     {:all true}
     (select db [1])
     (select db [2]))))

(deftest test-select-intersect
  (with-stmt
    ["SELECT 1 INTERSECT SELECT 2"]
    (intersect
     (select db [1])
     (select db [2]))))

(deftest test-select-intersect-all
  (with-stmt
    ["SELECT 1 INTERSECT ALL SELECT 2"]
    (intersect
     {:all true}
     (select db [1])
     (select db [2]))))

(deftest test-select-union
  (with-stmt
    ["SELECT 1 UNION SELECT 2"]
    (union
     (select db [1])
     (select db [2]))))

(deftest test-select-union-all
  (with-stmt
    ["SELECT 1 UNION ALL SELECT 2"]
    (union
     {:all true}
     (select db [1])
     (select db [2]))))

(deftest test-select-where-combine-and-1
  (with-stmt
    ["SELECT 1 WHERE (1 = 1)"]
    (select db [1]
      (where '(= 1 1) :and))))

(deftest test-select-where-combine-and-2
  (with-stmt
    ["SELECT 1 WHERE ((1 = 1) and (2 = 2))"]
    (select db [1]
      (where '(= 1 1))
      (where '(= 2 2) :and))))

(deftest test-select-where-combine-and-3
  (with-stmt
    ["SELECT 1 WHERE (((1 = 1) and (2 = 2)) and (3 = 3))"]
    (select db [1]
      (where '(= 1 1))
      (where '(= 2 2) :and)
      (where '(= 3 3) :and))))

(deftest test-select-where-combine-or-1
  (with-stmt
    ["SELECT 1 WHERE (1 = 1)"]
    (select db [1]
      (where '(= 1 1) :or))))

(deftest test-select-where-combine-or-2
  (with-stmt
    ["SELECT 1 WHERE ((1 = 1) or (2 = 2))"]
    (select db [1]
      (where '(= 1 1))
      (where '(= 2 2) :or))))

(deftest test-select-where-combine-or-3
  (with-stmt
    ["SELECT 1 WHERE (((1 = 1) or (2 = 2)) or (3 = 3))"]
    (select db [1]
      (where '(= 1 1))
      (where '(= 2 2) :or)
      (where '(= 3 3) :or))))

(deftest test-select-where-combine-mixed
  (with-stmt
    ["SELECT 1 WHERE (((1 = 1) and (2 = 2)) or (3 = 3))"]
    (select db [1]
      (where '(= 1 1))
      (where '(= 2 2) :and)
      (where '(= 3 3) :or))))

(deftest test-substring-from-to
  (with-stmt
    ["SELECT substring(? from 2 for 3)" "Thomas"]
    (select db ['(substring "Thomas" from 2 for 3)])))

(deftest test-substring-from-to-lower
  (with-stmt
    ["SELECT \"lower\"(substring(? from 2 for 3))" "Thomas"]
    (select db ['(lower (substring "Thomas" from 2 for 3))])))

(deftest test-substring-from-pattern
  (with-stmt
    ["SELECT substring(? from ?)" "Thomas" "...$"]
    (select db ['(substring "Thomas" from "...$")])))

(deftest test-substring-from-pattern-for-escape
  (with-stmt
    ["SELECT substring(? from ? for ?)" "Thomas" "%##\"o_a#\"_" "#"]
    (select db ['(substring "Thomas" from "%##\"o_a#\"_" for "#")])))

(deftest test-trim
  (with-stmt
    ["SELECT trim(both ? from ?)" "x" "xTomxx"]
    (select db ['(trim both "x" from "xTomxx")])))

(deftest test-select-from-fn
  (with-stmt
    ["SELECT * FROM \"generate_series\"(0, 10)"]
    (select db [*] (from '(generate_series 0 10)))))

(deftest test-select-from-fn-alias
  (with-stmt
    ["SELECT \"n\" FROM \"generate_series\"(0, 200) AS \"n\""]
    (select db [:n] (from (as '(generate_series 0 200) :n)))))

(deftest test-select-qualified-column
  (with-stmt
    ["SELECT \"continents\".\"id\" FROM \"continents\""]
    (select db [{:op :column :table :continents :name :id}]
      (from :continents))))

(deftest test-select-qualified-keyword-column
  (with-stmt
    ["SELECT \"continents\".\"id\" FROM \"continents\""]
    (select db [:continents.id] (from :continents))))

;; QUOTING

(deftest test-db-specifiy-quoting
  (are [db expected]
      (= expected (sql (select db [:continents.id] (from :continents))))
    (db/mysql)
    ["SELECT `continents`.`id` FROM `continents`"]
    (db/postgresql)
    ["SELECT \"continents\".\"id\" FROM \"continents\""]
    (db/oracle)
    ["SELECT \"continents\".\"id\" FROM \"continents\""]
    (db/vertica)
    ["SELECT \"continents\".\"id\" FROM \"continents\""]))

;; POSTGRESQL ARRAYS

(deftest test-array
  (with-stmt
    ["SELECT ARRAY[1, 2]"]
    (select db [[1 2]])))

(deftest test-array-concat
  (with-stmt
    ["SELECT (ARRAY[1, 2] || ARRAY[3, 4] || ARRAY[5, 6])"]
    (select db ['(|| [1 2] [3 4] [5 6])])))

(deftest test-select-array-contains
  (with-stmt
    ["SELECT (ARRAY[1, 2] @> ARRAY[3, 4])"]
    (select db [`(~(keyword "@>") [1 2] [3 4])])))

(deftest test-select-array-contained
  (with-stmt
    ["SELECT (ARRAY[1, 2] <@ ARRAY[3, 4])"]
    (select db [`(~(keyword "<@") [1 2] [3 4])])))

(deftest test-insert-array
  (sql= (insert db :test [:x] (values [{:x ["1" 2]}]))
        ["INSERT INTO \"test\" (\"x\") VALUES (ARRAY[?, 2])" "1"]))

;; POSTGRESQL FULLTEXT

(deftest test-cast-as-document-1
  (with-stmt
    ["SELECT CAST((\"title\" || ? || \"author\" || ? || \"abstract\" || ? || \"body\") AS document) FROM \"messages\" WHERE (\"mid\" = 12)" " " " " " "]
    (select db ['(cast (:|| :title " " :author " " :abstract " " :body) :document)]
      (from :messages)
      (where '(= :mid 12)))))

(deftest test-cast-as-document-2
  (with-stmt
    ["SELECT CAST((\"m\".\"title\" || ? || \"m\".\"author\" || ? || \"m\".\"abstract\" || ? || \"d\".\"body\") AS document) FROM \"messages\" \"m\", \"docs\" \"d\" WHERE ((\"mid\" = \"did\") and (\"mid\" = 12))" " " " " " "]
    (select db ['(cast (:|| :m.title " " :m.author " " :m.abstract " " :d.body) :document)]
      (from (as :messages :m) (as :docs :d))
      (where '(and (= :mid :did)
                   (= :mid 12))))))

(deftest test-basic-text-matching-1
  (with-stmt
    ["SELECT (CAST(? AS tsvector) @@ CAST(? AS tsquery))" "a fat cat sat on a mat and ate a fat rat" "rat & cat"]
    (select db [`(~(keyword "@@")
                  (cast "a fat cat sat on a mat and ate a fat rat" :tsvector)
                  (cast "rat & cat" :tsquery))])))

(deftest test-basic-text-matching-2
  (with-stmt
    ["SELECT (CAST(? AS tsquery) @@ CAST(? AS tsvector))" "fat & cow" "a fat cat sat on a mat and ate a fat rat"]
    (select db [`(~(keyword "@@")
                  (cast "fat & cow" :tsquery)
                  (cast "a fat cat sat on a mat and ate a fat rat" :tsvector))])))

(deftest test-basic-text-matching-3
  (with-stmt
    ["SELECT (\"to_tsvector\"(?) @@ \"to_tsquery\"(?))" "fat cats ate fat rats" "fat & rat"]
    (select db [`(~(keyword "@@")
                  (to_tsvector "fat cats ate fat rats")
                  (to_tsquery "fat & rat"))])))

(deftest test-basic-text-matching-4
  (with-stmt
    ["SELECT (CAST(? AS tsvector) @@ \"to_tsquery\"(?))" "fat cats ate fat rats" "fat & rat"]
    (select db [`(~(keyword "@@")
                  (cast "fat cats ate fat rats" :tsvector)
                  (to_tsquery "fat & rat"))])))

(deftest test-searching-a-table-1
  (with-stmt
    ["SELECT \"title\" FROM \"pgweb\" WHERE (\"to_tsvector\"(?, \"body\") @@ \"to_tsquery\"(?, ?))" "english" "english" "friend"]
    (select db [:title]
      (from :pgweb)
      (where `(~(keyword "@@")
               (to_tsvector "english" :body)
               (to_tsquery "english" "friend"))))))

(deftest test-searching-a-table-2
  (with-stmt
    ["SELECT \"title\" FROM \"pgweb\" WHERE (\"to_tsvector\"(\"body\") @@ \"to_tsquery\"(?))" "friend"]
    (select db [:title]
      (from :pgweb)
      (where `(~(keyword "@@")
               (to_tsvector :body)
               (to_tsquery "friend"))))))

(deftest test-searching-a-table-3
  (with-stmt
    ["SELECT \"title\" FROM \"pgweb\" WHERE (\"to_tsvector\"((\"title\" || ? || \"body\")) @@ \"to_tsquery\"(?)) ORDER BY \"last-mod-date\" DESC LIMIT 10" " " "create & table"]
    (select db [:title]
      (from :pgweb)
      (where `(~(keyword "@@")
               (to_tsvector (:|| :title " " :body))
               (to_tsquery "create & table")))
      (order-by (desc :last-mod-date))
      (limit 10))))

;; WITH QUERIES (COMMON TABLE EXPRESSIONS)

(deftest test-with-query
  (with-stmt
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
          "GROUP BY \"region\", \"product\"")]
    (with db [:regional-sales
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
        (group-by :region :product)))))

(deftest test-with-modify-data
  (with-stmt
    [(str "WITH \"moved-rows\" AS ("
          "DELETE FROM \"products\" "
          "WHERE ((\"date\" >= ?) and (\"date\" < ?)) "
          "RETURNING *) "
          "INSERT INTO \"product-logs\" SELECT * FROM \"moved-rows\"")
     "2010-10-01" "2010-11-01"]
    (with db [:moved-rows
              (delete db :products
                (where '(and (>= :date "2010-10-01")
                             (< :date "2010-11-01")))
                (returning :*))]
      (insert db :product-logs []
        (select db [:*] (from :moved-rows))))))

(deftest test-with-counter-update
  (with-stmt
    [(str "WITH \"upsert\" AS ("
          "UPDATE \"counter-table\" SET counter = counter+1 "
          "WHERE (\"id\" = ?) RETURNING *) "
          "INSERT INTO \"counter-table\" (\"id\", \"counter\") "
          "SELECT ?, 1 "
          "WHERE (NOT EXISTS (SELECT * FROM \"upsert\"))")
     "counter-name" "counter-name"]
    (with db [:upsert (update db :counter-table
                        '((= counter counter+1))
                        (where '(= :id "counter-name"))
                        (returning *))]
      (insert db :counter-table [:id :counter]
        (select db ["counter-name" 1])
        (where `(not-exists ~(select db [*] (from :upsert))))))))

(deftest test-with-delete
  (with-stmt
    ["WITH \"t\" AS (DELETE FROM \"foo\") DELETE FROM \"bar\""]
    (with db [:t (delete db :foo)]
      (delete db :bar))))

(deftest test-with-compose
  (with-stmt
    ["WITH \"a\" AS (SELECT * FROM \"b\") SELECT * FROM \"a\" WHERE (1 = 1)"]
    (compose (with db [:a (select db [:*] (from :b))]
               (select db [:*] (from :a)))
             (where '(= 1 1)))))

;; ATTRIBUTES OF COMPOSITE TYPES

(deftest test-attr-composite-type
  (with-stmt
    ["SELECT (\"new-emp\"()).\"name\" AS \"x\""]
    (select db [(as '(.-name (new-emp)) :x)])))

(deftest test-nested-attr-composite-type
  (with-stmt
    ["SELECT ((\"new-emp\"()).\"name\").\"first\" AS \"x\""]
    (select db [(as '(.-first (.-name (new-emp))) :x)])))

(deftest test-select-as-alias
  (with-stmt
    ["SELECT (SELECT count(*) FROM \"continents\") AS \"continents\", (SELECT count(*) FROM \"countries\") AS \"countries\""]
    (select db [(as (select db ['(count :*)] (from :continents)) :continents)
                (as (select db ['(count :*)] (from :countries)) :countries)])))

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

(deftest test-case
  (with-stmt
    [(str "SELECT \"a\", CASE WHEN (\"a\" = 1) THEN ?"
          " WHEN (\"a\" = 2) THEN ? "
          "ELSE ? END FROM \"test\"")
     "one" "two" "other"]
    (select db [:a '(case (= :a 1) "one"
                          (= :a 2) "two"
                          "other")]
      (from :test))))

(deftest test-case-as-alias
  (with-stmt
    [(str "SELECT \"a\", CASE WHEN (\"a\" = 1) THEN ?"
          " WHEN (\"a\" = 2) THEN ? "
          "ELSE ? END AS \"c\" FROM \"test\"")
     "one" "two" "other"]
    (select db [:a (as '(case (= :a 1) "one"
                              (= :a 2) "two"
                              "other") :c)]
      (from :test))))

(deftest test-full-outer-join
  (with-stmt
    [(str "SELECT \"a\" FROM \"test1\""
          " FULL OUTER JOIN \"test2\" ON (\"test1\".\"b\" = \"test2\".\"b\")")]
    (select db [:a]
      (from :test1)
      (join :test2
            '(on (= :test1.b :test2.b))
            :type :full :outer true))))

;; Window functions: http://www.postgresql.org/docs/9.4/static/tutorial-window.html

(deftest test-window-compare-salaries
  (sql= (select db [:depname :empno :salary '(over (avg :salary) (partition-by :depname))]
          (from :empsalary))
        ["SELECT \"depname\", \"empno\", \"salary\", \"avg\"(\"salary\") OVER (PARTITION BY \"depname\") FROM \"empsalary\""]))

(deftest test-window-compare-salaries-by-year
  (sql= (select db [:year :depname :empno :salary '(over (avg :salary) (partition-by [:year :depname]))]
          (from :empsalary))
        ["SELECT \"year\", \"depname\", \"empno\", \"salary\", \"avg\"(\"salary\") OVER (PARTITION BY \"year\", \"depname\") FROM \"empsalary\""]))

(deftest test-window-rank-over-order-by
  (sql= (select db [:depname :empno :salary '(over (rank) (partition-by :depname (order-by (desc :salary))))]
          (from :empsalary))
        ["SELECT \"depname\", \"empno\", \"salary\", \"rank\"() OVER (PARTITION BY \"depname\" ORDER BY \"salary\" DESC) FROM \"empsalary\""]))

(deftest test-window-rank-over-multiple-cols-order-by
  (sql= (select db [:year :depname :empno :salary '(over (rank) (partition-by [:year :depname] (order-by (desc :salary))))]
          (from :empsalary))
        ["SELECT \"year\", \"depname\", \"empno\", \"salary\", \"rank\"() OVER (PARTITION BY \"year\", \"depname\" ORDER BY \"salary\" DESC) FROM \"empsalary\""]))

(deftest test-window-over-empty
  (sql= (select db [:salary '(over (sum :salary))]
          (from :empsalary))
        ["SELECT \"salary\", \"sum\"(\"salary\") OVER () FROM \"empsalary\""]))

(deftest test-window-sum-over-order-by
  (sql= (select db [:salary '(over (sum :salary) (order-by :salary))]
          (from :empsalary))
        ["SELECT \"salary\", \"sum\"(\"salary\") OVER (ORDER BY \"salary\") FROM \"empsalary\""]))

(deftest test-window-rank-over-partition-by
  (sql= (select db [:depname :empno :salary :enroll-date]
          (from (as (select db [:depname :empno :salary :enroll-date
                                (as '(over (rank) (partition-by :depname (order-by (desc :salary) :empno))) :pos)]
                      (from :empsalary))
                    :ss))
          (where '(< pos 3)))
        [(str "SELECT \"depname\", \"empno\", \"salary\", \"enroll-date\" "
              "FROM (SELECT \"depname\", \"empno\", \"salary\", \"enroll-date\", "
              "\"rank\"() OVER (PARTITION BY \"depname\" ORDER BY \"salary\" DESC, \"empno\") AS \"pos\" "
              "FROM \"empsalary\") AS \"ss\" WHERE (pos < 3)")]))

(deftest test-window-alias
  (sql= (select db ['(over (sum :salary) :w)
                    '(over (avg :salary) :w)]
          (from :empsalary)
          (window (as '(partition-by
                        :depname (order-by (desc salary))) :w)))
        [(str "SELECT \"sum\"(\"salary\") OVER (\"w\"), "
              "\"avg\"(\"salary\") OVER (\"w\") "
              "FROM \"empsalary\" "
              "WINDOW \"w\" AS (PARTITION BY \"depname\" ORDER BY salary DESC)")]))

(deftest test-window-alias-order-by
  (sql= (select db [(as '(over (sum :salary) :w) :sum)]
          (from :empsalary)
          (order-by :sum)
          (window (as '(partition-by
                        :depname (order-by (desc salary))) :w)))
        [(str "SELECT \"sum\"(\"salary\") OVER (\"w\") AS \"sum\" "
              "FROM \"empsalary\" "
              "WINDOW \"w\" AS (PARTITION BY \"depname\" ORDER BY salary DESC) "
              "ORDER BY \"sum\"")]))

(deftest test-not-expr
  (sql= (select db [:*]
          (where `(not (= :id 1))))
        ["SELECT * WHERE (NOT (\"id\" = 1))"]))

(deftest test-slash-in-function-name
  (sql= (select db [`(~(symbol "m/s->km/h") 10)])
        ["SELECT \"m/s->km/h\"(10)"]))

(deftest test-upsert-on-conflict-do-update
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

(deftest test-upsert-on-conflict-do-nothing
  (sql= (insert db :distributors [:did :dname]
          (values [{:did 7 :dname "Redline GmbH"}])
          (on-conflict [:did]
            (do-nothing)))
        [(str "INSERT INTO \"distributors\" (\"did\", \"dname\") "
              "VALUES (7, ?) "
              "ON CONFLICT (\"did\") "
              "DO NOTHING")
         "Redline GmbH"]))

(deftest test-upsert-on-conflict-do-nothing-returning
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

(deftest test-upsert-on-conflict-do-update-where
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

(deftest test-upsert-on-conflict-where-do-nothing
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

(deftest test-upsert-on-conflict-on-constraint-do-nothing
  (sql= (insert db :distributors [:did :dname]
          (values [{:did 9 :dname "Antwerp Design"}])
          (on-conflict-on-constraint :distributors_pkey
            (do-nothing)))
        [(str "INSERT INTO \"distributors\" (\"did\", \"dname\") "
              "VALUES (9, ?) "
              "ON CONFLICT ON CONSTRAINT \"distributors_pkey\" "
              "DO NOTHING")
         "Antwerp Design"]))

(deftest test-sql-placeholder-constant
  (let [db (assoc db :sql-placeholder sql-placeholder-constant)]
    (sql= (select db  [:*]
            (from :distributors)
            (where '(and (= :dname "Anvil Distribution")
                         (= :zipcode "21201"))))
          ["SELECT * FROM \"distributors\" WHERE ((\"dname\" = ?) and (\"zipcode\" = ?))"
           "Anvil Distribution" "21201"])))

(deftest test-sql-placeholder-count
  (let [db (assoc db :sql-placeholder sql-placeholder-count)]
    (sql= (select db  [:*]
            (from :distributors)
            (where '(and (= :dname "Anvil Distribution")
                         (= :zipcode "21201"))))
          ["SELECT * FROM \"distributors\" WHERE ((\"dname\" = $1) and (\"zipcode\" = $2))"
           "Anvil Distribution" "21201"])))

(deftest test-sql-placeholder-count-subselect
  (let [db (assoc db :sql-placeholder sql-placeholder-count)]
    (sql= (select db ["a" "b" :*]
            (from (as (select db ["c" "d"]) :x)))
          ["SELECT $1, $2, * FROM (SELECT $3, $4) AS \"x\"" "a" "b" "c" "d"])))

(deftest test-explain
  (sql= (explain db
          (select db [:*]
            (from :foo)))
        ["EXPLAIN SELECT * FROM \"foo\""]))

(deftest test-explain-boolean-options
  (doseq [option [:analyze :buffers :costs :timing :verbose]
          value [true false]]
    (sql= (explain db
            (select db [:*]
              (from :foo))
            {option value})
          [(format "EXPLAIN (%s %s) SELECT * FROM \"foo\""
                   (str/upper-case (name option))
                   (str/upper-case (str value)))])))

(deftest test-explain-multiple-options
  (sql= (explain db
          (select db [:*]
            (from :foo))
          {:analyze true
           :verbose true})
        ["EXPLAIN (ANALYZE TRUE, VERBOSE TRUE) SELECT * FROM \"foo\""]))

(deftest test-explain-format
  (doseq [value [:text :xml :json :yaml]]
    (sql= (explain db
            (select db [:*]
              (from :foo))
            {:format value})
          [(format "EXPLAIN (FORMAT %s) SELECT * FROM \"foo\""
                   (str/upper-case (name value)))])))
