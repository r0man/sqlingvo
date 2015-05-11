(ns sqlingvo.core-test
  (:import java.util.Date)
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.java.io :refer [file]]
            [clojure.test :refer :all]
            [sqlingvo.compiler :refer [compile-stmt]]
            [sqlingvo.core :refer :all]
            [sqlingvo.db :as db]
            [sqlingvo.expr :refer :all]
            [sqlingvo.util :refer :all]))

(def db (db/postgresql))

(defmacro deftest-stmt [name sql forms & body]
  `(deftest ~name
     (let [[result# ~'stmt] (~forms {})]
       (is (= ~sql (sqlingvo.core/sql ~'stmt)))
       ~@body)))

(deftest test-from
  (let [[from stmt] ((from :continents) {})]
    (is (= [{:op :table :children [:name] :name :continents}] from))
    (is (= {:from [{:op :table, :children [:name] :name :continents}]} stmt))))

;; COMPOSE

(deftest-stmt test-compose
  ["SELECT \"id\", \"name\" FROM \"continents\" WHERE (\"id\" = 1) ORDER BY \"name\""]
  (compose (select db [:id :name]
             (from :continents))
           (where '(= :id 1))
           (order-by :name)))

(deftest-stmt test-compose-where-clause-using-and
  ["SELECT \"color\", \"num_sides\" FROM \"shapes\" WHERE ((\"num_sides\" = 3) and (\"color\" = ?))" "green"]
  (let [triangles (compose (select db [:color :num_sides] (from :shapes))
                           (where '(= :num_sides 3)))]
    (compose triangles (where '(= :color "green") :and))))

(deftest-stmt test-compose-selects
  ["SELECT 3, 2, 1"]
  (compose (select db [1 2 3])
           (select db [3 2 1])))

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

(deftest-stmt test-cast-int-as-text
  ["SELECT CAST(1 AS text)"]
  (select db [`(cast 1 :text)]))

(deftest-stmt test-cast-text-as-int
  ["SELECT CAST(? AS int)" "1"]
  (select db [`(cast "1" :int)]))

(deftest-stmt test-cast-with-alias
  ["SELECT CAST(? AS int) AS \"numeric_id\"" "1"]
  (select db [(as `(cast "1" :int) :numeric_id)]))

;; CREATE TABLE

(deftest-stmt test-create-table-tmp-if-not-exists-inherits
  ["CREATE TEMPORARY TABLE IF NOT EXISTS \"import\" () INHERITS (\"quotes\")"]
  (create-table db :import
    (temporary true)
    (if-not-exists true)
    (inherits :quotes))
  (is (= :create-table (:op stmt)))
  (is (= {:op :temporary} (:temporary stmt)))
  (is (= {:op :if-not-exists} (:if-not-exists stmt)))
  (is (= [(parse-table :quotes)] (:inherits stmt))))

(deftest-stmt test-create-table-tmp-if-not-exists-false
  ["CREATE TEMPORARY TABLE \"import\" () INHERITS (\"quotes\")"]
  (create-table db :import
    (temporary true)
    (if-not-exists false)
    (inherits :quotes)))

(deftest-stmt test-create-table-like-including-defaults
  ["CREATE TABLE \"tmp_films\" (LIKE \"films\" INCLUDING DEFAULTS)"]
  (create-table db :tmp-films
    (like :films :including [:defaults]))
  (is (= :create-table (:op stmt)))
  (let [like (:like stmt)]
    (is (= :like (:op like)))
    (is (= (parse-table :films) (:table like)))
    (is (= [:defaults] (:including like)))))

(deftest-stmt test-create-table-like-excluding-indexes
  ["CREATE TABLE \"tmp_films\" (LIKE \"films\" EXCLUDING INDEXES)"]
  (create-table db :tmp-films
    (like :films :excluding [:indexes]))
  (is (= :create-table (:op stmt)))
  (let [like (:like stmt)]
    (is (= :like (:op like)))
    (is (= (parse-table :films) (:table like)))
    (is (= [:indexes] (:excluding like)))))

(deftest-stmt test-create-table-films
  [(str "CREATE TABLE \"films\" ("
        "\"code\" CHAR(5) PRIMARY KEY, "
        "\"title\" VARCHAR(40) NOT NULL, "
        "\"did\" INTEGER NOT NULL, "
        "\"date_prod\" DATE, "
        "\"kind\" VARCHAR(10), "
        "\"len\" INTERVAL, "
        "\"created_at\" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT \"now\"(), "
        "\"updated_at\" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT \"now\"())")]
  (create-table db :films
    (column :code :char :length 5 :primary-key? true)
    (column :title :varchar :length 40 :not-null? true)
    (column :did :integer :not-null? true)
    (column :date-prod :date)
    (column :kind :varchar :length 10)
    (column :len :interval)
    (column :created-at :timestamp-with-time-zone :not-null? true :default '(now))
    (column :updated-at :timestamp-with-time-zone :not-null? true :default '(now))))

(deftest-stmt test-create-table-compound-primary-key
  [(str "CREATE TABLE \"ratings\" ("
        "\"id\" SERIAL, "
        "\"user_id\" INTEGER NOT NULL, "
        "\"spot_id\" INTEGER NOT NULL, "
        "\"rating\" INTEGER NOT NULL, "
        "\"created_at\" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT \"now\"(), "
        "\"updated_at\" TIMESTAMP WITH "
        "TIME ZONE NOT NULL DEFAULT \"now\"(), "
        "PRIMARY KEY(user_id, spot_id, created_at))")]
  (create-table db :ratings
    (column :id :serial)
    (column :user-id :integer :not-null? true :references :users/id)
    (column :spot-id :integer :not-null? true :references :spots/id)
    (column :rating :integer :not-null? true)
    (column :created-at :timestamp-with-time-zone :not-null? true :default '(now))
    (column :updated-at :timestamp-with-time-zone :not-null? true :default '(now))
    (primary-key :user-id :spot-id :created-at)))

;; COPY

(deftest-stmt test-copy-stdin
  ["COPY \"country\" FROM STDIN"]
  (copy db :country []
    (from :stdin))
  (is (= :copy (:op stmt)))
  (is (= [:stdin] (:from stmt))))

(deftest-stmt test-copy-country
  ["COPY \"country\" FROM ?" "/usr1/proj/bray/sql/country_data"]
  (copy db :country []
    (from "/usr1/proj/bray/sql/country_data"))
  (is (= :copy (:op stmt)))
  (is (= ["/usr1/proj/bray/sql/country_data"] (:from stmt))))

(deftest-stmt test-copy-country-with-encoding
  ["COPY \"country\" FROM ? ENCODING ?" "/usr1/proj/bray/sql/country_data" "UTF-8"]
  (copy db :country []
    (from "/usr1/proj/bray/sql/country_data")
    (encoding "UTF-8"))
  (is (= :copy (:op stmt)))
  (is (= ["/usr1/proj/bray/sql/country_data"] (:from stmt))))

(deftest-stmt test-copy-country-with-delimiter
  ["COPY \"country\" FROM ? DELIMITER ?" "/usr1/proj/bray/sql/country_data" " "]
  (copy db :country []
    (from "/usr1/proj/bray/sql/country_data")
    (delimiter " ")))

(deftest-stmt test-copy-country-columns
  ["COPY \"country\" (\"id\", \"name\") FROM ?" "/usr1/proj/bray/sql/country_data"]
  (copy db :country [:id :name]
    (from "/usr1/proj/bray/sql/country_data"))
  (is (= :copy (:op stmt)))
  (is (= ["/usr1/proj/bray/sql/country_data"] (:from stmt)))
  (is (= (map parse-column [:id :name]) (:columns stmt))))

(deftest test-copy-from-expands-to-absolute-path
  (is (= ["COPY \"country\" FROM ?" (.getAbsolutePath (file "country_data"))]
         (sql (copy db :country [] (from (file "country_data")))))))

(deftest-stmt test-select-from
  ["SELECT * FROM \"continents\""]
  (select db [:*]
    (from :continents)))

(deftest-stmt test-select-in-list
  ["SELECT * FROM \"continents\" WHERE 1 IN (1, 2, 3)"]
  (select db [:*]
    (from :continents)
    (where '(in 1 (1 2 3)))))

(deftest-stmt test-select-in-empty-list
  ["SELECT * FROM \"continents\" WHERE 1 IN (NULL)"]
  (select db [:*]
    (from :continents)
    (where '(in 1 ()))))

;; DELETE

(deftest-stmt test-delete-films
  ["DELETE FROM \"films\""]
  (delete db :films)
  (is (= :delete (:op stmt)))
  (is (= (parse-table :films) (:table stmt))))

(deftest-stmt test-delete-all-films-but-musicals
  ["DELETE FROM \"films\" WHERE (\"kind\" <> ?)" "Musical"]
  (delete db :films
    (where '(<> :kind "Musical")))
  (is (= :delete (:op stmt)))
  (is (= (parse-table :films) (:table stmt)))
  (is (= (parse-condition '(<> :kind "Musical")) (:where stmt))))

(deftest-stmt test-delete-completed-tasks-returning-all
  ["DELETE FROM \"tasks\" WHERE (\"status\" = ?) RETURNING *" "DONE"]
  (delete db :tasks
    (where '(= :status "DONE"))
    (returning *))
  (is (= :delete (:op stmt)))
  (is (= (parse-table :tasks) (:table stmt)))
  (is (= (parse-condition '(= :status "DONE")) (:where stmt)))
  (is (= [(parse-expr *)] (:returning stmt))))

(deftest-stmt test-delete-films-by-producer-name
  ["DELETE FROM \"films\" WHERE \"producer_id\" IN (SELECT \"id\" FROM \"producers\" WHERE (\"name\" = ?))" "foo"]
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
         (:where stmt))))

(deftest-stmt test-delete-quotes
  [(str "DELETE FROM \"quotes\" WHERE ((\"company_id\" = 1) and (\"date\" > (SELECT \"min\"(\"date\") FROM \"import\")) and "
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
         (:where stmt))))

;; DROP TABLE

(deftest-stmt test-drop-continents
  ["DROP TABLE \"continents\""]
  (drop-table db [:continents])
  (is (= :drop-table (:op stmt)))
  (is (= [(parse-table :continents)] (:tables stmt)))
  (is (= [:tables] (:children stmt))))

(deftest-stmt test-drop-continents-and-countries
  ["DROP TABLE \"continents\", \"countries\""]
  (drop-table db [:continents :countries])
  (is (= :drop-table (:op stmt)))
  (is (= (map parse-table [:continents :countries]) (:tables stmt)))
  (is (= [:tables] (:children stmt))))

(deftest-stmt test-drop-continents-countries-if-exists-restrict
  ["DROP TABLE IF EXISTS \"continents\", \"countries\" RESTRICT"]
  (drop-table db [:continents :countries]
    (if-exists true)
    (restrict true))
  (is (= :drop-table (:op stmt)))
  (is (= {:op :if-exists} (:if-exists stmt)))
  (is (= (map parse-table [:continents :countries]) (:tables stmt)))
  (is (= {:op :restrict} (:restrict stmt)))
  (is (= [:tables] (:children stmt))))

(deftest-stmt test-drop-continents-if-exists
  ["DROP TABLE IF EXISTS \"continents\""]
  (drop-table db [:continents]
    (if-exists true))
  (is (= (map parse-table [:continents]) (:tables stmt)))
  (is (= [:tables] (:children stmt))))

(deftest-stmt test-drop-continents-if-exists-false
  ["DROP TABLE \"continents\""]
  (drop-table db [:continents]
    (if-exists false))
  (is (= (map parse-table [:continents]) (:tables stmt)))
  (is (= [:tables] (:children stmt))))

;; INSERT

(deftest-stmt test-insert-default-values
  ["INSERT INTO \"films\" DEFAULT VALUES"]
  (insert db :films []
    (values :default))
  (is (= :insert (:op stmt)))
  (is (= [] (:columns stmt)))
  (is (= true (:default-values stmt)))
  (is (= (parse-table :films) (:table stmt))))

(deftest-stmt test-insert-single-row-as-map
  ["INSERT INTO \"films\" (\"date_prod\", \"title\", \"did\", \"kind\", \"code\") VALUES (?, ?, 106, ?, ?)"
   "1961-06-16" "Yojimbo" "Drama" "T_601"]
  (insert db :films []
    (values {:code "T_601" :title "Yojimbo" :did 106 :date-prod "1961-06-16" :kind "Drama"}))
  (is (= :insert (:op stmt)))
  (is (= [] (:columns stmt)))
  (is (= [(parse-map-expr {:code "T_601" :title "Yojimbo" :did 106 :date-prod "1961-06-16" :kind "Drama"})]
         (:values stmt)))
  (is (= (parse-table :films) (:table stmt))))

(deftest-stmt test-insert-single-row-as-seq
  ["INSERT INTO \"films\" (\"date_prod\", \"title\", \"did\", \"kind\", \"code\") VALUES (?, ?, 106, ?, ?)"
   "1961-06-16" "Yojimbo" "Drama" "T_601"]
  (insert db :films []
    (values [{:code "T_601" :title "Yojimbo" :did 106 :date-prod "1961-06-16" :kind "Drama"}]))
  (is (= :insert (:op stmt)))
  (is (= [] (:columns stmt)))
  (is (= [(parse-map-expr {:code "T_601" :title "Yojimbo" :did 106 :date-prod "1961-06-16" :kind "Drama"})]
         (:values stmt)))
  (is (= (parse-table :films) (:table stmt))))

(deftest-stmt test-insert-multi-row
  ["INSERT INTO \"films\" (\"date_prod\", \"title\", \"did\", \"kind\", \"code\") VALUES (?, ?, 110, ?, ?), (?, ?, 140, ?, ?)"
   "1985-02-10" "Tampopo" "Comedy" "B6717" "1985-02-10" "The Dinner Game" "Comedy" "HG120"]
  (insert db :films []
    (values [{:code "B6717" :title "Tampopo" :did 110 :date-prod "1985-02-10" :kind "Comedy"},
             {:code "HG120" :title "The Dinner Game" :did 140 :date-prod "1985-02-10":kind "Comedy"}]))
  (is (= :insert (:op stmt)))
  (is (= [] (:columns stmt)))
  (is (= (map parse-map-expr
              [{:code "B6717" :title "Tampopo" :did 110 :date-prod "1985-02-10" :kind "Comedy"},
               {:code "HG120" :title "The Dinner Game" :did 140 :date-prod "1985-02-10":kind "Comedy"}])
         (:values stmt)))
  (is (= (parse-table :films) (:table stmt))))

(deftest-stmt test-insert-returning
  ["INSERT INTO \"distributors\" (\"dname\", \"did\") VALUES (?, 106) RETURNING *" "XYZ Widgets"]
  (insert db :distributors []
    (values [{:did 106 :dname "XYZ Widgets"}])
    (returning *))
  (is (= :insert (:op stmt)))
  (is (= [] (:columns stmt)))
  (is (= (parse-table :distributors) (:table stmt)))
  (is (= [(parse-expr *)] (:returning stmt))))

(deftest-stmt test-insert-subselect
  ["INSERT INTO \"films\" SELECT * FROM \"tmp_films\" WHERE (\"date_prod\" < ?)" "2004-05-07"]
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
         (:select stmt))))

(deftest-stmt test-insert-airports
  [(str "INSERT INTO \"airports\" (\"country_id\", \"name\", \"gps_code\", \"iata_code\", \"wikipedia_url\", \"location\") "
        "SELECT DISTINCT ON (\"a\".\"iata_code\") \"c\".\"id\", \"a\".\"name\", \"a\".\"gps_code\", \"a\".\"iata_code\", \"a\".\"wikipedia\", \"a\".\"geom\" "
        "FROM \"natural_earth\".\"airports\" \"a\" JOIN \"countries\" \"c\" ON (\"c\".\"geography\" && \"a\".\"geom\") "
        "LEFT JOIN \"airports\" ON (\"airports\".\"iata_code\" = \"a\".\"iata_code\") "
        "WHERE ((\"a\".\"gps_code\" IS NOT NULL) and (\"a\".\"iata_code\" IS NOT NULL) and (\"airports\".\"iata_code\" IS NULL))")]
  (insert db :airports [:country-id, :name :gps-code :iata-code :wikipedia-url :location]
    (select db (distinct [:c.id :a.name :a.gps-code :a.iata-code :a.wikipedia :a.geom] :on [:a.iata-code])
      (from (as :natural-earth.airports :a))
      (join (as :countries :c) '(on (:&& :c.geography :a.geom)))
      (join :airports '(on (= :airports.iata-code :a.iata-code)) :type :left)
      (where '(and (is-not-null :a.gps-code)
                   (is-not-null :a.iata-code)
                   (is-null :airports.iata-code))))))

(deftest-stmt test-insert-only-columns
  ["INSERT INTO \"x\" (\"a\", \"b\") VALUES (1, 2)"]
  (insert db :x [:a :b] (values [{:a 1 :b 2 :c 3}])))

(deftest-stmt test-insert-values-with-fn-call
  ["INSERT INTO \"x\" (\"a\", \"b\") VALUES (1, \"lower\"(?)), (2, ?)" "B" "b"]
  (insert db :x [:a :b]
    (values [{:a 1 :b '(lower "B")}
             {:a 2 :b "b"}])))

(deftest test-insert-fixed-columns-mixed-values
  (is (= (sql (insert db :table [:a :b]
                (values [{:a 1 :b 2} {:b 3} {:c 3}])))
         [(str "INSERT INTO \"table\" (\"a\", \"b\") VALUES (1, 2), "
               "(NULL, 3), (NULL, NULL)")])))

(deftest test-insert-fixed-columns-mixed-values-2
  (is (= (sql (insert db :quotes [:id :exchange-id :company-id
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
                          :id 6}])))
         [(str "INSERT INTO \"quotes\" (\"id\", \"exchange_id\", "
               "\"company_id\", \"symbol\", \"created_at\", \"updated_at\") "
               "VALUES (5, 2, 5, ?, ?, ?), (6, 2, NULL, ?, ?, ?)")
          "MSFT"
          #inst "2012-11-02T18:22:59.688-00:00"
          #inst "2012-11-02T18:22:59.688-00:00"
          "SPY"
          #inst "2012-11-02T18:22:59.688-00:00"
          #inst "2012-11-02T18:22:59.688-00:00"])))

;; SELECT

(deftest-stmt test-select-1
  ["SELECT 1"]
  (select db [1])
  (is (= :select (:op stmt)))
  (is (= [(parse-expr 1)] (:exprs stmt))))

(deftest-stmt test-select-1-as
  ["SELECT 1 AS \"n\""]
  (select db [(as 1 :n)])
  (is (= :select (:op stmt)))
  (is (= [(parse-expr (as 1 :n))] (:exprs stmt))))

(deftest-stmt test-select-x-as-x
  ["SELECT ? AS \"x\"" "x"]
  (select db [(as "x" :x)])
  (is (= :select (:op stmt)))
  (is (= [(parse-expr (as "x" :x))] (:exprs stmt))))

(deftest-stmt test-select-1-2-3
  ["SELECT 1, 2, 3"]
  (select db [1 2 3])
  (is (= :select (:op stmt)))
  (is (= (map parse-expr [1 2 3]) (:exprs stmt))))

(deftest-stmt test-select-1-2-3-as
  ["SELECT 1 AS \"a\", 2 AS \"b\", 3 AS \"c\""]
  (select db [(as 1 :a) (as 2 :b) (as 3 :c)])
  (is (= :select (:op stmt)))
  (is (= (map parse-expr [(as 1 :a) (as 2 :b) (as 3 :c)])
         (:exprs stmt))))

(deftest-stmt test-select-count-as
  ["SELECT count(*) AS \"count\" FROM \"tweets\""]
  (select db [(as '(count :*) :count)]
    (from :tweets)))

(deftest-stmt test-select-count-distinct
  ["SELECT count(DISTINCT \"user_id\") FROM \"tweets\""]
  (select db ['(count distinct :user-id)]
    (from :tweets)))

(deftest-stmt test-select-select-1
  ["SELECT (SELECT 1)"]
  (select db [(select db [1])])
  (is (= :select (:op stmt)))
  (is (= [(ast (select db [1]))] (:exprs stmt))))

(deftest-stmt test-select-1-in-1-2-3
  ["SELECT 1 WHERE 1 IN (1, 2, 3)"]
  (select db [1]
    (where '(in 1 (1 2 3))))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr 1)] (:exprs stmt)))
  (is (= (parse-condition '(in 1 (1 2 3))) (:where stmt))))

(deftest-stmt test-select-1-in-1-2-3-backquote
  ["SELECT 1 WHERE 1 IN (1, 2, 3)"]
  (select db [1]
    (where `(in 1 (1 2 3)))))

(deftest-stmt test-select-select-1-select-x
  ["SELECT (SELECT 1), (SELECT ?)" "x"]
  (select db [(select db [1]) (select db ["x"])])
  (is (= :select (:op stmt)))
  (is (= [(ast (select db [1])) (ast (select db ["x"]))] (:exprs stmt))))

(deftest-stmt test-select-string
  ["SELECT * FROM \"continents\" WHERE (\"name\" = ?)" "Europe"]
  (select db [*]
    (from :continents)
    (where '(= :name "Europe")))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= (parse-condition '(= :name "Europe")) (:where stmt))))

(deftest-stmt test-select-where-single-arg-and
  ["SELECT 1 WHERE (1 = 1)"]
  (select db [1]
    (where '(and (= 1 1))))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr 1)] (:exprs stmt)))
  (is (= (parse-condition '(and (= 1 1))) (:where stmt))))

(deftest-stmt test-select-less-2-arity
  ["SELECT 1 WHERE (1 < 2)"]
  (select db [1]
    (where '(< 1 2)))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr 1)] (:exprs stmt)))
  (is (= (parse-condition '(< 1 2)) (:where stmt))))

(deftest-stmt test-select-less-3-arity
  ["SELECT 1 WHERE (1 < 2) AND (2 < 3)"]
  (select db [1]
    (where '(< 1 2 3)))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr 1)] (:exprs stmt)))
  (is (= (parse-condition '(< 1 2 3)) (:where stmt))))

(deftest-stmt test-select-like
  ["SELECT * FROM \"films\" WHERE (\"title\" like ?)" "%Zombie%"]
  (select db [*]
    (from :films)
    (where '(like :title "%Zombie%")))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-from :films)] (:from stmt)))
  (is (= (parse-condition '(like :title "%Zombie%")) (:where stmt))))

(deftest-stmt test-select-not-like
  ["SELECT * FROM \"films\" WHERE (\"title\" NOT LIKE ?)" "%Zombie%"]
  (select db [*]
    (from :films)
    (where '(not-like :title "%Zombie%")))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-from :films)] (:from stmt)))
  (is (= (parse-condition '(not-like :title "%Zombie%")) (:where stmt))))

(deftest-stmt test-select-continents
  ["SELECT * FROM \"continents\""]
  (select db [*]
    (from :continents))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-from :continents)] (:from stmt))))

(deftest-stmt test-select-continents-qualified
  ["SELECT \"continents\".* FROM \"continents\""]
  (select db [:continents.*]
    (from :continents))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr :continents.*)] (:exprs stmt)))
  (is (= [(parse-from :continents)] (:from stmt))))

(deftest-stmt test-select-films
  ["SELECT * FROM \"films\""]
  (select db [*] (from :films))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-from :films)] (:from stmt))))

(deftest-stmt test-select-comedy-films
  ["SELECT * FROM \"films\" WHERE (\"kind\" = ?)" "Comedy"]
  (select db [*]
    (from :films)
    (where '(= :kind "Comedy")))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= (parse-condition '(= :kind "Comedy")) (:where stmt)))
  (is (= [(parse-from :films)] (:from stmt))))

(deftest-stmt test-select-is-null
  ["SELECT 1 WHERE (NULL IS NULL)"]
  (select db [1]
    (where '(is-null nil)))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr 1)] (:exprs stmt)))
  (is (= (parse-condition '(is-null nil)) (:where stmt))))

(deftest-stmt test-select-is-not-null
  ["SELECT 1 WHERE (NULL IS NOT NULL)"]
  (select db [1]
    (where '(is-not-null nil)))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr 1)] (:exprs stmt)))
  (is (= (parse-condition '(is-not-null nil)) (:where stmt))))

(deftest-stmt test-select-backquote-date
  ["SELECT * FROM \"countries\" WHERE (\"created_at\" > ?)" (Date. 0)]
  (select db [*]
    (from :countries)
    (where `(> :created-at ~(Date. 0))))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= (parse-condition `(> :created-at ~(Date. 0))) (:where stmt))))

(deftest-stmt test-select-star-number-string
  ["SELECT *, 1, ?" "x"]
  (select db [* 1 "x"])
  (is (= :select (:op stmt)))
  (is (= (map parse-expr [* 1 "x"]) (:exprs stmt))))

(deftest-stmt test-select-column
  ["SELECT \"created_at\" FROM \"continents\""]
  (select db [:created-at]
    (from :continents))
  (is (= :select (:op stmt)))
  (is (= [(parse-table :continents)] (:from stmt)))
  (is (= [(parse-expr :created-at)] (:exprs stmt))))

(deftest-stmt test-select-columns
  ["SELECT \"name\", \"created_at\" FROM \"continents\""]
  (select db [:name :created-at]
    (from :continents))
  (is (= :select (:op stmt)))
  (is (= [(parse-table :continents)] (:from stmt)))
  (is (= (map parse-expr [:name :created-at]) (:exprs stmt))))

(deftest-stmt test-select-column-alias
  ["SELECT \"created_at\" AS \"c\" FROM \"continents\""]
  (select db [(as :created-at :c)]
    (from :continents))
  (is (= :select (:op stmt)))
  (is (= [(parse-table :continents)] (:from stmt)))
  (is (= [(parse-expr (as :created-at :c))] (:exprs stmt))))

(deftest-stmt test-select-multiple-fns
  ["SELECT \"greatest\"(1, 2), \"lower\"(?)" "X"]
  (select db ['(greatest 1 2) '(lower "X")])
  (is (= :select (:op stmt)))
  (is (= (map parse-expr ['(greatest 1 2) '(lower "X")]) (:exprs stmt))))

(deftest-stmt test-select-nested-fns
  ["SELECT (1 + \"greatest\"(2, 3))"]
  (select db ['(+ 1 (greatest 2 3))])
  (is (= :select (:op stmt)))
  (is (= [(parse-expr '(+ 1 (greatest 2 3)))] (:exprs stmt))))

(deftest-stmt test-select-fn-alias
  ["SELECT \"max\"(\"created_at\") AS \"m\" FROM \"continents\""]
  (select db [(as '(max :created-at) :m)]
    (from :continents))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr (as '(max :created-at) :m))] (:exprs stmt)))
  (is (= [(parse-table :continents)] (:from stmt))))

(deftest-stmt test-select-limit
  ["SELECT * FROM \"continents\" LIMIT 10"]
  (select db [*]
    (from :continents)
    (limit 10))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-table :continents)] (:from stmt)))
  (is (= {:op :limit :count 10} (:limit stmt))))

(deftest-stmt test-select-limit-nil
  ["SELECT * FROM \"continents\""]
  (select db [*]
    (from :continents)
    (limit nil)))

(deftest-stmt test-select-offset
  ["SELECT * FROM \"continents\" OFFSET 15"]
  (select db [*]
    (from :continents)
    (offset 15))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-table :continents)] (:from stmt)))
  (is (= {:op :offset :start 15} (:offset stmt))))

(deftest-stmt test-select-limit-offset
  ["SELECT * FROM \"continents\" LIMIT 10 OFFSET 20"]
  (select db [*]
    (from :continents)
    (limit 10)
    (offset 20))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-table :continents)] (:from stmt)))
  (is (= {:op :limit :count 10} (:limit stmt)))
  (is (= {:op :offset :start 20} (:offset stmt))))

(deftest-stmt test-select-column-max
  ["SELECT \"max\"(\"created_at\") FROM \"continents\""]
  (select db ['(max :created-at)]
    (from :continents))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr '(max :created-at))] (:exprs stmt)))
  (is (= [(parse-table :continents)] (:from stmt))))

(deftest-stmt test-select-distinct-subquery-alias
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
    (is (= (map parse-expr [(as 1 :a) (as 2 :b)]) (:exprs from)))))

(deftest-stmt test-select-distinct-on-subquery-alias
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
    (is (= (map parse-expr [(as 1 :a) (as 2 :b)]) (:exprs from)))))

(deftest-stmt test-select-most-recent-weather-report
  ["SELECT DISTINCT ON (\"location\") \"location\", \"time\", \"report\" FROM \"weather_reports\" ORDER BY \"location\", \"time\" DESC"]
  (select db (distinct [:location :time :report] :on [:location])
    (from :weather-reports)
    (order-by :location (desc :time)))
  (is (= :select (:op stmt)))
  (let [distinct (:distinct stmt)]
    (is (= :distinct (:op distinct)))
    (is (= (map parse-expr [:location :time :report]) (:exprs distinct)))
    (is (= [(parse-expr :location)] (:on distinct))))
  (is (= [(parse-from :weather-reports)] (:from stmt)))
  (is (= [(parse-expr :location) (desc :time)] (:order-by stmt))))

(deftest-stmt test-select-order-by-asc
  ["SELECT * FROM \"continents\" ORDER BY \"created_at\" ASC"]
  (select db [*]
    (from :continents)
    (order-by (asc :created-at)))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-from :continents)] (:from stmt)))
  (is (= [(parse-expr (asc :created-at))] (:order-by stmt))))

(deftest-stmt test-select-order-by-asc-expr
  ["SELECT * FROM \"weather\".\"datasets\" ORDER BY \"abs\"((\"st_scalex\"(\"rast\") * \"st_scaley\"(\"rast\"))) DESC"]
  (select db [*]
    (from :weather.datasets)
    (order-by (desc '(abs (* (st_scalex :rast) (st_scaley :rast)))))))

(deftest-stmt test-select-order-by-desc
  ["SELECT * FROM \"continents\" ORDER BY \"created_at\" DESC"]
  (select db [*]
    (from :continents)
    (order-by (desc :created-at)))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-from :continents)] (:from stmt)))
  (is (= [(parse-expr (desc :created-at))] (:order-by stmt))))

(deftest-stmt test-select-order-by-nulls-first
  ["SELECT * FROM \"continents\" ORDER BY \"created_at\" NULLS FIRST"]
  (select db [*]
    (from :continents)
    (order-by (nulls :created-at :first)))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-from :continents)] (:from stmt)))
  (is (= [(parse-expr (nulls :created-at :first))] (:order-by stmt))))

(deftest-stmt test-select-order-by-nulls-last
  ["SELECT * FROM \"continents\" ORDER BY \"created_at\" NULLS LAST"]
  (select db [*]
    (from :continents)
    (order-by (nulls :created-at :last)))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-from :continents)] (:from stmt)))
  (is (= [(parse-expr (nulls :created-at :last))] (:order-by stmt))))

(deftest-stmt test-select-order-by-if-true
  ["SELECT * FROM \"continents\" ORDER BY \"name\""]
  (let [opts {:order-by :name}]
    (select db [*]
      (from :continents)
      (if (:order-by opts)
        (order-by (:order-by opts)))))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-from :continents)] (:from stmt)))
  (is (= [(parse-expr :name)] (:order-by stmt))))

(deftest-stmt test-select-order-by-if-false
  ["SELECT * FROM \"continents\""]
  (let [opts {}]
    (select db [*]
      (from :continents)
      (if (:order-by opts)
        (order-by (:order-by opts)))))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-from :continents)] (:from stmt)))
  (is (nil? (:order-by stmt))))

(deftest-stmt test-select-order-by-nil
  ["SELECT * FROM \"continents\""]
  (select db [*]
    (from :continents)
    (order-by nil))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-from :continents)] (:from stmt)))
  (is (nil? (:order-by stmt))))

(deftest-stmt test-select-1-where-1-is-1
  ["SELECT 1 WHERE (1 = 1)"]
  (select db [1]
    (where '(= 1 1)))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr 1)] (:exprs stmt)))
  (is (= (parse-condition '(= 1 1)) (:where stmt))))

(deftest-stmt test-select-1-where-1-is-2-is-3
  ["SELECT 1 WHERE (1 = 2) AND (2 = 3)"]
  (select db [1]
    (where '(= 1 2 3)))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr 1)] (:exprs stmt)))
  (is (= (parse-condition '(= 1 2 3)) (:where stmt))))

(deftest-stmt test-select-subquery-alias
  ["SELECT * FROM (SELECT 1, 2, 3) AS \"x\""]
  (select db [*]
    (from (as (select db [1 2 3]) :x)))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (let [from (first (:from stmt))]
    (is (= :select (:op from)))
    (is (= :x (:as from)))
    (is (= (map parse-expr [1 2 3]) (:exprs from)))))

(deftest-stmt test-select-subqueries-alias
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
    (is (= [(parse-expr 2)] (:exprs from)))))

(deftest-stmt test-select-parition-by
  ["SELECT \"id\", \"lag\"(\"close\") over (partition by \"company_id\" order by \"date\" desc) FROM \"quotes\""]
  (select db [:id '((lag :close) over (partition by :company-id order by :date desc))]
    (from :quotes))
  (is (= :select (:op stmt)))
  (is (= (map parse-expr [:id '((lag :close) over (partition by :company-id order by :date desc))])
         (:exprs stmt)))
  (is (= [(parse-from :quotes)] (:from stmt))))

(deftest-stmt test-select-total-return
  ["SELECT \"id\", (\"close\" / (\"lag\"(\"close\") over (partition by \"company_id\" order by \"date\" desc) - 1)) FROM \"quotes\""]
  (select db [:id '(/ :close (- ((lag :close) over (partition by :company-id order by :date desc)) 1))]
    (from :quotes))
  (is (= :select (:op stmt)))
  (is (= (map parse-expr [:id '(/ :close (- ((lag :close) over (partition by :company-id order by :date desc)) 1))])
         (:exprs stmt)))
  (is (= [(parse-from :quotes)] (:from stmt))))

(deftest-stmt test-select-total-return-alias
  ["SELECT \"id\", (\"close\" / (\"lag\"(\"close\") over (partition by \"company_id\" order by \"date\" desc) - 1)) AS \"daily_return\" FROM \"quotes\""]
  (select db [:id (as '(/ :close (- ((lag :close) over (partition by :company-id order by :date desc)) 1)) :daily-return)]
    (from :quotes))
  (is (= :select (:op stmt)))
  (is (= (map parse-expr [:id (as '(/ :close (- ((lag :close) over (partition by :company-id order by :date desc)) 1)) :daily-return)])
         (:exprs stmt)))
  (is (= [(parse-from :quotes)] (:from stmt))))

(deftest-stmt test-select-group-by-a-order-by-1
  ["SELECT \"a\", \"max\"(\"b\") FROM \"table_1\" GROUP BY \"a\" ORDER BY 1"]
  (select db [:a '(max :b)]
    (from :table-1)
    (group-by :a)
    (order-by 1))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr :a) (parse-expr '(max :b))] (:exprs stmt)))
  (is (= [(parse-from :table-1)] (:from stmt)))
  (is (= [(parse-expr 1)] (:order-by stmt))))

(deftest-stmt test-select-order-by-query-select
  ["SELECT \"a\", \"b\" FROM \"table_1\" ORDER BY (\"a\" + \"b\"), \"c\""]
  (select db [:a :b]
    (from :table-1)
    (order-by '(+ :a :b) :c))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr :a) (parse-expr :b)] (:exprs stmt)))
  (is (= [(parse-from :table-1)] (:from stmt)))
  (is (= [(parse-expr '(+ :a :b)) (parse-expr :c)] (:order-by stmt))))

(deftest-stmt test-select-order-by-sum
  ["SELECT (\"a\" + \"b\") AS \"sum\", \"c\" FROM \"table_1\" ORDER BY \"sum\""]
  (select db [(as '(+ :a :b) :sum) :c]
    (from :table-1)
    (order-by :sum))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr (as '(+ :a :b) :sum)) (parse-expr :c)] (:exprs stmt)))
  (is (= [(parse-from :table-1)] (:from stmt)))
  (is (= [(parse-expr :sum)] (:order-by stmt))))

(deftest-stmt test-select-setval
  ["SELECT \"setval\"(\"continent_id_seq\", (SELECT \"max\"(\"id\") FROM \"continents\"))"]
  (select db [`(setval :continent-id-seq ~(select db [`(max :id)] (from :continents)))])
  (is (= :select (:op stmt)))
  (is (= (map parse-expr [`(setval :continent-id-seq ~(select db [`(max :id)] (from :continents)))])
         (:exprs stmt))))

(deftest-stmt test-select-regex-match
  ["SELECT \"id\", \"symbol\", \"quote\" FROM \"quotes\" WHERE (? ~ \"concat\"(?, \"symbol\", ?))" "$AAPL" "(^|\\s)\\$" "($|\\s)"]
  (select db [:id :symbol :quote]
    (from :quotes)
    (where `(~(symbol "~") "$AAPL" (concat "(^|\\s)\\$" :symbol "($|\\s)"))))
  (is (= :select (:op stmt)))
  (is (= [(parse-from :quotes)] (:from stmt)))
  (is (= (map parse-expr [:id :symbol :quote]) (:exprs stmt)))
  (is (= (parse-condition `(~(symbol "~") "$AAPL" (concat "(^|\\s)\\$" :symbol "($|\\s)"))) (:where stmt)))  )

(deftest-stmt test-select-join-on-columns
  ["SELECT * FROM \"countries\" JOIN \"continents\" ON (\"continents\".\"id\" = \"countries\".\"continent_id\")"]
  (select db [*]
    (from :countries)
    (join :continents '(on (= :continents.id :countries.continent-id))))
  (is (= :select (:op stmt)))
  (is (= [(parse-from :countries)] (:from stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (let [join (first (:joins stmt))]
    (is (= :join (:op join)))
    (is (= (parse-from :continents) (:from join)))
    (is (= (parse-expr '(= :continents.id :countries.continent-id)) (:on join)))))

(deftest-stmt test-select-join-with-keywords
  ["SELECT * FROM \"continents\" JOIN \"countries\" ON (\"countries\".\"continent_id\" = \"continents\".\"id\")"]
  (select db [*]
    (from :continents)
    (join :countries.continent-id :continents.id))
  (is (= :select (:op stmt)))
  (is (= [(parse-from :continents)] (:from stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (let [join (first (:joins stmt))]
    (is (= :join (:op join)))
    (is (= (parse-from :countries) (:from join)))
    (is (= (parse-expr '(= :countries.continent-id :continents.id)) (:on join)))))

(deftest-stmt test-select-join-on-columns-alias
  ["SELECT * FROM \"countries\" \"c\" JOIN \"continents\" ON (\"continents\".\"id\" = \"c\".\"continent_id\")"]
  (select db [*]
    (from (as :countries :c))
    (join :continents '(on (= :continents.id :c.continent-id))))
  (is (= :select (:op stmt)))
  (is (= [(parse-from (as :countries :c))] (:from stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (let [join (first (:joins stmt))]
    (is (= :join (:op join)))
    (is (= (parse-from :continents) (:from join)))
    (is (= (parse-expr '(= :continents.id :c.continent-id)) (:on join)))))

(deftest-stmt test-select-join-using-column
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
    (is (= (map parse-expr [:id]) (:using join)))))

(deftest-stmt test-select-join-using-columns
  ["SELECT * FROM \"countries\" JOIN \"continents\" USING (\"id\", \"created_at\")"]
  (select db [*]
    (from :countries)
    (join :continents '(using :id :created-at)))
  (is (= :select (:op stmt)))
  (is (= [(parse-from :countries)] (:from stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (let [join (first (:joins stmt))]
    (is (= :join (:op join)))
    (is (= (parse-from :continents) (:from join)))
    (is (= (map parse-expr [:id :created-at]) (:using join)))))

(deftest-stmt test-select-join-alias
  ["SELECT * FROM \"countries\" \"c\" JOIN \"continents\" ON (\"continents\".\"id\" = \"c\".\"continent_id\")"]
  (select db [*]
    (from (as :countries :c))
    (join :continents '(on (= :continents.id :c.continent-id)))))

(deftest-stmt test-select-join-syntax-quote
  ["SELECT * FROM \"countries\" \"c\" JOIN \"continents\" ON (\"continents\".\"id\" = \"c\".\"continent_id\")"]
  (select db [*]
    (from (as :countries :c))
    (join :continents `(on (= :continents.id :c.continent-id)))))

(deftest-stmt test-select-join-subselect-alias
  [(str "SELECT \"quotes\".*, \"start_date\" FROM \"quotes\" JOIN (SELECT \"company_id\", \"min\"(\"date\") AS \"start_date\" "
        "FROM \"quotes\" GROUP BY \"company_id\") AS \"start_dates\" ON ((\"quotes\".\"company_id\" = \"start_dates\".\"company_id\") and (\"quotes\".\"date\" = \"start_dates\".\"start_date\"))")]
  (select db [:quotes.* :start-date]
    (from :quotes)
    (join (as (select db [:company-id (as '(min :date) :start-date)]
                (from :quotes)
                (group-by :company-id))
              :start-dates)
          '(on (and (= :quotes.company-id :start-dates.company-id)
                    (= :quotes.date :start-dates.start-date))))))

(deftest-stmt test-select-except
  ["SELECT 1 EXCEPT SELECT 2"]
  (select db [1]
    (except (select db [2])))
  (is (= :select (:op stmt)))
  (is (= (map parse-expr [1]) (:exprs stmt)))
  (let [except (first (:set stmt))]
    (is (= :except (:op except)))
    (let [stmt (:stmt except)]
      (is (= :select (:op stmt)))
      (is (= (map parse-expr [2]) (:exprs stmt))))))

(deftest-stmt test-select-except-all
  ["SELECT 1 EXCEPT ALL SELECT 2"]
  (select db [1]
    (except (select db [2]) :all true))
  (is (= :select (:op stmt)))
  (is (= (map parse-expr [1]) (:exprs stmt)))
  (let [except (first (:set stmt))]
    (is (= :except (:op except)))
    (is (= true (:all except)))
    (let [stmt (:stmt except)]
      (is (= :select (:op stmt)))
      (is (= (map parse-expr [2]) (:exprs stmt))))))

(deftest-stmt test-select-intersect
  ["SELECT 1 INTERSECT SELECT 2"]
  (select db [1]
    (intersect (select db [2])))
  (is (= :select (:op stmt)))
  (is (= (map parse-expr [1]) (:exprs stmt)))
  (let [intersect (first (:set stmt))]
    (is (= :intersect (:op intersect)))
    (let [stmt (:stmt intersect)]
      (is (= :select (:op stmt)))
      (is (= (map parse-expr [2]) (:exprs stmt))))))

(deftest-stmt test-select-intersect-all
  ["SELECT 1 INTERSECT ALL SELECT 2"]
  (select db [1]
    (intersect (select db [2]) :all true))
  (is (= :select (:op stmt)))
  (is (= (map parse-expr [1]) (:exprs stmt)))
  (let [intersect (first (:set stmt))]
    (is (= :intersect (:op intersect)))
    (is (= true (:all intersect)))
    (let [stmt (:stmt intersect)]
      (is (= :select (:op stmt)))
      (is (= (map parse-expr [2]) (:exprs stmt))))))

(deftest-stmt test-select-union
  ["SELECT 1 UNION SELECT 2"]
  (select db [1]
    (union (select db [2])))
  (is (= :select (:op stmt)))
  (is (= (map parse-expr [1]) (:exprs stmt)))
  (let [union (first (:set stmt))]
    (is (= :union (:op union)))
    (let [stmt (:stmt union)]
      (is (= :select (:op stmt)))
      (is (= (map parse-expr [2]) (:exprs stmt))))))

(deftest-stmt test-select-union-all
  ["SELECT 1 UNION ALL SELECT 2"]
  (select db [1]
    (union (select db [2]) :all true))
  (is (= :select (:op stmt)))
  (is (= (map parse-expr [1]) (:exprs stmt)))
  (let [union (first (:set stmt))]
    (is (= :union (:op union)))
    (is (= true (:all union)))
    (let [stmt (:stmt union)]
      (is (= :select (:op stmt)))
      (is (= (map parse-expr [2]) (:exprs stmt))))))

(deftest-stmt test-select-where-combine-and-1
  ["SELECT 1 WHERE (1 = 1)"]
  (select db [1]
    (where '(= 1 1) :and)))

(deftest-stmt test-select-where-combine-and-2
  ["SELECT 1 WHERE ((1 = 1) and (2 = 2))"]
  (select db [1]
    (where '(= 1 1))
    (where '(= 2 2) :and)))

(deftest-stmt test-select-where-combine-and-3
  ["SELECT 1 WHERE (((1 = 1) and (2 = 2)) and (3 = 3))"]
  (select db [1]
    (where '(= 1 1))
    (where '(= 2 2) :and)
    (where '(= 3 3) :and)))

(deftest-stmt test-select-where-combine-or-1
  ["SELECT 1 WHERE (1 = 1)"]
  (select db [1]
    (where '(= 1 1) :or)))

(deftest-stmt test-select-where-combine-or-2
  ["SELECT 1 WHERE ((1 = 1) or (2 = 2))"]
  (select db [1]
    (where '(= 1 1))
    (where '(= 2 2) :or)))

(deftest-stmt test-select-where-combine-or-3
  ["SELECT 1 WHERE (((1 = 1) or (2 = 2)) or (3 = 3))"]
  (select db [1]
    (where '(= 1 1))
    (where '(= 2 2) :or)
    (where '(= 3 3) :or)))

(deftest-stmt test-select-where-combine-mixed
  ["SELECT 1 WHERE (((1 = 1) and (2 = 2)) or (3 = 3))"]
  (select db [1]
    (where '(= 1 1))
    (where '(= 2 2) :and)
    (where '(= 3 3) :or)))

(deftest-stmt test-substring-from-to
  ["SELECT substring(? from 2 for 3)" "Thomas"]
  (select db ['(substring "Thomas" from 2 for 3)]))

(deftest-stmt test-substring-from-to-lower
  ["SELECT \"lower\"(substring(? from 2 for 3))" "Thomas"]
  (select db ['(lower (substring "Thomas" from 2 for 3))]))

(deftest-stmt test-substring-from-pattern
  ["SELECT substring(? from ?)" "Thomas" "...$"]
  (select db ['(substring "Thomas" from "...$")]))

(deftest-stmt test-substring-from-pattern-for-escape
  ["SELECT substring(? from ? for ?)" "Thomas" "%##\"o_a#\"_" "#"]
  (select db ['(substring "Thomas" from "%##\"o_a#\"_" for "#")]))

(deftest-stmt test-trim
  ["SELECT trim(both ? from ?)" "x" "xTomxx"]
  (select db ['(trim both "x" from "xTomxx")]))

(deftest-stmt test-select-from-fn
  ["SELECT * FROM \"generate_series\"(0, 10)"]
  (select db [*] (from '(generate_series 0 10))))

(deftest-stmt test-select-from-fn-alias
  ["SELECT \"n\" FROM \"generate_series\"(0, 200) AS \"n\""]
  (select db [:n] (from (as '(generate_series 0 200) :n))))

(deftest-stmt test-select-qualified-column
  ["SELECT \"continents\".\"id\" FROM \"continents\""]
  (select db [{:op :column :table :continents :name :id}]
    (from :continents)))

(deftest-stmt test-select-qualified-keyword-column
  ["SELECT \"continents\".\"id\" FROM \"continents\""]
  (select db [:continents.id] (from :continents)))

;; TRUNCATE

(deftest-stmt test-truncate-continents
  ["TRUNCATE TABLE \"continents\""]
  (truncate db [:continents])
  (is (= :truncate (:op stmt)))
  (is (= [(parse-table :continents)] (:tables stmt)))
  (is (= [:tables] (:children stmt))))

(deftest-stmt test-truncate-continents-and-countries
  ["TRUNCATE TABLE \"continents\", \"countries\""]
  (truncate db [:continents :countries])
  (is (= :truncate (:op stmt)))
  (is (= (map parse-table [:continents :countries]) (:tables stmt)))
  (is (= [:tables] (:children stmt))))

(deftest-stmt test-truncate-continents-restart-restrict
  ["TRUNCATE TABLE \"continents\" RESTART IDENTITY RESTRICT"]
  (truncate db [:continents]
            (restart-identity true)
            (restrict true))
  (is (= :truncate (:op stmt)))
  (is (= [(parse-table :continents)] (:tables stmt)))
  (is (= {:op :restrict} (:restrict stmt)))
  (is (= {:op :restart-identity} (:restart-identity stmt))))

(deftest-stmt test-truncate-continents-continue-cascade
  ["TRUNCATE TABLE \"continents\" CONTINUE IDENTITY CASCADE"]
  (truncate db [:continents]
            (continue-identity true)
            (cascade true))
  (is (= :truncate (:op stmt)))
  (is (= [(parse-table :continents)] (:tables stmt)))
  (is (= {:op :cascade} (:cascade stmt)))
  (is (= {:op :continue-identity} (:continue-identity stmt))))

(deftest-stmt test-truncate-continue-identity
  ["TRUNCATE TABLE \"continents\" CONTINUE IDENTITY"]
  (truncate db [:continents]
            (continue-identity true)))

(deftest-stmt test-truncate-continue-identity-false
  ["TRUNCATE TABLE \"continents\""]
  (truncate db [:continents]
            (continue-identity false)))

(deftest-stmt test-truncate-cascade-true
  ["TRUNCATE TABLE \"continents\" CASCADE"]
  (truncate db [:continents]
            (cascade true)))

(deftest-stmt test-truncate-cascade-false
  ["TRUNCATE TABLE \"continents\""]
  (truncate db [:continents]
            (cascade false)))

(deftest-stmt test-truncate-restart-identity
  ["TRUNCATE TABLE \"continents\" RESTART IDENTITY"]
  (truncate db [:continents]
            (restart-identity true)))

(deftest-stmt test-truncate-restart-identity-false
  ["TRUNCATE TABLE \"continents\""]
  (truncate db [:continents]
            (restart-identity false)))

(deftest-stmt test-truncate-restrict
  ["TRUNCATE TABLE \"continents\""]
  (truncate db [:continents]
            (restrict false)))

(deftest-stmt test-truncate-restrict-false
  ["TRUNCATE TABLE \"continents\""]
  (truncate db [:continents]
            (restrict false)))

;; UPDATE

(deftest-stmt test-update-drama-to-dramatic
  ["UPDATE \"films\" SET \"kind\" = ? WHERE (\"kind\" = ?)" "Dramatic" "Drama"]
  (update db :films {:kind "Dramatic"}
    (where '(= :kind "Drama")))
  (is (= :update (:op stmt)))
  (is (= (parse-table :films) (:table stmt)))
  (is (= (parse-condition '(= :kind "Drama")) (:where stmt)))
  (is (= (parse-map-expr {:kind "Dramatic"})
         (:row stmt))))

(deftest-stmt test-update-drama-to-dramatic-returning
  ["UPDATE \"films\" SET \"kind\" = ? WHERE (\"kind\" = ?) RETURNING *" "Dramatic" "Drama"]
  (update db :films {:kind "Dramatic"}
    (where '(= :kind "Drama"))
    (returning *))
  (is (= :update (:op stmt)))
  (is (= (parse-table :films) (:table stmt)))
  (is (= (parse-condition '(= :kind "Drama")) (:where stmt)))
  (is (= (parse-map-expr {:kind "Dramatic"})
         (:row stmt)))
  (is (= [(parse-expr *)] (:returning stmt))))

(deftest-stmt test-update-daily-return
  ["UPDATE \"quotes\" SET \"daily_return\" = \"u\".\"daily_return\" FROM (SELECT \"id\", \"lag\"(\"close\") over (partition by \"company_id\" order by \"date\" desc) AS \"daily_return\" FROM \"quotes\") AS \"u\" WHERE (\"quotes\".\"id\" = \"u\".\"id\")"]
  (update db :quotes '((= :daily-return :u.daily-return))
    (where '(= :quotes.id :u.id))
    (from (as (select db [:id (as '((lag :close) over (partition by :company-id order by :date desc)) :daily-return)]
                (from :quotes))
              :u))))

(deftest-stmt test-update-prices
  [(str "UPDATE \"prices\" SET \"daily_return\" = \"u\".\"daily_return\" "
        "FROM (SELECT \"id\", ((\"close\" / \"lag\"(\"close\") over (partition by \"quote_id\" order by \"date\" desc)) - 1) AS \"daily_return\" "
        "FROM \"prices\" WHERE (\"prices\".\"quote_id\" = 1)) AS \"u\" WHERE ((\"prices\".\"id\" = \"u\".\"id\") and (\"prices\".\"quote_id\" = 1))")]
  (let [quote {:id 1}]
    (update db :prices '((= :daily-return :u.daily-return))
      (from (as (select db [:id (as '(- (/ :close ((lag :close) over (partition by :quote-id order by :date desc))) 1) :daily-return)]
                  (from :prices)
                  (where `(= :prices.quote-id ~(:id quote))))
                :u))
      (where `(and (= :prices.id :u.id)
                   (= :prices.quote-id ~(:id quote)))))))

(deftest-stmt test-update-airports
  [(str "UPDATE \"airports\" SET \"country_id\" = \"u\".\"id\", \"gps_code\" = \"u\".\"gps_code\", \"wikipedia_url\" = \"u\".\"wikipedia\", \"location\" = \"u\".\"geom\" "
        "FROM (SELECT DISTINCT ON (\"a\".\"iata_code\") \"c\".\"id\", \"a\".\"name\", \"a\".\"gps_code\", \"a\".\"iata_code\", \"a\".\"wikipedia\", \"a\".\"geom\" "
        "FROM \"natural_earth\".\"airports\" \"a\" JOIN \"countries\" \"c\" ON (\"c\".\"geography\" && \"a\".\"geom\") "
        "LEFT JOIN \"airports\" ON (\"lower\"(\"airports\".\"iata_code\") = \"lower\"(\"a\".\"iata_code\")) "
        "WHERE ((\"a\".\"gps_code\" IS NOT NULL) and (\"a\".\"iata_code\" IS NOT NULL) and (\"airports\".\"iata_code\" IS NOT NULL))) AS \"u\" "
        "WHERE (\"airports\".\"iata_code\" = \"u\".\"iata_code\")")]
  (update db :airports
          '((= :country-id :u.id)
            (= :gps-code :u.gps-code)
            (= :wikipedia-url :u.wikipedia)
            (= :location :u.geom))
    (from (as (select db (distinct [:c.id :a.name :a.gps-code :a.iata-code :a.wikipedia :a.geom] :on [:a.iata-code])
                (from (as :natural-earth.airports :a))
                (join (as :countries :c) '(on (:&& :c.geography :a.geom)))
                (join :airports '(on (= (lower :airports.iata-code) (lower :a.iata-code))) :type :left)
                (where '(and (is-not-null :a.gps-code)
                             (is-not-null :a.iata-code)
                             (is-not-null :airports.iata-code))))
              :u))
    (where '(= :airports.iata-code :u.iata-code))))

(deftest-stmt test-update-countries
  [(str "UPDATE \"countries\" SET \"geom\" = \"u\".\"geom\" FROM (SELECT \"iso_a2\", \"iso_a3\", \"iso_n3\", \"geom\" FROM \"natural_earth\".\"countries\") AS \"u\" "
        "WHERE ((\"lower\"(\"countries\".\"iso_3166_1_alpha_2\") = \"lower\"(\"u\".\"iso_a2\")) or (\"lower\"(\"countries\".\"iso_3166_1_alpha_3\") = \"lower\"(\"u\".\"iso_a3\")))")]
  (update db :countries
          '((= :geom :u.geom))
    (from (as (select db [:iso-a2 :iso-a3 :iso-n3 :geom]
                (from :natural-earth.countries)) :u))
    (where '(or (= (lower :countries.iso-3166-1-alpha-2) (lower :u.iso-a2))
                (= (lower :countries.iso-3166-1-alpha-3) (lower :u.iso-a3))))))

(deftest-stmt test-update-with-fn-call
  ["UPDATE \"films\" SET \"name\" = \"lower\"(\"name\") WHERE (\"id\" = 1)"]
  (update db :films {:name '(lower :name)}
    (where `(= :id 1))))

;; QUOTING

(deftest test-db-specifiy-quoting
  (are [db expected]
    (is (= expected (sql (select db [:continents.id] (from :continents)))))
    (db/mysql) ["SELECT `continents`.`id` FROM `continents`"]
    (db/postgresql) ["SELECT \"continents\".\"id\" FROM \"continents\""]
    (db/oracle) ["SELECT continents.id FROM continents"]
    (db/vertica) ["SELECT \"continents\".\"id\" FROM \"continents\""]))

;; POSTGRESQL ARRAYS

(deftest-stmt test-array
  ["SELECT ARRAY[1, 2]"]
  (select db [[1 2]]))

(deftest-stmt test-array-concat
  ["SELECT (ARRAY[1, 2] || ARRAY[3, 4] || ARRAY[5, 6])"]
  (select db ['(|| [1 2] [3 4] [5 6])]))

(deftest-stmt test-select-array-contains
  ["SELECT (ARRAY[1, 2] @> ARRAY[3, 4])"]
  (select db [`(~(keyword "@>") [1 2] [3 4])]))

(deftest-stmt test-select-array-contained
  ["SELECT (ARRAY[1, 2] <@ ARRAY[3, 4])"]
  (select db [`(~(keyword "<@") [1 2] [3 4])]))

;; POSTGRESQL FULLTEXT

(deftest-stmt test-cast-as-document-1
  ["SELECT CAST((\"title\" || ? || \"author\" || ? || \"abstract\" || ? || \"body\") AS document) FROM \"messages\" WHERE (\"mid\" = 12)" " " " " " "]
  (select db ['(cast (:|| :title " " :author " " :abstract " " :body) :document)]
    (from :messages)
    (where '(= :mid 12))))

(deftest-stmt test-cast-as-document-2
  ["SELECT CAST((\"m\".\"title\" || ? || \"m\".\"author\" || ? || \"m\".\"abstract\" || ? || \"d\".\"body\") AS document) FROM \"messages\" \"m\", \"docs\" \"d\" WHERE ((\"mid\" = \"did\") and (\"mid\" = 12))" " " " " " "]
  (select db ['(cast (:|| :m.title " " :m.author " " :m.abstract " " :d.body) :document)]
    (from (as :messages :m) (as :docs :d))
    (where '(and (= :mid :did)
                 (= :mid 12)))))

(deftest-stmt test-basic-text-matching-1
  ["SELECT (CAST(? AS tsvector) @@ CAST(? AS tsquery))" "a fat cat sat on a mat and ate a fat rat" "rat & cat"]
  (select db [`(~(keyword "@@")
                (cast "a fat cat sat on a mat and ate a fat rat" :tsvector)
                (cast "rat & cat" :tsquery))]))

(deftest-stmt test-basic-text-matching-2
  ["SELECT (CAST(? AS tsquery) @@ CAST(? AS tsvector))" "fat & cow" "a fat cat sat on a mat and ate a fat rat"]
  (select db [`(~(keyword "@@")
                (cast "fat & cow" :tsquery)
                (cast "a fat cat sat on a mat and ate a fat rat" :tsvector))]))

(deftest-stmt test-basic-text-matching-3
  ["SELECT (\"to_tsvector\"(?) @@ \"to_tsquery\"(?))" "fat cats ate fat rats" "fat & rat"]
  (select db [`(~(keyword "@@")
                (to_tsvector "fat cats ate fat rats")
                (to_tsquery "fat & rat"))]))

(deftest-stmt test-basic-text-matching-4
  ["SELECT (CAST(? AS tsvector) @@ \"to_tsquery\"(?))" "fat cats ate fat rats" "fat & rat"]
  (select db [`(~(keyword "@@")
                (cast "fat cats ate fat rats" :tsvector)
                (to_tsquery "fat & rat"))]))

(deftest-stmt test-searching-a-table-1
  ["SELECT \"title\" FROM \"pgweb\" WHERE (\"to_tsvector\"(?, \"body\") @@ \"to_tsquery\"(?, ?))" "english" "english" "friend"]
  (select db [:title]
    (from :pgweb)
    (where `(~(keyword "@@")
             (to_tsvector "english" :body)
             (to_tsquery "english" "friend")))))

(deftest-stmt test-searching-a-table-2
  ["SELECT \"title\" FROM \"pgweb\" WHERE (\"to_tsvector\"(\"body\") @@ \"to_tsquery\"(?))" "friend"]
  (select db [:title]
    (from :pgweb)
    (where `(~(keyword "@@")
             (to_tsvector :body)
             (to_tsquery "friend")))))

(deftest-stmt test-searching-a-table-3
  ["SELECT \"title\" FROM \"pgweb\" WHERE (\"to_tsvector\"((\"title\" || ? || \"body\")) @@ \"to_tsquery\"(?)) ORDER BY \"last_mod_date\" DESC LIMIT 10" " " "create & table"]
  (select db [:title]
    (from :pgweb)
    (where `(~(keyword "@@")
             (to_tsvector (:|| :title " " :body))
             (to_tsquery "create & table")))
    (order-by (desc :last-mod-date))
    (limit 10)))

;; WITH QUERIES (COMMON TABLE EXPRESSIONS)

(deftest-stmt test-with-query
  [(str "WITH regional_sales AS ("
        "SELECT \"region\", \"sum\"(\"amount\") AS \"total_sales\" "
        "FROM \"orders\" GROUP BY \"region\"), "
        "top_regions AS ("
        "SELECT \"region\" "
        "FROM \"regional_sales\" "
        "WHERE (\"total_sales\" > (SELECT (\"sum\"(\"total_sales\") / 10) FROM \"regional_sales\"))) "
        "SELECT \"region\", \"product\", \"sum\"(\"quantity\") AS \"product_units\", \"sum\"(\"amount\") AS \"product_sales\" "
        "FROM \"orders\" "
        "WHERE \"region\" IN (SELECT \"region\" "
        "FROM \"top_regions\") "
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
      (group-by :region :product))))

(deftest-stmt test-with-modify-data
  [(str "WITH moved_rows AS ("
        "DELETE FROM \"products\" "
        "WHERE ((\"date\" >= ?) and (\"date\" < ?)) "
        "RETURNING *) "
        "INSERT INTO \"product_logs\" SELECT * FROM \"moved_rows\"")
   "2010-10-01" "2010-11-01"]
  (with db [:moved-rows
            (delete db :products
              (where '(and (>= :date "2010-10-01")
                           (< :date "2010-11-01")))
              (returning :*))]
    (insert db :product-logs []
      (select db [:*] (from :moved-rows)))))

(deftest-stmt test-with-counter-update
  [(str "WITH upsert AS ("
        "UPDATE \"counter_table\" SET counter = counter+1 "
        "WHERE (\"id\" = ?) RETURNING *) "
        "INSERT INTO \"counter_table\" (\"id\", \"counter\") "
        "SELECT ?, 1 "
        "WHERE (NOT EXISTS (SELECT * FROM \"upsert\"))")
   "counter-name" "counter-name"]
  (with db [:upsert (update db :counter_table '((= counter counter+1))
                      (where '(= :id "counter-name"))
                      (returning *))]
    (insert db :counter_table [:id :counter]
      (select db ["counter-name" 1])
      (where `(not-exists ~(select db [*] (from :upsert)))))))

(deftest-stmt test-with-delete
  ["WITH t AS (DELETE FROM \"foo\") DELETE FROM \"bar\""]
  (with db [:t (delete db :foo)]
    (delete db :bar)))

(deftest-stmt test-with-compose
  ["WITH a AS (SELECT * FROM \"b\") SELECT * FROM \"a\" WHERE (1 = 1)"]
  (compose (with db [:a (select db [:*] (from :b))]
             (select db [:*] (from :a)))
           (where '(= 1 1))))

;; ATTRIBUTES OF COMPOSITE TYPES

(deftest-stmt test-attr-composite-type
  ["SELECT (\"new_emp\"()).\"name\" AS \"x\""]
  (select db [(as '(.-name (new-emp)) :x)]))

(deftest-stmt test-nested-attr-composite-type
  ["SELECT ((\"new_emp\"()).\"name\").\"first\" AS \"x\""]
  (select db [(as '(.-first (.-name (new-emp))) :x)]))

(deftest-stmt test-select-as-alias
  ["SELECT (SELECT count(*) FROM \"continents\") AS \"continents\", (SELECT count(*) FROM \"countries\") AS \"countries\""]
  (select db [(as (select db ['(count :*)] (from :continents)) :continents)
              (as (select db ['(count :*)] (from :countries)) :countries)]))

(deftest test-refresh-materialized-view
  (is (= (sql (refresh-materialized-view db :order-summary))
         ["REFRESH MATERIALIZED VIEW \"order_summary\""]))
  (is (= (sql (refresh-materialized-view db :order-summary
                                         (concurrently true)))
         ["REFRESH MATERIALIZED VIEW CONCURRENTLY \"order_summary\""]))
  (is (= (sql (refresh-materialized-view db :order-summary
                                         (with-data true)))
         ["REFRESH MATERIALIZED VIEW \"order_summary\" WITH DATA"]))
  (is (= (sql (refresh-materialized-view db :order-summary
                                         (with-data false)))
         ["REFRESH MATERIALIZED VIEW \"order_summary\" WITH NO DATA"]))
  (is (= (sql (refresh-materialized-view db :order-summary
                                         (concurrently true)
                                         (with-data false)))
         ["REFRESH MATERIALIZED VIEW CONCURRENTLY \"order_summary\" WITH NO DATA"])))

(deftest test-drop-materialized-view
  (is (= (sql (drop-materialized-view db :order-summary))
         ["DROP MATERIALIZED VIEW \"order_summary\""]))
  (is (= (sql (drop-materialized-view db :order-summary
                (if-exists true)))
         ["DROP MATERIALIZED VIEW IF EXISTS \"order_summary\""]))
  (is (= (sql (drop-materialized-view db :order-summary
                (cascade true)))
         ["DROP MATERIALIZED VIEW \"order_summary\" CASCADE"]))
  (is (= (sql (drop-materialized-view db :order-summary
                (restrict true)))
         ["DROP MATERIALIZED VIEW \"order_summary\" RESTRICT"]))
  (is (= (sql (drop-materialized-view db :order-summary
                (if-exists true)
                (cascade true)))
         ["DROP MATERIALIZED VIEW IF EXISTS \"order_summary\" CASCADE"])))

(deftest-stmt test-case
  [(str "SELECT \"a\", CASE WHEN (\"a\" = 1) THEN ?"
        " WHEN (\"a\" = 2) THEN ? "
        "ELSE ? END FROM \"test\"")
   "one" "two" "other"]
  (select db [:a '(case (= :a 1) "one"
                        (= :a 2) "two"
                        "other")]
    (from :test)))

(deftest-stmt test-case-as-alias
  [(str "SELECT \"a\", CASE WHEN (\"a\" = 1) THEN ?"
        " WHEN (\"a\" = 2) THEN ? "
        "ELSE ? END AS \"c\" FROM \"test\"")
   "one" "two" "other"]
  (select db [:a (as '(case (= :a 1) "one"
                            (= :a 2) "two"
                            "other") :c)]
    (from :test)))

(deftest-stmt test-full-outer-join
  [(str "SELECT \"a\" FROM \"test1\""
        " FULL OUTER JOIN \"test2\" ON (\"test1\".\"b\" = \"test2\".\"b\")")]
  (select db [:a]
    (from :test1)
    (join :test2
          '(on (= :test1.b :test2.b))
          :type :full :outer true)))

;; Window functions: http://www.postgresql.org/docs/9.4/static/tutorial-window.html

(deftest test-window-compare-salaries
  (is (= (sql (select db [:depname :empno :salary '(over (avg :salary) (partition-by :depname))]
                (from :empsalary)))
         ["SELECT \"depname\", \"empno\", \"salary\", \"avg\"(\"salary\") OVER (PARTITION BY \"depname\") FROM \"empsalary\""])))

(deftest test-window-compare-salaries-by-year
  (is (= (sql (select db [:year :depname :empno :salary '(over (avg :salary) (partition-by [:year :depname]))]
                (from :empsalary)))
         ["SELECT \"year\", \"depname\", \"empno\", \"salary\", \"avg\"(\"salary\") OVER (PARTITION BY \"year\", \"depname\") FROM \"empsalary\""])))

(deftest test-window-rank-over-order-by
  (is (= (sql (select db [:depname :empno :salary '(over (rank) (partition-by :depname (order-by (desc :salary))))]
                (from :empsalary)))
         ["SELECT \"depname\", \"empno\", \"salary\", \"rank\"() OVER (PARTITION BY \"depname\" ORDER BY \"salary\" DESC) FROM \"empsalary\""])))

(deftest test-window-rank-over-multiple-cols-order-by
  (is (= (sql (select db [:year :depname :empno :salary '(over (rank) (partition-by [:year :depname] (order-by (desc :salary))))]
                (from :empsalary)))
         ["SELECT \"year\", \"depname\", \"empno\", \"salary\", \"rank\"() OVER (PARTITION BY \"year\", \"depname\" ORDER BY \"salary\" DESC) FROM \"empsalary\""])))

(deftest test-window-over-empty
  (is (= (sql (select db [:salary '(over (sum :salary))]
                (from :empsalary)))
         ["SELECT \"salary\", \"sum\"(\"salary\") OVER () FROM \"empsalary\""])))

(deftest test-window-sum-over-order-by
  (is (= (sql (select db [:salary '(over (sum :salary) (order-by :salary))]
                (from :empsalary)))
         ["SELECT \"salary\", \"sum\"(\"salary\") OVER (ORDER BY \"salary\") FROM \"empsalary\""])))

(deftest test-window-rank-over-partition-by
  (is (= (sql (select db [:depname :empno :salary :enroll-date]
                (from (as (select db [:depname :empno :salary :enroll-date
                                      (as '(over (rank) (partition-by :depname (order-by (desc :salary) :empno))) :pos)]
                            (from :empsalary))
                          :ss))
                (where '(< pos 3))))
         [(str "SELECT \"depname\", \"empno\", \"salary\", \"enroll_date\" "
               "FROM (SELECT \"depname\", \"empno\", \"salary\", \"enroll_date\", "
               "\"rank\"() OVER (PARTITION BY \"depname\" ORDER BY \"salary\" DESC, \"empno\") AS \"pos\" "
               "FROM \"empsalary\") AS \"ss\" WHERE (pos < 3)")])))

(deftest test-window-alias
  (is (= (sql (select db ['(over (sum :salary) :w)
                          '(over (avg :salary) :w)]
                (from :empsalary)
                (window (as '(partition-by
                              :depname (order-by (desc salary))) :w))))
         [(str "SELECT \"sum\"(\"salary\") OVER (\"w\"), "
               "\"avg\"(\"salary\") OVER (\"w\") "
               "FROM \"empsalary\" "
               "WINDOW \"w\" AS (PARTITION BY \"depname\" ORDER BY salary DESC)")])))

(deftest test-window-alias-order-by
  (is (= (sql (select db [(as '(over (sum :salary) :w) :sum)]
                (from :empsalary)
                (order-by :sum)
                (window (as '(partition-by
                              :depname (order-by (desc salary))) :w))))
         [(str "SELECT \"sum\"(\"salary\") OVER (\"w\") AS \"sum\" "
               "FROM \"empsalary\" "
               "WINDOW \"w\" AS (PARTITION BY \"depname\" ORDER BY salary DESC) "
               "ORDER BY \"sum\"")])))

(deftest test-not-expr
  (is (= (sql (select db [:*]
                (where `(not (= :id 1)))))
         ["SELECT * WHERE (NOT (\"id\" = 1))"])))
