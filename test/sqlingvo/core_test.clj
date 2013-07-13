(ns sqlingvo.core-test
  (:import java.sql.Date)
  (:refer-clojure :exclude [distinct group-by])
  (:require [clojure.algo.monads :refer :all])
  (:use clojure.test
        sqlingvo.compiler
        sqlingvo.util
        sqlingvo.core))

(defmacro deftest-stmt [name sql stmt & body]
  `(deftest ~name
     (let [~'stmt (ast ~stmt)]
       (is (= ~sql (sql ~stmt)))
       ~@body)))

;; COMPOSE

(deftest-stmt test-compose
  ["SELECT id, name FROM continents WHERE (id = 1) ORDER BY name"]
  (compose (select [:id :name] (from :continents))
           (where '(= :id 1))
           (order-by :name)))

(deftest-stmt test-compose-where-clause-using-and
  ["SELECT color, num_sides FROM shapes WHERE ((num_sides = 3) and (color = ?))" "green"]
  (let [triangles (compose (select [:color :num_sides] (from :shapes))
                           (where '(= :num_sides 3)))]
    (compose triangles (where '(= :color "green") :and))))

;; AS

(deftest test-as
  (are [args expected]
    (is (= expected (apply as args)))
    [:id :other]
    {:op :column :schema nil :table nil :name :id :as :other}
    [:continents [:id :name]]
    [{:op :column :schema nil :table :continents :name :id :as :continents-id}
     {:op :column :schema nil :table :continents :name :name :as :continents-name}]
    [:public.continents [:id :name]]
    [{:op :column :schema :public :table :continents :name :id :as :public-continents-id}
     {:op :column :schema :public :table :continents :name :name :as :public-continents-name}]
    ['(count *) :count]
    {:as :count :op :fn :name :count :args [{:op :constant :form '*}]}))

;; CAST

(deftest-stmt test-cast-int-as-text
  ["SELECT CAST(1 AS text)"]
  (select [`(cast 1 :text)]))

(deftest-stmt test-cast-text-as-int
  ["SELECT CAST(? AS int)" "1"]
  (select [`(cast "1" :int)]))

;; CREATE TABLE

(deftest-stmt test-create-table-tmp-if-not-exists-inherits
  ["CREATE TEMPORARY TABLE IF NOT EXISTS import () INHERITS (quotes)"]
  (create-table :import
    (temporary true)
    (if-not-exists true)
    (inherits :quotes))
  (is (= :create-table (:op stmt)))
  (is (= {:op :temporary} (:temporary stmt)))
  (is (= {:op :if-not-exists} (:if-not-exists stmt)))
  (is (= [(parse-table :quotes)] (:inherits stmt))))

(deftest-stmt test-create-table-tmp-if-not-exists-false
  ["CREATE TEMPORARY TABLE import () INHERITS (quotes)"]
  (create-table :import
    (temporary true)
    (if-not-exists false)
    (inherits :quotes)))

(deftest-stmt test-create-table-like-including-defaults
  ["CREATE TABLE tmp_films (LIKE films INCLUDING DEFAULTS)"]
  (create-table :tmp-films
    (like :films :including [:defaults]))
  (is (= :create-table (:op stmt)))
  (let [like (:like stmt)]
    (is (= :like (:op like)))
    (is (= (parse-table :films) (:table like)))
    (is (= [:defaults] (:including like)))))

(deftest-stmt test-create-table-like-excluding-indexes
  ["CREATE TABLE tmp_films (LIKE films EXCLUDING INDEXES)"]
  (create-table :tmp-films
    (like :films :excluding [:indexes]))
  (is (= :create-table (:op stmt)))
  (let [like (:like stmt)]
    (is (= :like (:op like)))
    (is (= (parse-table :films) (:table like)))
    (is (= [:indexes] (:excluding like)))))

(deftest-stmt test-create-table-films
  [(str "CREATE TABLE films ("
        "code CHAR(5) PRIMARY KEY, "
        "title VARCHAR(40) NOT NULL, "
        "did INTEGER NOT NULL, "
        "date_prod DATE, "
        "kind VARCHAR(10), "
        "len INTERVAL, "
        "created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "
        "updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now())")]
  (create-table :films
    (column :code :char :length 5 :primary-key? true)
    (column :title :varchar :length 40 :not-null? true)
    (column :did :integer :not-null? true)
    (column :date-prod :date)
    (column :kind :varchar :length 10)
    (column :len :interval)
    (column :created-at :timestamp-with-time-zone :not-null? true :default "now()")
    (column :updated-at :timestamp-with-time-zone :not-null? true :default "now()")))

;; COPY

(deftest-stmt test-copy-stdin
  ["COPY country FROM STDIN"]
  (copy :country []
    (from :stdin))
  (is (= :copy (:op stmt)))
  (is (= [:stdin] (:from stmt))))

(deftest-stmt test-copy-country
  ["COPY country FROM ?" "/usr1/proj/bray/sql/country_data"]
  (copy :country []
    (from "/usr1/proj/bray/sql/country_data"))
  (is (= :copy (:op stmt)))
  (is (= ["/usr1/proj/bray/sql/country_data"] (:from stmt))))

(deftest-stmt test-copy-country-columns
  ["COPY country (id, name) FROM ?" "/usr1/proj/bray/sql/country_data"]
  (copy :country [:id :name]
    (from "/usr1/proj/bray/sql/country_data"))
  (is (= :copy (:op stmt)))
  (is (= ["/usr1/proj/bray/sql/country_data"] (:from stmt)))
  (is (= (map parse-column [:id :name]) (:columns stmt))))

;; DELETE

(deftest-stmt test-delete-films
  ["DELETE FROM films"]
  (delete :films)
  (is (= :delete (:op stmt)))
  (is (= (parse-table :films) (:table stmt))))

(deftest-stmt test-delete-all-films-but-musicals
  ["DELETE FROM films WHERE (kind <> ?)" "Musical"]
  (delete :films
    (where '(<> :kind "Musical")))
  (is (= :delete (:op stmt)))
  (is (= (parse-table :films) (:table stmt)))
  (is (= (parse-condition '(<> :kind "Musical")) (:where stmt))))

(deftest-stmt test-delete-completed-tasks-returning-all
  ["DELETE FROM tasks WHERE (status = ?) RETURNING *" "DONE"]
  (delete :tasks
    (where '(= status "DONE"))
    (returning *))
  (is (= :delete (:op stmt)))
  (is (= (parse-table :tasks) (:table stmt)))
  (is (= (parse-condition '(= status "DONE")) (:where stmt)))
  (is (= [(parse-expr *)] (:returning stmt))))

(deftest-stmt test-delete-films-by-producer-name
  ["DELETE FROM films WHERE (producer_id in (SELECT id FROM producers WHERE (name = ?)))" "foo"]
  (delete :films
    (where `(in :producer-id
                ~(select [:id]
                   (from :producers)
                   (where '(= name "foo"))))))
  (is (= :delete (:op stmt)))
  (is (= (parse-table :films) (:table stmt)))
  (is (= (parse-condition `(in :producer-id
                               ~(select [:id]
                                  (from :producers)
                                  (where '(= name "foo")))))
         (:where stmt))))

(deftest-stmt test-delete-quotes
  [(str "DELETE FROM quotes WHERE ((company_id = 1) and (date > (SELECT min(date) FROM import)) and "
        "(date > (SELECT max(date) FROM import)))")]
  (delete :quotes
    (where `(and (= :company-id 1)
                 (> :date ~(select ['(min :date)] (from :import)))
                 (> :date ~(select ['(max :date)] (from :import))))))
  (is (= :delete (:op stmt)))
  (is (= (parse-table :quotes) (:table stmt)))
  (is (= (parse-condition `(and (= :company-id 1)
                                (> :date ~(select ['(min :date)] (from :import)))
                                (> :date ~(select ['(max :date)] (from :import)))))
         (:where stmt))))

;; DROP TABLE

(deftest-stmt test-drop-continents
  ["DROP TABLE continents"]
  (drop-table [:continents])
  (is (= :drop-table (:op stmt)))
  (is (= [(parse-table :continents)] (:tables stmt))))

(deftest-stmt test-drop-continents-and-countries
  ["DROP TABLE continents, countries"]
  (drop-table [:continents :countries])
  (is (= :drop-table (:op stmt)))
  (is (= (map parse-table [:continents :countries]) (:tables stmt))))

(deftest-stmt test-drop-continents-countries-if-exists-restrict
  ["DROP TABLE IF EXISTS continents, countries RESTRICT"]
  (drop-table [:continents :countries]
    (if-exists true)
    (restrict true))
  (is (= :drop-table (:op stmt)))
  (is (= {:op :if-exists} (:if-exists stmt)))
  (is (= (map parse-table [:continents :countries]) (:tables stmt)))
  (is (= {:op :restrict} (:restrict stmt))))

(deftest-stmt test-drop-continents-if-exists
  ["DROP TABLE IF EXISTS continents"]
  (drop-table [:continents]
    (if-exists true)))

(deftest-stmt test-drop-continents-if-exists-false
  ["DROP TABLE continents"]
  (drop-table [:continents]
    (if-exists false)))

;; INSERT

(deftest-stmt test-insert-default-values
  ["INSERT INTO films DEFAULT VALUES"]
  (insert :films []
    (values :default))
  (is (= :insert (:op stmt)))
  (is (= [] (:columns stmt)))
  (is (= true (:default-values stmt)))
  (is (= (parse-table :films) (:table stmt))))

(deftest-stmt test-insert-single-row-as-map
  ["INSERT INTO films (did, date_prod, kind, title, code) VALUES (?, ?, ?, ?, ?)"
   106 "1961-06-16" "Drama" "Yojimbo" "T_601"]
  (insert :films []
    (values {:code "T_601" :title "Yojimbo" :did 106 :date-prod "1961-06-16" :kind "Drama"}))
  (is (= :insert (:op stmt)))
  (is (= [] (:columns stmt)))
  (is (= [{:code "T_601" :title "Yojimbo" :did 106 :date-prod "1961-06-16" :kind "Drama"}]
         (:values stmt)))
  (is (= (parse-table :films) (:table stmt))))

(deftest-stmt test-insert-single-row-as-seq
  ["INSERT INTO films (did, date_prod, kind, title, code) VALUES (?, ?, ?, ?, ?)"
   106 "1961-06-16" "Drama" "Yojimbo" "T_601"]
  (insert :films []
    (values [{:code "T_601" :title "Yojimbo" :did 106 :date-prod "1961-06-16" :kind "Drama"}]))
  (is (= :insert (:op stmt)))
  (is (= [] (:columns stmt)))
  (is (= [{:code "T_601" :title "Yojimbo" :did 106 :date-prod "1961-06-16" :kind "Drama"}]
         (:values stmt)))
  (is (= (parse-table :films) (:table stmt))))

(deftest-stmt test-insert-multi-row
  ["INSERT INTO films (did, date_prod, kind, title, code) VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)"
   110 "1985-02-10" "Comedy" "Tampopo" "B6717" 140 "1985-02-10" "Comedy" "The Dinner Game" "HG120"]
  (insert :films []
    (values [{:code "B6717" :title "Tampopo" :did 110 :date-prod "1985-02-10" :kind "Comedy"},
             {:code "HG120" :title "The Dinner Game" :did 140 :date-prod "1985-02-10":kind "Comedy"}]))
  (is (= :insert (:op stmt)))
  (is (= [] (:columns stmt)))
  (is (= [{:code "B6717" :title "Tampopo" :did 110 :date-prod "1985-02-10" :kind "Comedy"},
          {:code "HG120" :title "The Dinner Game" :did 140 :date-prod "1985-02-10":kind "Comedy"}]
         (:values stmt)))
  (is (= (parse-table :films) (:table stmt))))

(deftest-stmt test-insert-returning
  ["INSERT INTO distributors (did, dname) VALUES (?, ?) RETURNING *" 106 "XYZ Widgets"]
  (insert :distributors []
    (values [{:did 106 :dname "XYZ Widgets"}])
    (returning *))
  (is (= :insert (:op stmt)))
  (is (= [] (:columns stmt)))
  (is (= (parse-table :distributors) (:table stmt)))
  (is (= [(parse-expr *)] (:returning stmt))))

(deftest-stmt test-insert-subselect
  ["INSERT INTO films SELECT * FROM tmp_films WHERE (date_prod < ?)" "2004-05-07"]
  (insert :films []
    (select [*]
      (from :tmp-films)
      (where '(< :date-prod "2004-05-07"))))
  (is (= :insert (:op stmt)))
  (is (= [] (:columns stmt)))
  (is (= (parse-table :films) (:table stmt)))
  (is (= (ast (select [*]
                (from :tmp-films)
                (where '(< :date-prod "2004-05-07"))))
         (:select stmt))))

(deftest-stmt test-insert-airports
  [(str "INSERT INTO airports (country_id, name, gps_code, iata_code, wikipedia_url, location) "
        "SELECT DISTINCT ON (a.iata_code) c.id, a.name, a.gps_code, a.iata_code, a.wikipedia, a.geom "
        "FROM natural_earth.airports AS a JOIN countries AS c ON (c.geography && a.geom) "
        "LEFT JOIN airports ON (airports.iata_code = a.iata_code) "
        "WHERE ((a.gps_code IS NOT NULL) and (a.iata_code IS NOT NULL) and (airports.iata_code IS NULL))")]
  (insert :airports [:country-id, :name :gps-code :iata-code :wikipedia-url :location]
    (select (distinct [:c.id :a.name :a.gps-code :a.iata-code :a.wikipedia :a.geom] :on [:a.iata-code])
      (from (as :natural-earth.airports :a))
      (join (as :countries :c) '(on (:&& :c.geography :a.geom)))
      (join :airports '(on (= :airports.iata-code :a.iata-code)) :type :left)
      (where '(and (is-not-null :a.gps-code)
                   (is-not-null :a.iata-code)
                   (is-null :airports.iata-code))))))

(deftest-stmt test-insert-only-columns
  ["INSERT INTO x (a, b) VALUES (?, ?)" 1 2]
  (insert :x [:a :b] (values [{:a 1 :b 2 :c 3}])))

;; SELECT

(deftest-stmt test-select-1
  ["SELECT 1"]
  (select [1])
  (is (= :select (:op stmt)))
  (is (= [(parse-expr 1)] (:exprs stmt))))

(deftest-stmt test-select-1-as
  ["SELECT 1 AS n"]
  (select [(as 1 :n)])
  (is (= :select (:op stmt)))
  (is (= [(parse-expr (as 1 :n))] (:exprs stmt))))

(deftest-stmt test-select-x-as
  ["SELECT ? AS x" "x"]
  (select [(as "x" :x)])
  (is (= :select (:op stmt)))
  (is (= [(parse-expr (as "x" :x))] (:exprs stmt))))

(deftest-stmt test-select-1-2-3
  ["SELECT 1, 2, 3"]
  (select [1 2 3])
  (is (= :select (:op stmt)))
  (is (= (map parse-expr [1 2 3]) (:exprs stmt))))

(deftest-stmt test-select-1-2-3-as
  ["SELECT 1 AS a, 2 AS b, 3 AS c"]
  (select [(as 1 :a) (as 2 :b) (as 3 :c)])
  (is (= :select (:op stmt)))
  (is (= (map parse-expr [(as 1 :a) (as 2 :b) (as 3 :c)])
         (:exprs stmt))))

(deftest-stmt test-select-count-distinct
  ["SELECT count(DISTINCT user_id) FROM tweets"]
  (select ['(count distinct :user-id)]
    (from :tweets)))

(deftest-stmt test-select-select-1
  ["SELECT (SELECT 1)"]
  (select [(select [1])])
  (is (= :select (:op stmt)))
  (is (= [(ast (select [1]))] (:exprs stmt))))

(deftest-stmt test-select-1-in-1-2-3
  ["SELECT 1 WHERE (1 in (1, 2, 3))"]
  (select [1]
    (where '(in 1 (1 2 3))))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr 1)] (:exprs stmt)))
  (is (= (parse-condition '(in 1 (1 2 3))) (:where stmt))))

(deftest-stmt test-select-select-1-select-x
  ["SELECT (SELECT 1), (SELECT ?)" "x"]
  (select [(select [1]) (select ["x"])])
  (is (= :select (:op stmt)))
  (is (= [(ast (select [1])) (ast (select ["x"]))] (:exprs stmt))))

(deftest-stmt test-select-string
  ["SELECT * FROM continents WHERE (name = ?)" "Europe"]
  (select [*]
    (from :continents)
    (where '(= :name "Europe")))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= (parse-condition '(= :name "Europe")) (:where stmt))))

(deftest-stmt test-select-where-single-arg-and
  ["SELECT 1 WHERE (1 = 1)"]
  (select [1]
    (where '(and (= 1 1))))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr 1)] (:exprs stmt)))
  (is (= (parse-condition '(and (= 1 1))) (:where stmt))))

(deftest-stmt test-select-less-2-arity
  ["SELECT 1 WHERE (1 < 2)"]
  (select [1]
    (where '(< 1 2)))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr 1)] (:exprs stmt)))
  (is (= (parse-condition '(< 1 2)) (:where stmt))))

(deftest-stmt test-select-less-3-arity
  ["SELECT 1 WHERE (1 < 2) AND (2 < 3)"]
  (select [1]
    (where '(< 1 2 3)))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr 1)] (:exprs stmt)))
  (is (= (parse-condition '(< 1 2 3)) (:where stmt))))

(deftest-stmt test-select-continents
  ["SELECT * FROM continents"]
  (select [*]
    (from :continents))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-from :continents)] (:from stmt))))

(deftest-stmt test-select-continents-qualified
  ["SELECT continents.* FROM continents"]
  (select [:continents.*]
    (from :continents))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr :continents.*)] (:exprs stmt)))
  (is (= [(parse-from :continents)] (:from stmt))))

(deftest-stmt test-select-films
  ["SELECT * FROM films"]
  (select [*] (from :films))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-from :films)] (:from stmt))))

(deftest-stmt test-select-comedy-films
  ["SELECT * FROM films WHERE (kind = ?)" "Comedy"]
  (select [*]
    (from :films)
    (where '(= :kind "Comedy")))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= (parse-condition '(= :kind "Comedy")) (:where stmt)))
  (is (= [(parse-from :films)] (:from stmt))))

(deftest-stmt test-select-is-null
  ["SELECT 1 WHERE (NULL IS NULL)"]
  (select [1]
    (where '(is-null nil)))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr 1)] (:exprs stmt)))
  (is (= (parse-condition '(is-null nil)) (:where stmt))))

(deftest-stmt test-select-is-not-null
  ["SELECT 1 WHERE (NULL IS NOT NULL)"]
  (select [1]
    (where '(is-not-null nil)))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr 1)] (:exprs stmt)))
  (is (= (parse-condition '(is-not-null nil)) (:where stmt))))

(deftest-stmt test-select-backquote-date
  ["SELECT * FROM countries WHERE (created_at > ?)" (Date. 0)]
  (select [*]
    (from :countries)
    (where `(> :created-at ~(Date. 0))))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= (parse-condition `(> :created-at ~(Date. 0))) (:where stmt))))

(deftest-stmt test-select-star-number-string
  ["SELECT *, 1, ?" "x"]
  (select [* 1 "x"])
  (is (= :select (:op stmt)))
  (is (= (map parse-expr [* 1 "x"]) (:exprs stmt))))

(deftest-stmt test-select-column
  ["SELECT created_at FROM continents"]
  (select [:created-at]
    (from :continents))
  (is (= :select (:op stmt)))
  (is (= [(parse-table :continents)] (:from stmt)))
  (is (= [(parse-expr :created-at)] (:exprs stmt))))

(deftest-stmt test-select-columns
  ["SELECT name, created_at FROM continents"]
  (select [:name :created-at]
    (from :continents))
  (is (= :select (:op stmt)))
  (is (= [(parse-table :continents)] (:from stmt)))
  (is (= (map parse-expr [:name :created-at]) (:exprs stmt))))

(deftest-stmt test-select-column-alias
  ["SELECT created_at AS c FROM continents"]
  (select [(as :created-at :c)]
    (from :continents))
  (is (= :select (:op stmt)))
  (is (= [(parse-table :continents)] (:from stmt)))
  (is (= [(parse-expr (as :created-at :c))] (:exprs stmt))))

(deftest-stmt test-select-multiple-fns
  ["SELECT greatest(1, 2), lower(?)" "X"]
  (select ['(greatest 1 2) '(lower "X")])
  (is (= :select (:op stmt)))
  (is (= (map parse-expr ['(greatest 1 2) '(lower "X")]) (:exprs stmt))))

(deftest-stmt test-select-nested-fns
  ["SELECT (1 + greatest(2, 3))"]
  (select ['(+ 1 (greatest 2 3))])
  (is (= :select (:op stmt)))
  (is (= [(parse-expr '(+ 1 (greatest 2 3)))] (:exprs stmt))))

(deftest-stmt test-select-fn-alias
  ["SELECT max(created_at) AS m FROM continents"]
  (select [(as '(max :created-at) :m)]
    (from :continents))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr (as '(max :created-at) :m))] (:exprs stmt)))
  (is (= [(parse-table :continents)] (:from stmt))))

(deftest-stmt test-select-limit
  ["SELECT * FROM continents LIMIT 10"]
  (select [*]
    (from :continents)
    (limit 10))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-table :continents)] (:from stmt)))
  (is (= {:op :limit :count 10} (:limit stmt))))

(deftest-stmt test-select-offset
  ["SELECT * FROM continents OFFSET 15"]
  (select [*]
    (from :continents)
    (offset 15))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-table :continents)] (:from stmt)))
  (is (= {:op :offset :start 15} (:offset stmt))))

(deftest-stmt test-select-limit-offset
  ["SELECT * FROM continents LIMIT 10 OFFSET 20"]
  (select [*]
    (from :continents)
    (limit 10)
    (offset 20))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-table :continents)] (:from stmt)))
  (is (= {:op :limit :count 10} (:limit stmt)))
  (is (= {:op :offset :start 20} (:offset stmt))))

(deftest-stmt test-select-column-max
  ["SELECT max(created_at) FROM continents"]
  (select ['(max :created-at)]
    (from :continents))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr '(max :created-at))] (:exprs stmt)))
  (is (= [(parse-table :continents)] (:from stmt))))

(deftest-stmt test-select-distinct-subquery-alias
  ["SELECT DISTINCT x.a, x.b FROM (SELECT 1 AS a, 2 AS b) AS x"]
  (select (distinct [:x.a :x.b])
    (from (as (select [(as 1 :a) (as 2 :b)]) :x)))
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
  ["SELECT DISTINCT ON (x.a, x.b) x.a, x.b FROM (SELECT 1 AS a, 2 AS b) AS x"]
  (select (distinct [:x.a :x.b] :on [:x.a :x.b])
    (from (as (select [(as 1 :a) (as 2 :b)]) :x)))
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
  ["SELECT DISTINCT ON (location) location, time, report FROM weather_reports ORDER BY location, time DESC"]
  (select (distinct [:location :time :report] :on [:location])
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
  ["SELECT * FROM continents ORDER BY created_at ASC"]
  (select [*]
    (from :continents)
    (order-by (asc :created-at)))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-from :continents)] (:from stmt)))
  (is (= [(parse-expr (asc :created-at))] (:order-by stmt))))

(deftest-stmt test-select-order-by-asc-expr
  ["SELECT * FROM weather.datasets ORDER BY abs((ST_ScaleX(rast) * ST_ScaleY(rast))) DESC"]
  (select [*]
    (from :weather.datasets)
    (order-by (desc '(abs (* (ST_ScaleX :rast) (ST_ScaleY :rast)))))))

(deftest-stmt test-select-order-by-desc
  ["SELECT * FROM continents ORDER BY created_at DESC"]
  (select [*]
    (from :continents)
    (order-by (desc :created-at)))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-from :continents)] (:from stmt)))
  (is (= [(parse-expr (desc :created-at))] (:order-by stmt))))

(deftest-stmt test-select-order-by-nulls-first
  ["SELECT * FROM continents ORDER BY created_at NULLS FIRST"]
  (select [*]
    (from :continents)
    (order-by (nulls :created-at :first)))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-from :continents)] (:from stmt)))
  (is (= [(parse-expr (nulls :created-at :first))] (:order-by stmt))))

(deftest-stmt test-select-order-by-nulls-last
  ["SELECT * FROM continents ORDER BY created_at NULLS LAST"]
  (select [*]
    (from :continents)
    (order-by (nulls :created-at :last)))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-from :continents)] (:from stmt)))
  (is (= [(parse-expr (nulls :created-at :last))] (:order-by stmt))))

(deftest-stmt test-select-order-by-if-true
  ["SELECT * FROM continents ORDER BY name"]
  (let [opts {:order-by :name}]
    (select [*]
      (from :continents)
      (if (:order-by opts)
        (order-by (:order-by opts)))))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-from :continents)] (:from stmt)))
  (is (= [(parse-expr :name)] (:order-by stmt))))

(deftest-stmt test-select-order-by-if-false
  ["SELECT * FROM continents"]
  (let [opts {}]
    (select [*]
      (from :continents)
      (if (:order-by opts)
        (order-by (:order-by opts)))))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-from :continents)] (:from stmt)))
  (is (nil? (:order-by stmt))))

(deftest-stmt test-select-order-by-nil
  ["SELECT * FROM continents"]
  (select [*]
    (from :continents)
    (order-by nil))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-from :continents)] (:from stmt)))
  (is (nil? (:order-by stmt))))

(deftest-stmt test-select-1-where-1-is-1
  ["SELECT 1 WHERE (1 = 1)"]
  (select [1]
    (where '(= 1 1)))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr 1)] (:exprs stmt)))
  (is (= (parse-condition '(= 1 1)) (:where stmt))))

(deftest-stmt test-select-1-where-1-is-2-is-3
  ["SELECT 1 WHERE (1 = 2) AND (2 = 3)"]
  (select [1]
    (where '(= 1 2 3)))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr 1)] (:exprs stmt)))
  (is (= (parse-condition '(= 1 2 3)) (:where stmt))))

(deftest-stmt test-select-subquery-alias
  ["SELECT * FROM (SELECT 1, 2, 3) AS x"]
  (select [*]
    (from (as (select [1 2 3]) :x)))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (let [from (first (:from stmt))]
    (is (= :select (:op from)))
    (is (= :x (:as from)))
    (is (= (map parse-expr [1 2 3]) (:exprs from)))))

(deftest-stmt test-select-subqueries-alias
  ["SELECT * FROM (SELECT 1) AS x, (SELECT 2) AS y"]
  (select [*]
    (from (as (select [1]) :x)
          (as (select [2]) :y)))
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
  ["SELECT id, lag(close) over (partition by company_id order by date desc) FROM quotes"]
  (select [:id '((lag :close) over (partition by :company-id order by :date desc))]
    (from :quotes))
  (is (= :select (:op stmt)))
  (is (= (map parse-expr [:id '((lag :close) over (partition by :company-id order by :date desc))])
         (:exprs stmt)))
  (is (= [(parse-from :quotes)] (:from stmt))))

(deftest-stmt test-select-total-return
  ["SELECT id, (close / (lag(close) over (partition by company_id order by date desc) - 1)) FROM quotes"]
  (select [:id '(/ close (- ((lag :close) over (partition by :company-id order by :date desc)) 1))]
    (from :quotes))
  (is (= :select (:op stmt)))
  (is (= (map parse-expr [:id '(/ close (- ((lag :close) over (partition by :company-id order by :date desc)) 1))])
         (:exprs stmt)))
  (is (= [(parse-from :quotes)] (:from stmt))))

(deftest-stmt test-select-total-return-alias
  ["SELECT id, (close / (lag(close) over (partition by company_id order by date desc) - 1)) AS daily_return FROM quotes"]
  (select [:id (as '(/ close (- ((lag :close) over (partition by :company-id order by :date desc)) 1)) :daily-return)]
    (from :quotes))
  (is (= :select (:op stmt)))
  (is (= (map parse-expr [:id (as '(/ close (- ((lag :close) over (partition by :company-id order by :date desc)) 1)) :daily-return)])
         (:exprs stmt)))
  (is (= [(parse-from :quotes)] (:from stmt))))

(deftest-stmt test-select-group-by-a-order-by-1
  ["SELECT a, max(b) FROM table_1 GROUP BY a ORDER BY 1"]
  (select [:a '(max :b)]
    (from :table-1)
    (group-by :a)
    (order-by 1))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr :a) (parse-expr '(max :b))] (:exprs stmt)))
  (is (= [(parse-from :table-1)] (:from stmt)))
  (is (= [(parse-expr 1)] (:order-by stmt))))

(deftest-stmt test-select-order-by-query-select
  ["SELECT a, b FROM table_1 ORDER BY (a + b), c"]
  (select [:a :b]
    (from :table-1)
    (order-by '(+ :a :b) :c))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr :a) (parse-expr :b)] (:exprs stmt)))
  (is (= [(parse-from :table-1)] (:from stmt)))
  (is (= [(parse-expr '(+ :a :b)) (parse-expr :c)] (:order-by stmt))))

(deftest-stmt test-select-order-by-sum
  ["SELECT (a + b) AS sum, c FROM table_1 ORDER BY sum"]
  (select [(as '(+ :a :b) :sum) :c]
    (from :table-1)
    (order-by :sum))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr (as '(+ :a :b) :sum)) (parse-expr :c)] (:exprs stmt)))
  (is (= [(parse-from :table-1)] (:from stmt)))
  (is (= [(parse-expr :sum)] (:order-by stmt))))

(deftest-stmt test-select-setval
  ["SELECT setval(continent_id_seq, (SELECT max(id) FROM continents))"]
  (select [`(setval :continent-id-seq ~(select [`(max :id)] (from :continents)))])
  (is (= :select (:op stmt)))
  (is (= (map parse-expr [`(setval :continent-id-seq ~(select [`(max :id)] (from :continents)))])
         (:exprs stmt))))

(deftest-stmt test-select-regex-match
  ["SELECT id, symbol, quote FROM quotes WHERE (? ~ concat(?, symbol, ?))" "$AAPL" "(^|\\s)\\$" "($|\\s)"]
  (select [:id :symbol :quote]
    (from :quotes)
    (where `(~(symbol "~") "$AAPL" (concat "(^|\\s)\\$" :symbol "($|\\s)"))))
  (is (= :select (:op stmt)))
  (is (= [(parse-from :quotes)] (:from stmt)))
  (is (= (map parse-expr [:id :symbol :quote]) (:exprs stmt)))
  (is (= (parse-condition `(~(symbol "~") "$AAPL" (concat "(^|\\s)\\$" :symbol "($|\\s)"))) (:where stmt)))  )

(deftest-stmt test-select-join-on-columns
  ["SELECT * FROM countries JOIN continents ON (continents.id = countries.continent_id)"]
  (select [*]
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
  ["SELECT * FROM continents JOIN countries ON (countries.continent_id = continents.id)"]
  (select [*]
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
  ["SELECT * FROM countries AS c JOIN continents ON (continents.id = c.continent_id)"]
  (select [*]
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
  ["SELECT * FROM countries JOIN continents USING (id)"]
  (select [*]
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
  ["SELECT * FROM countries JOIN continents USING (id, created_at)"]
  (select [*]
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
  ["SELECT * FROM countries AS c JOIN continents ON (continents.id = c.continent_id)"]
  (select [*]
    (from (as :countries :c))
    (join :continents '(on (= :continents.id :c.continent-id)))))

(deftest-stmt test-select-join-subselect-alias
  [(str "SELECT quotes.*, start_date FROM quotes JOIN (SELECT company_id, min(date) AS start_date "
        "FROM quotes GROUP BY company_id) AS start_dates ON ((quotes.company_id = start_dates.company_id) and (quotes.date = start_dates.start_date))")]
  (select [:quotes.* :start-date]
    (from :quotes)
    (join (as (select [:company-id (as '(min :date) :start-date)]
                (from :quotes)
                (group-by :company-id))
              :start-dates)
          '(on (and (= :quotes.company-id :start-dates.company-id)
                    (= :quotes.date :start-dates.start-date))))))

(deftest-stmt test-select-except
  ["SELECT 1 EXCEPT SELECT 2"]
  (select [1]
    (except (select [2])))
  (is (= :select (:op stmt)))
  (is (= (map parse-expr [1]) (:exprs stmt)))
  (let [except (first (:set stmt))]
    (is (= :except (:op except)))
    (let [stmt (:stmt except)]
      (is (= :select (:op stmt)))
      (is (= (map parse-expr [2]) (:exprs stmt))))))

(deftest-stmt test-select-except-all
  ["SELECT 1 EXCEPT ALL SELECT 2"]
  (select [1]
    (except (select [2]) :all true))
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
  (select [1]
    (intersect (select [2])))
  (is (= :select (:op stmt)))
  (is (= (map parse-expr [1]) (:exprs stmt)))
  (let [intersect (first (:set stmt))]
    (is (= :intersect (:op intersect)))
    (let [stmt (:stmt intersect)]
      (is (= :select (:op stmt)))
      (is (= (map parse-expr [2]) (:exprs stmt))))))

(deftest-stmt test-select-intersect-all
  ["SELECT 1 INTERSECT ALL SELECT 2"]
  (select [1]
    (intersect (select [2]) :all true))
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
  (select [1]
    (union (select [2])))
  (is (= :select (:op stmt)))
  (is (= (map parse-expr [1]) (:exprs stmt)))
  (let [union (first (:set stmt))]
    (is (= :union (:op union)))
    (let [stmt (:stmt union)]
      (is (= :select (:op stmt)))
      (is (= (map parse-expr [2]) (:exprs stmt))))))

(deftest-stmt test-select-union-all
  ["SELECT 1 UNION ALL SELECT 2"]
  (select [1]
    (union (select [2]) :all true))
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
  (select [1]
    (where '(= 1 1) :and)))

(deftest-stmt test-select-where-combine-and-2
  ["SELECT 1 WHERE ((1 = 1) and (2 = 2))"]
  (select [1]
    (where '(= 1 1))
    (where '(= 2 2) :and)))

(deftest-stmt test-select-where-combine-and-3
  ["SELECT 1 WHERE (((1 = 1) and (2 = 2)) and (3 = 3))"]
  (select [1]
    (where '(= 1 1))
    (where '(= 2 2) :and)
    (where '(= 3 3) :and)))

(deftest-stmt test-select-where-combine-or-1
  ["SELECT 1 WHERE (1 = 1)"]
  (select [1]
    (where '(= 1 1) :or)))

(deftest-stmt test-select-where-combine-or-2
  ["SELECT 1 WHERE ((1 = 1) or (2 = 2))"]
  (select [1]
    (where '(= 1 1))
    (where '(= 2 2) :or)))

(deftest-stmt test-select-where-combine-or-3
  ["SELECT 1 WHERE (((1 = 1) or (2 = 2)) or (3 = 3))"]
  (select [1]
    (where '(= 1 1))
    (where '(= 2 2) :or)
    (where '(= 3 3) :or)))

(deftest-stmt test-select-where-combine-mixed
  ["SELECT 1 WHERE (((1 = 1) and (2 = 2)) or (3 = 3))"]
  (select [1]
    (where '(= 1 1))
    (where '(= 2 2) :and)
    (where '(= 3 3) :or)))

(deftest-stmt test-substring-from-to
  ["SELECT substring(? from 2 for 3)" "Thomas"]
  (select ['(substring "Thomas" from 2 for 3)]))

(deftest-stmt test-substring-from-to-lower
  ["SELECT lower(substring(? from 2 for 3))" "Thomas"]
  (select ['(lower (substring "Thomas" from 2 for 3))]))

(deftest-stmt test-substring-from-pattern
  ["SELECT substring(? from ?)" "Thomas" "...$"]
  (select ['(substring "Thomas" from "...$")]))

(deftest-stmt test-substring-from-pattern-for-escape
  ["SELECT substring(? from ? for ?)" "Thomas" "%##\"o_a#\"_" "#"]
  (select ['(substring "Thomas" from "%##\"o_a#\"_" for "#")]))

(deftest-stmt test-trim
  ["SELECT trim(both ? from ?)" "x" "xTomxx"]
  (select ['(trim both "x" from "xTomxx")]))

(deftest-stmt test-select-from-fn
  ["SELECT * FROM generate_series(0, 10)"]
  (select [*] (from '(generate_series 0 10))))

(deftest-stmt test-select-from-fn-alias
  ["SELECT n FROM generate_series(0, 200) AS n"]
  (select [:n] (from (as '(generate_series 0 200) :n))))

(deftest-stmt test-select-qualified-column
  ["SELECT continents.id FROM continents"]
  (select [{:op :column :table :continents :name :id}]
    (from :continents)))

(deftest-stmt test-select-qualified-keyword-column
  ["SELECT continents.id FROM continents"]
  (select [:continents.id] (from :continents)))

;; TRUNCATE

(deftest-stmt test-truncate-continents
  ["TRUNCATE TABLE continents"]
  (truncate [:continents])
  (is (= :truncate (:op stmt)))
  (is (= [(parse-table :continents)] (:tables stmt)))
  (is (nil? (:children stmt))))

(deftest-stmt test-truncate-continents-and-countries
  ["TRUNCATE TABLE continents, countries"]
  (truncate [:continents :countries])
  (is (= :truncate (:op stmt)))
  (is (= (map parse-table [:continents :countries]) (:tables stmt)))
  (is (nil? (:children stmt))))

(deftest-stmt test-truncate-continents-restart-restrict
  ["TRUNCATE TABLE continents RESTART IDENTITY RESTRICT"]
  (truncate [:continents]
    (restart-identity true)
    (restrict true))
  (is (= :truncate (:op stmt)))
  (is (= [(parse-table :continents)] (:tables stmt)))
  (is (= {:op :restrict} (:restrict stmt)))
  (is (= {:op :restart-identity} (:restart-identity stmt))))

(deftest-stmt test-truncate-continents-continue-cascade
  ["TRUNCATE TABLE continents CONTINUE IDENTITY CASCADE"]
  (truncate [:continents]
    (continue-identity true)
    (cascade true))
  (is (= :truncate (:op stmt)))
  (is (= [(parse-table :continents)] (:tables stmt)))
  (is (= {:op :cascade} (:cascade stmt)))
  (is (= {:op :continue-identity} (:continue-identity stmt))))

(deftest-stmt test-truncate-cascade
  ["TRUNCATE TABLE continents CASCADE"]
  (truncate [:continents]
    (cascade true)))

(deftest-stmt test-truncate-continue-identity
  ["TRUNCATE TABLE continents CONTINUE IDENTITY"]
  (truncate [:continents]
    (continue-identity true)))

(deftest-stmt test-truncate-continue-identity-false
  ["TRUNCATE TABLE continents"]
  (truncate [:continents]
    (continue-identity false)))

(deftest-stmt test-truncate-cascade
  ["TRUNCATE TABLE continents"]
  (truncate [:continents]
    (cascade false)))

(deftest-stmt test-truncate-cascade-false
  ["TRUNCATE TABLE continents"]
  (truncate [:continents]
    (cascade false)))

(deftest-stmt test-truncate-restart-identity
  ["TRUNCATE TABLE continents RESTART IDENTITY"]
  (truncate [:continents]
    (restart-identity true)))

(deftest-stmt test-truncate-restart-identity-false
  ["TRUNCATE TABLE continents"]
  (truncate [:continents]
    (restart-identity false)))

(deftest-stmt test-truncate-restrict
  ["TRUNCATE TABLE continents"]
  (truncate [:continents]
    (restrict false)))

(deftest-stmt test-truncate-restrict-false
  ["TRUNCATE TABLE continents"]
  (truncate [:continents]
    (restrict false)))

;; UPDATE

(deftest-stmt test-update-drama-to-dramatic
  ["UPDATE films SET kind = ? WHERE (kind = ?)" "Dramatic" "Drama"]
  (update :films {:kind "Dramatic"}
    (where '(= :kind "Drama")))
  (is (= :update (:op stmt)))
  (is (= (parse-table :films) (:table stmt)))
  (is (= (parse-condition '(= :kind "Drama")) (:where stmt)))
  (is (= {:kind "Dramatic"} (:row stmt))))

(deftest-stmt test-update-drama-to-dramatic-returning
  ["UPDATE films SET kind = ? WHERE (kind = ?) RETURNING *" "Dramatic" "Drama"]
  (update :films {:kind "Dramatic"}
    (where '(= :kind "Drama"))
    (returning *))
  (is (= :update (:op stmt)))
  (is (= (parse-table :films) (:table stmt)))
  (is (= (parse-condition '(= :kind "Drama")) (:where stmt)))
  (is (= {:kind "Dramatic"} (:row stmt)))
  (is (= [(parse-expr *)] (:returning stmt))))

(deftest-stmt test-update-daily-return
  ["UPDATE quotes SET daily_return = u.daily_return FROM (SELECT id, lag(close) over (partition by company_id order by date desc) AS daily_return FROM quotes) AS u WHERE (quotes.id = u.id)"]
  (update :quotes '((= :daily-return :u.daily-return))
    (where '(= :quotes.id :u.id))
    (from (as (select [:id (as '((lag :close) over (partition by :company-id order by :date desc)) :daily-return)]
                (from :quotes))
              :u))))

(deftest-stmt test-update-prices
  [(str "UPDATE prices SET daily_return = u.daily_return "
        "FROM (SELECT id, ((close / lag(close) over (partition by quote_id order by date desc)) - 1) AS daily_return "
        "FROM prices WHERE (prices.quote_id = 1)) AS u WHERE ((prices.id = u.id) and (prices.quote_id = 1))")]
  (let [quote {:id 1}]
    (update :prices '((= :daily-return :u.daily-return))
      (from (as (select [:id (as '(- (/ close ((lag :close) over (partition by :quote-id order by :date desc))) 1) :daily-return)]
                  (from :prices)
                  (where `(= :prices.quote-id ~(:id quote))))
                :u))
      (where `(and (= :prices.id :u.id)
                   (= :prices.quote-id ~(:id quote)))))))

(deftest-stmt test-update-airports
  [(str "UPDATE airports SET country_id = u.id, gps_code = u.gps_code, wikipedia_url = u.wikipedia, location = u.geom "
        "FROM (SELECT DISTINCT ON (a.iata_code) c.id, a.name, a.gps_code, a.iata_code, a.wikipedia, a.geom "
        "FROM natural_earth.airports AS a JOIN countries AS c ON (c.geography && a.geom) "
        "LEFT JOIN airports ON (lower(airports.iata_code) = lower(a.iata_code)) "
        "WHERE ((a.gps_code IS NOT NULL) and (a.iata_code IS NOT NULL) and (airports.iata_code IS NOT NULL))) AS u "
        "WHERE (airports.iata_code = u.iata_code)")]
  (update :airports
      '((= :country-id :u.id)
        (= :gps-code :u.gps-code)
        (= :wikipedia-url :u.wikipedia)
        (= :location :u.geom))
    (from (as (select (distinct [:c.id :a.name :a.gps-code :a.iata-code :a.wikipedia :a.geom] :on [:a.iata-code])
                (from (as :natural-earth.airports :a))
                (join (as :countries :c) '(on (:&& :c.geography :a.geom)))
                (join :airports '(on (= (lower :airports.iata-code) (lower :a.iata-code))) :type :left)
                (where '(and (is-not-null :a.gps-code)
                             (is-not-null :a.iata-code)
                             (is-not-null :airports.iata-code))))
              :u))
    (where '(= :airports.iata-code :u.iata-code))))

(deftest-stmt test-update-countries
  [(str "UPDATE countries SET geom = u.geom FROM (SELECT iso_a2, iso_a3, iso_n3, geom FROM natural_earth.countries) AS u "
        "WHERE ((lower(countries.iso_3166_1_alpha_2) = lower(u.iso_a2)) or (lower(countries.iso_3166_1_alpha_3) = lower(u.iso_a3)))")]
  (update :countries
      '((= :geom :u.geom))
    (from (as (select [:iso-a2 :iso-a3 :iso-n3 :geom]
                (from :natural-earth.countries)) :u))
    (where '(or (= (lower :countries.iso-3166-1-alpha-2) (lower :u.iso-a2))
                (= (lower :countries.iso-3166-1-alpha-3) (lower :u.iso-a3))))))

;; POSTGRESQL ARRAYS

(deftest-stmt test-array
  ["SELECT ARRAY[1, 2]"]
  (select [[1 2]]))

(deftest-stmt test-array-concat
  ["SELECT (ARRAY[1, 2] || ARRAY[3, 4] || ARRAY[5, 6])"]
  (select ['(|| [1 2] [3 4] [5 6])]))

(deftest test-comp-stmts
  (let [s (select [*]
            (from :continents))
        o (order-by :name)]
    (is (= ["SELECT * FROM continents ORDER BY name"]
           (sql (with-monad state-m
                  (m-seq [s o])))))))

;; POSTGRESQL FULLTEXT

(deftest-stmt test-cast-as-document-1
  ["SELECT CAST((title || ? || author || ? || abstract || ? || body) AS document) FROM messages WHERE (mid = 12)" " " " " " "]
  (select ['(cast (:|| :title " " :author " " :abstract " " :body) :document)]
    (from :messages)
    (where '(= :mid 12))))

(deftest-stmt test-cast-as-document-2
  ["SELECT CAST((m.title || ? || m.author || ? || m.abstract || ? || d.body) AS document) FROM messages AS m, docs AS d WHERE ((mid = did) and (mid = 12))" " " " " " "]
  (select ['(cast (:|| :m.title " " :m.author " " :m.abstract " " :d.body) :document)]
    (from (as :messages :m) (as :docs :d))
    (where '(and (= :mid :did)
                 (= :mid 12)))))

(deftest-stmt test-basic-text-matching-1
  ["SELECT (CAST(? AS tsvector) @@ CAST(? AS tsquery))" "a fat cat sat on a mat and ate a fat rat" "rat & cat"]
  (select [`(~(keyword "@@")
             (cast "a fat cat sat on a mat and ate a fat rat" :tsvector)
             (cast "rat & cat" :tsquery))]))

(deftest-stmt test-basic-text-matching-2
  ["SELECT (CAST(? AS tsquery) @@ CAST(? AS tsvector))" "fat & cow" "a fat cat sat on a mat and ate a fat rat"]
  (select [`(~(keyword "@@")
             (cast "fat & cow" :tsquery)
             (cast "a fat cat sat on a mat and ate a fat rat" :tsvector))]))

(deftest-stmt test-basic-text-matching-3
  ["SELECT (to_tsvector(?) @@ to_tsquery(?))" "fat cats ate fat rats" "fat & rat"]
  (select [`(~(keyword "@@")
             (to_tsvector "fat cats ate fat rats")
             (to_tsquery "fat & rat"))]))

(deftest-stmt test-basic-text-matching-4
  ["SELECT (CAST(? AS tsvector) @@ to_tsquery(?))" "fat cats ate fat rats" "fat & rat"]
  (select [`(~(keyword "@@")
             (cast "fat cats ate fat rats" :tsvector)
             (to_tsquery "fat & rat"))]))

(deftest-stmt test-searching-a-table-1
  ["SELECT title FROM pgweb WHERE (to_tsvector(?, body) @@ to_tsquery(?, ?))" "english" "english" "friend"]
  (select [:title]
    (from :pgweb)
    (where `(~(keyword "@@")
             (to_tsvector "english" :body)
             (to_tsquery "english" "friend")))))

(deftest-stmt test-searching-a-table-2
  ["SELECT title FROM pgweb WHERE (to_tsvector(body) @@ to_tsquery(?))" "friend"]
  (select [:title]
    (from :pgweb)
    (where `(~(keyword "@@")
             (to_tsvector :body)
             (to_tsquery "friend")))))

(deftest-stmt test-searching-a-table-3
  ["SELECT title FROM pgweb WHERE (to_tsvector((title || ? || body)) @@ to_tsquery(?)) ORDER BY last_mod_date DESC LIMIT 10" " " "create & table"]
  (select [:title]
    (from :pgweb)
    (where `(~(keyword "@@")
             (to_tsvector (:|| :title " " :body))
             (to_tsquery "create & table")))
    (order-by (desc :last-mod-date))
    (limit 10)))
