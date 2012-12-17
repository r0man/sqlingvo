(ns sqlingvo.test.core
  (:refer-clojure :exclude [distinct group-by replace])
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        sqlingvo.core))

(defmacro with-database [& body]
  `(jdbc/with-connection "jdbc:sqlite:/tmp/sqlingvo.sqlite"
     ~@body))

(deftest test-create-table
  (are [stmt expected]
       (is (= expected (sql stmt)))
       (-> (create-table :import)
           (if-not-exists true)
           (inherits :quotes)
           (temporary true))
       ["CREATE TEMPORARY TABLE IF NOT EXISTS import () INHERITS (quotes)"]
       (-> (create-table :tmp-films) (like :films))
       ["CREATE TABLE tmp-films (LIKE films)"]
       (-> (create-table :tmp-films) (like :films :including [:all]))
       ["CREATE TABLE tmp-films (LIKE films INCLUDING ALL)"]
       (-> (create-table :tmp-films) (like :films :including [:defaults :constraints]))
       ["CREATE TABLE tmp-films (LIKE films INCLUDING DEFAULTS INCLUDING CONSTRAINTS)"]
       (-> (create-table :tmp-films) (like :films :excluding [:defaults :constraints]))
       ["CREATE TABLE tmp-films (LIKE films EXCLUDING DEFAULTS EXCLUDING CONSTRAINTS)"]))

(deftest test-insert
  (are [stmt expected]
       (is (= expected (sql stmt)))
       (-> (insert :films
               (-> (select *)
                   (from :tmp-films)
                   (where '(< :date-prod "2004-05-07")))))
       ["INSERT INTO films SELECT * FROM tmp-films WHERE (date-prod < ?)" "2004-05-07"]
       (insert :airports [:country-id, :name :gps-code :iata-code :wikipedia-url :location]
         (-> (select (distinct [:c.id :a.name :a.gps-code :a.iata-code :a.wikipedia :a.geom] :on [:a.iata-code]))
             (from (as :natural-earth.airports :a))
             (join (as :countries :c) '(on (:&& :c.geography :a.geom)))
             (join :airports '(on (= :airports.iata-code :a.iata-code)) :type :left)
             (where '(and (is-not-null :a.gps-code)
                          (is-not-null :a.iata-code)
                          (is-null :airports.iata-code)))))
       [(str "INSERT INTO airports (country-id, name, gps-code, iata-code, wikipedia-url, location) "
             "SELECT DISTINCT ON (a.iata-code) c.id, a.name, a.gps-code, a.iata-code, a.wikipedia, a.geom "
             "FROM natural-earth.airports AS a "
             "LEFT JOIN airports ON (airports.iata-code = a.iata-code) "
             "JOIN countries AS c ON (c.geography && a.geom) "
             "WHERE ((a.gps-code IS NOT NULL) and (a.iata-code IS NOT NULL) and (airports.iata-code IS NULL))")]))

(deftest test-select
  (are [stmt expected]
       (is (= expected (sql stmt)))
       (-> (select * 1 "x") (from :continents))
       ["SELECT *, 1, ? FROM continents" "x"]
       (-> (select :created-at) (from :continents))
       ["SELECT created-at FROM continents"]
       (-> (select :created-at/c) (from :continents))
       ["SELECT created-at AS c FROM continents"]
       (-> (select :name :created-at) (from :continents))
       ["SELECT name, created-at FROM continents"]
       (-> (select :name '(max :created-at)) (from :continents))
       ["SELECT name, max(created-at) FROM continents"]
       (select '(greatest 1 2) '(lower "X"))
       ["SELECT greatest(1, 2), lower(?)" "X"]
       (select '(+ 1 (greatest 2 3)))
       ["SELECT (1 + greatest(2, 3))"]
       (-> (select (as '(max :created-at) :m)) (from :continents))
       ["SELECT max(created-at) AS m FROM continents"]
       (-> (select *) (from :continents) (limit 1))
       ["SELECT * FROM continents LIMIT 1"]
       (-> (-> (select *)) (from :continents) (offset 1))
       ["SELECT * FROM continents OFFSET 1"]
       (-> (-> (select *)) (from :continents) (limit 1) (offset 2))
       ["SELECT * FROM continents LIMIT 1 OFFSET 2"]
       (-> (select *) (from :continents) (order-by :created-at))
       ["SELECT * FROM continents ORDER BY created-at"]
       (-> (select *) (from :continents) (order-by "created-at"))
       ["SELECT * FROM continents ORDER BY created-at"]
       (-> (select *) (from :continents) (order-by "(lower :name)"))
       ["SELECT * FROM continents ORDER BY lower(name)"]
       (-> (select *) (from :continents) (order-by ['(lower :name)]))
       ["SELECT * FROM continents ORDER BY lower(name)"]
       (-> (select *) (from :continents) (order-by :created-at :direction :asc))
       ["SELECT * FROM continents ORDER BY created-at ASC"]
       (-> (select *) (from :continents) (order-by :created-at :direction :desc))
       ["SELECT * FROM continents ORDER BY created-at DESC"]
       (-> (select *) (from :continents) (order-by :created-at :nulls :first))
       ["SELECT * FROM continents ORDER BY created-at NULLS FIRST"]
       (-> (select *) (from :continents) (order-by :created-at :nulls :last))
       ["SELECT * FROM continents ORDER BY created-at NULLS LAST"]
       (-> (select *) (from :continents) (order-by [:name :created-at] :direction :asc))
       ["SELECT * FROM continents ORDER BY name, created-at ASC"]
       (-> (select *) (from (as (select 1 2 3) :x)))
       ["SELECT * FROM (SELECT 1, 2, 3) AS x"]
       (-> (select *) (from (as (select 1) :x) (as (select 2) :y)))
       ["SELECT * FROM (SELECT 1) AS x, (SELECT 2) AS y"]
       (-> (select *) (from :continents) (group-by :created-at))
       ["SELECT * FROM continents GROUP BY created-at"]
       (-> (select *) (from :continents) (group-by :name :created-at))
       ["SELECT * FROM continents GROUP BY name, created-at"]
       (-> (select 1) (where '(= 1 1)))
       ["SELECT 1 WHERE (1 = 1)"]
       (-> (select 1) (where (list '= 1 1)))
       ["SELECT 1 WHERE (1 = 1)"]
       (-> (select 1) (where (list 'and (list '= 1 1) (list '= 1 2))))
       ["SELECT 1 WHERE ((1 = 1) and (1 = 2))"]
       (-> (select 1) (where '(= 1 2 3)))
       ["SELECT 1 WHERE (1 = 2) AND (2 = 3)"]
       (union (select 1) (select 2))
       ["SELECT 1 UNION SELECT 2"]
       (union (select 1) (select 2) :all true)
       ["SELECT 1 UNION ALL SELECT 2"]
       (intersect (select 1) (select 2))
       ["SELECT 1 INTERSECT SELECT 2"]
       (intersect (select 1) (select 2) :all true)
       ["SELECT 1 INTERSECT ALL SELECT 2"]
       (except (select 1) (select 2))
       ["SELECT 1 EXCEPT SELECT 2"]
       (except (select 1) (select 2) :all true)
       ["SELECT 1 EXCEPT ALL SELECT 2"]
       (-> (select *) (from :continents) (where '(= :name "Europe")))
       ["SELECT * FROM continents WHERE (name = ?)" "Europe"]
       (-> (select *)
           (from :countries)
           (join :continents '(on (= :continents.id :countries.continent-id))))
       ["SELECT * FROM countries JOIN continents ON (continents.id = countries.continent-id)"]
       (-> (select *)
           (from (as :countries :c))
           (join :continents '(on (= :continents.id :c.continent-id))))
       ["SELECT * FROM countries AS c JOIN continents ON (continents.id = c.continent-id)"]
       (-> (select *)
           (from :countries)
           (join :continents '(using :id)))
       ["SELECT * FROM countries JOIN continents USING (id)"]
       (-> (select *)
           (from :countries)
           (join :continents '(using :id :created-at)))
       ["SELECT * FROM countries JOIN continents USING (id, created-at)"]
       (-> (select *)
           (from :countries)
           (where `(> :created-at ~(java.sql.Date. 0))))
       ["SELECT * FROM countries WHERE (created-at > ?)" (java.sql.Date. 0)]
       (-> (select :id '((lag :close) over (partition by :company-id order by :date desc))) (from :quotes))
       ["SELECT id, lag(close) over (partition by company-id order by date desc) FROM quotes"]
       (-> (select :id '(- ((lag :close) over (partition by :company-id order by :date desc)) 1)) (from :quotes))
       ["SELECT id, (lag(close) over (partition by company-id order by date desc) - 1) FROM quotes"]
       (-> (select :id '(/ close (- ((lag :close) over (partition by :company-id order by :date desc)) 1))) (from :quotes))
       ["SELECT id, (close / (lag(close) over (partition by company-id order by date desc) - 1)) FROM quotes"]
       (-> (select :id (as '(/ close (- ((lag :close) over (partition by :company-id order by :date desc)) 1)) :daily-return)) (from :quotes))
       ["SELECT id, (close / (lag(close) over (partition by company-id order by date desc) - 1)) AS daily-return FROM quotes"]
       (-> (select :quotes.* :start-date)
           (from :quotes)
           (join (as (-> (select :company-id (as '(min :date) :start-date))
                         (from :quotes)
                         (group-by :company-id))
                     :start-dates)
                 '(on (and (= :quotes.company-id :start-dates.company-id)
                           (= :quotes.date :start-dates.start-date)))))
       ["SELECT quotes.*, start-date FROM quotes JOIN (SELECT company-id, min(date) AS start-date FROM quotes GROUP BY company-id) AS start-dates ON ((quotes.company-id = start-dates.company-id) and (quotes.date = start-dates.start-date))"]
       (-> (select :id :symbol :quote)
           (from :quotes)
           (where '("~" "$AAPL" (concat "(^|\\s)\\$" :symbol "($|\\s)"))))
       ["SELECT id, symbol, quote FROM quotes WHERE (? ~ concat(?, symbol, ?))" "$AAPL" "(^|\\s)\\$" "($|\\s)"]
       (select `(setval :continentd-id-seq
                        ~(-> (select `(max :id))
                             (from :continents))))
       ["SELECT setval(continentd-id-seq, (SELECT max(id) FROM continents))"]
       (-> (select (distinct [:x.a :x.b]))
           (from (as (select (as 1 :a) (as 2 :b)) :x)))
       ["SELECT DISTINCT x.a, x.b FROM (SELECT 1 AS a, 2 AS b) AS x"]
       (-> (select (distinct [:x.a :x.b] :on [:x.a :x.b]))
           (from (as (select (as 1 :a) (as 2 :b)) :x)))
       ["SELECT DISTINCT ON (x.a, x.b) x.a, x.b FROM (SELECT 1 AS a, 2 AS b) AS x"]
       (-> (select (distinct [:location :time :report] :on [:location]))
           (from :weather-reports)
           (order-by [:location :time] :direction :desc))
       ["SELECT DISTINCT ON (location) location, time, report FROM weather-reports ORDER BY location, time DESC"]
       ))

(deftest test-update
  (are [stmt expected]
       (is (= expected (sql stmt)))
       (-> (update :films {:kind "Dramatic"})
           (where '(= :kind "Drama")))
       ["UPDATE films SET kind = ? WHERE (kind = ?)" "Dramatic" "Drama"]
       (-> (update :quotes '((= :daily-return :u.daily-return)))
           (where '(= :quotes.id :u.id))
           (from (as (-> (select :id (as '((lag :close) over (partition by :company-id order by :date desc)) :daily-return))
                         (from :quotes)) :u)))
       ["UPDATE quotes SET daily-return = u.daily-return FROM (SELECT id, lag(close) over (partition by company-id order by date desc) AS daily-return FROM quotes) AS u WHERE (quotes.id = u.id)"]
       (let [quote {:id 1}]
         (-> (update :prices '((= :daily-return :u.daily-return)))
             (from (as (-> (select :id (as '(- (/ close ((lag :close) over (partition by :quote-id order by :date desc))) 1)
                                           :daily-return))
                           (from :prices)
                           (where `(= :prices.quote-id ~(:id quote)))) :u))
             (where `(and (= :prices.id :u.id)
                          (= :prices.quote-id ~(:id quote))))))
       [(str "UPDATE prices SET daily-return = u.daily-return "
             "FROM (SELECT id, ((close / lag(close) over (partition by quote-id order by date desc)) - 1) AS daily-return "
             "FROM prices WHERE (prices.quote-id = 1)) AS u WHERE ((prices.id = u.id) and (prices.quote-id = 1))")]
       (-> (update
               :airports
               '((= :country-id :u.id)
                 (= :gps-code :u.gps-code)
                 (= :wikipedia-url :u.wikipedia)
                 (= :location :u.geom)))
           (from (-> (select (distinct [:c.id :a.name :a.gps-code :a.iata-code :a.wikipedia :a.geom] :on [:a.iata-code]))
                     (from (as :natural-earth.airports :a))
                     (join (as :countries :c) '(on (:&& :c.geography :a.geom)))
                     (join :airports '(on (= (lower :airports.iata-code) (lower :a.iata-code))) :type :left)
                     (where '(and (is-not-null :a.gps-code)
                                  (is-not-null :a.iata-code)
                                  (is-not-null :airports.iata-code)))
                     (as :u)))
           (where '(= :airports.iata-code :u.iata-code)))
       [(str "UPDATE airports SET country-id = u.id, gps-code = u.gps-code, wikipedia-url = u.wikipedia, location = u.geom "
             "FROM (SELECT DISTINCT ON (a.iata-code) c.id, a.name, a.gps-code, a.iata-code, a.wikipedia, a.geom "
             "FROM natural-earth.airports AS a LEFT JOIN airports ON (lower(airports.iata-code) = lower(a.iata-code)) "
             "JOIN countries AS c ON (c.geography && a.geom) WHERE ((a.gps-code IS NOT NULL) and "
             "(a.iata-code IS NOT NULL) and (airports.iata-code IS NOT NULL))) AS u WHERE (airports.iata-code = u.iata-code)")]))

(deftest test-run-stmt
  (with-database
    (are [stmt expected]
         (is (= expected (run-stmt stmt)))
         (select 1)
         [{:1 1}]
         (select (as 1 :n))
         [{:n 1}]
         (select (as "s" :s))
         [{:s "s"}]
         (select 1 2 3)
         [{:1 1 :2 2 :3 3}]
         (select (as 1 :a) (as 2 :b) (as 3 :c))
         [{:a 1 :b 2 :c 3}]
         (select '(lower "X"))
         [(assoc nil (keyword "lower(?)") "x")]
         (-> (select *) (from (as (select 1 2 3) :x)))
         [{:1 1 :2 2 :3 3}]
         (-> (select *) (from (as (select 1) :x) (as (select 2) :y)))
         [{:1 1 :2 2}]
         (-> (select 1) (where '(= 1 1)))
         [{:1 1}]
         (-> (select 1) (where '(!= 1 1)))
         nil
         (-> (select 1) (where '(= 1 2 3)))
         nil
         (-> (select 1) (where '(< 1 2)))
         [{:1 1}]
         (-> (select 1) (where '(< 1 2 3)))
         [{:1 1}]
         (select (select 1))
         [(assoc nil (keyword "(select 1)") 1)]
         (select (select 1) (select "x"))
         [(assoc nil (keyword "(select 1)") 1 (keyword "(select ?)") "x")]
         (union (select 1) (select 1))
         [{:1 1}]
         (union (select 1) (select 1) :all true)
         [{:1 1} {:1 1}]
         (union (select 1) (select 2) :all true)
         [{:1 1} {:1 2}]
         (intersect (select 1) (select 2))
         nil
         (intersect (select 1) (select 1))
         [{:1 1}]
         (except (select 1) (select 2))
         [{:1 1}]
         (except (select 1) (select 1))
         nil)))
