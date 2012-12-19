(ns sqlingvo.test.core
  (:refer-clojure :exclude [distinct group-by replace])
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        sqlingvo.core))

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
       (-> (select *) (from (as (select 1 2 3) :x)))
       ["SELECT * FROM (SELECT 1, 2, 3) AS x"]
       (-> (select *) (from (as (select 1) :x) (as (select 2) :y)))
       ["SELECT * FROM (SELECT 1) AS x, (SELECT 2) AS y"]
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
           (from (as :countries :c))
           (join :continents '(on (= :continents.id :c.continent-id))))
       ["SELECT * FROM countries AS c JOIN continents ON (continents.id = c.continent-id)"]
       (-> (select (distinct [:x.a :x.b]))
           (from (as (select (as 1 :a) (as 2 :b)) :x)))
       ["SELECT DISTINCT x.a, x.b FROM (SELECT 1 AS a, 2 AS b) AS x"]
       (-> (select (distinct [:x.a :x.b] :on [:x.a :x.b]))
           (from (as (select (as 1 :a) (as 2 :b)) :x)))
       ["SELECT DISTINCT ON (x.a, x.b) x.a, x.b FROM (SELECT 1 AS a, 2 AS b) AS x"]
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
