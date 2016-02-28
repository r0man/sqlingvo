(ns sqlingvo.update-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.test :refer :all]
            [sqlingvo.core :refer :all]
            [sqlingvo.expr :refer :all]
            [sqlingvo.test :refer [db sql= with-stmt]]))

(deftest test-update-drama-to-dramatic
  (with-stmt
    ["UPDATE \"films\" SET \"kind\" = ? WHERE (\"kind\" = ?)" "Dramatic" "Drama"]
    (update db :films
      {:kind "Dramatic"}
      (where '(= :kind "Drama")))
    (is (= :update (:op stmt)))
    (is (= (parse-table :films) (:table stmt)))
    (is (= (parse-condition '(= :kind "Drama")) (:where stmt)))
    (is (= (parse-map-expr {:kind "Dramatic"})
           (:row stmt)))))

(deftest test-update-drama-to-dramatic-returning
  (with-stmt
    ["UPDATE \"films\" SET \"kind\" = ? WHERE (\"kind\" = ?) RETURNING *" "Dramatic" "Drama"]
    (update db :films
      {:kind "Dramatic"}
      (where '(= :kind "Drama"))
      (returning *))
    (is (= :update (:op stmt)))
    (is (= (parse-table :films) (:table stmt)))
    (is (= (parse-condition '(= :kind "Drama")) (:where stmt)))
    (is (= (parse-map-expr {:kind "Dramatic"})
           (:row stmt)))
    (is (= [(parse-expr *)] (:returning stmt)))))

(deftest test-update-daily-return
  (with-stmt
    ["UPDATE \"quotes\" SET \"daily-return\" = \"u\".\"daily-return\" FROM (SELECT \"id\", \"lag\"(\"close\") over (partition by \"company-id\" order by \"date\" desc) AS \"daily-return\" FROM \"quotes\") AS \"u\" WHERE (\"quotes\".\"id\" = \"u\".\"id\")"]
    (update db :quotes
      '((= :daily-return :u.daily-return))
      (where '(= :quotes.id :u.id))
      (from (as (select db [:id (as '((lag :close) over (partition by :company-id order by :date desc)) :daily-return)]
                  (from :quotes))
                :u)))))

(deftest test-update-prices
  (with-stmt
    [(str "UPDATE \"prices\" SET \"daily-return\" = \"u\".\"daily-return\" "
          "FROM (SELECT \"id\", ((\"close\" / \"lag\"(\"close\") over (partition by \"quote-id\" order by \"date\" desc)) - 1) AS \"daily-return\" "
          "FROM \"prices\" WHERE (\"prices\".\"quote-id\" = 1)) AS \"u\" WHERE ((\"prices\".\"id\" = \"u\".\"id\") and (\"prices\".\"quote-id\" = 1))")]
    (let [quote {:id 1}]
      (update db :prices
        '((= :daily-return :u.daily-return))
        (from (as (select db [:id (as '(- (/ :close ((lag :close) over (partition by :quote-id order by :date desc))) 1) :daily-return)]
                    (from :prices)
                    (where `(= :prices.quote-id ~(:id quote))))
                  :u))
        (where `(and (= :prices.id :u.id)
                     (= :prices.quote-id ~(:id quote))))))))

(deftest test-update-airports
  (with-stmt
    [(str "UPDATE \"airports\" SET \"country-id\" = \"u\".\"id\", \"gps-code\" = \"u\".\"gps-code\", \"wikipedia-url\" = \"u\".\"wikipedia\", \"location\" = \"u\".\"geom\" "
          "FROM (SELECT DISTINCT ON (\"a\".\"iata-code\") \"c\".\"id\", \"a\".\"name\", \"a\".\"gps-code\", \"a\".\"iata-code\", \"a\".\"wikipedia\", \"a\".\"geom\" "
          "FROM \"natural-earth\".\"airports\" \"a\" JOIN \"countries\" \"c\" ON (\"c\".\"geography\" && \"a\".\"geom\") "
          "LEFT JOIN \"airports\" ON (\"lower\"(\"airports\".\"iata-code\") = \"lower\"(\"a\".\"iata-code\")) "
          "WHERE ((\"a\".\"gps-code\" IS NOT NULL) and (\"a\".\"iata-code\" IS NOT NULL) and (\"airports\".\"iata-code\" IS NOT NULL))) AS \"u\" "
          "WHERE (\"airports\".\"iata-code\" = \"u\".\"iata-code\")")]
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
      (where '(= :airports.iata-code :u.iata-code)))))

(deftest test-update-countries
  (with-stmt
    [(str "UPDATE \"countries\" SET \"geom\" = \"u\".\"geom\" FROM (SELECT \"iso-a2\", \"iso-a3\", \"iso-n3\", \"geom\" FROM \"natural-earth\".\"countries\") AS \"u\" "
          "WHERE ((\"lower\"(\"countries\".\"iso-3166-1-alpha-2\") = \"lower\"(\"u\".\"iso-a2\")) or (\"lower\"(\"countries\".\"iso-3166-1-alpha-3\") = \"lower\"(\"u\".\"iso-a3\")))")]
    (update db :countries
      '((= :geom :u.geom))
      (from (as (select db [:iso-a2 :iso-a3 :iso-n3 :geom]
                  (from :natural-earth.countries)) :u))
      (where '(or (= (lower :countries.iso-3166-1-alpha-2) (lower :u.iso-a2))
                  (= (lower :countries.iso-3166-1-alpha-3) (lower :u.iso-a3)))))))

(deftest test-update-with-fn-call
  (with-stmt
    ["UPDATE \"films\" SET \"name\" = \"lower\"(\"name\") WHERE (\"id\" = 1)"]
    (update db :films
      {:name '(lower :name)}
      (where `(= :id 1)))))
