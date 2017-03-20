(ns sqlingvo.update-test
  (:require #?(:clj [sqlingvo.test :refer [db sql=]]
               :cljs [sqlingvo.test :refer [db] :refer-macros [sql=]])
            [clojure.test :refer [deftest is]]
            [sqlingvo.core :as sql]))

(deftest test-update-keyword-db
  (sql= (sql/update :postgresql :films
          {:kind "Dramatic"}
          (sql/where '(= :kind "Drama")))
        ["UPDATE \"films\" SET \"kind\" = ? WHERE (\"kind\" = ?)"
         "Dramatic" "Drama"]))

(deftest test-update-drama-to-dramatic
  (sql= (sql/update db :films
          {:kind "Dramatic"}
          (sql/where '(= :kind "Drama")))
        ["UPDATE \"films\" SET \"kind\" = ? WHERE (\"kind\" = ?)"
         "Dramatic" "Drama"]))

(deftest test-update-drama-to-dramatic-returning
  (sql= (sql/update db :films
          {:kind "Dramatic"}
          (sql/where '(= :kind "Drama"))
          (sql/returning :*))
        ["UPDATE \"films\" SET \"kind\" = ? WHERE (\"kind\" = ?) RETURNING *"
         "Dramatic" "Drama"]))

(deftest test-update-daily-return
  (sql= (sql/update db :quotes
          '((= :daily-return :u.daily-return))
          (sql/where '(= :quotes.id :u.id))
          (sql/from (sql/as
                     (sql/select db [:id (sql/as '((lag :close) over (partition by :company-id order by :date desc)) :daily-return)]
                       (sql/from :quotes))
                     :u)))
        [(str "UPDATE \"quotes\" "
              "SET \"daily-return\" = \"u\".\"daily-return\" "
              "FROM (SELECT \"id\", lag(\"close\") over (partition by \"company-id\" order by \"date\" desc) AS \"daily-return\" FROM \"quotes\") AS \"u\" WHERE (\"quotes\".\"id\" = \"u\".\"id\")")]))

(deftest test-update-prices
  (let [quote {:id 1}]
    (sql= (sql/update db :prices
            '((= :daily-return :u.daily-return))
            (sql/from
             (sql/as (sql/select db [:id (sql/as '(- (/ :close ((lag :close) over (partition by :quote-id order by :date desc))) 1) :daily-return)]
                       (sql/from :prices)
                       (sql/where `(= :prices.quote-id ~(:id quote))))
                     :u))
            (sql/where `(and (= :prices.id :u.id)
                             (= :prices.quote-id ~(:id quote)))))
          [(str "UPDATE \"prices\" SET \"daily-return\" = \"u\".\"daily-return\" "
                "FROM (SELECT \"id\", ((\"close\" / lag(\"close\") over (partition by \"quote-id\" order by \"date\" desc)) - 1) AS \"daily-return\" "
                "FROM \"prices\" WHERE (\"prices\".\"quote-id\" = 1)) AS \"u\" WHERE ((\"prices\".\"id\" = \"u\".\"id\") and (\"prices\".\"quote-id\" = 1))")])))

(deftest test-update-airports
  (sql= (sql/update db :airports
          '((= :country-id :u.id)
            (= :gps-code :u.gps-code)
            (= :wikipedia-url :u.wikipedia)
            (= :location :u.geom))
          (sql/from
           (sql/as
            (sql/select db (sql/distinct [:c.id :a.name :a.gps-code :a.iata-code :a.wikipedia :a.geom] :on [:a.iata-code])
              (sql/from (sql/as :natural-earth.airports :a))
              (sql/join (sql/as :countries :c) '(on (:&& :c.geography :a.geom)))
              (sql/join :airports '(on (= (lower :airports.iata-code) (lower :a.iata-code))) :type :left)
              (sql/where '(and (is-not-null :a.gps-code)
                               (is-not-null :a.iata-code)
                               (is-not-null :airports.iata-code))))
            :u))
          (sql/where '(= :airports.iata-code :u.iata-code)))
        [(str "UPDATE \"airports\" SET \"country-id\" = \"u\".\"id\", \"gps-code\" = \"u\".\"gps-code\", \"wikipedia-url\" = \"u\".\"wikipedia\", \"location\" = \"u\".\"geom\" "
              "FROM (SELECT DISTINCT ON (\"a\".\"iata-code\") \"c\".\"id\", \"a\".\"name\", \"a\".\"gps-code\", \"a\".\"iata-code\", \"a\".\"wikipedia\", \"a\".\"geom\" "
              "FROM \"natural-earth\".\"airports\" \"a\" JOIN \"countries\" \"c\" ON (\"c\".\"geography\" && \"a\".\"geom\") "
              "LEFT JOIN \"airports\" ON (lower(\"airports\".\"iata-code\") = lower(\"a\".\"iata-code\")) "
              "WHERE ((\"a\".\"gps-code\" IS NOT NULL) and (\"a\".\"iata-code\" IS NOT NULL) and (\"airports\".\"iata-code\" IS NOT NULL))) AS \"u\" "
              "WHERE (\"airports\".\"iata-code\" = \"u\".\"iata-code\")")]))

(deftest test-update-countries
  (sql= (sql/update db :countries
          '((= :geom :u.geom))
          (sql/from
           (sql/as
            (sql/select db [:iso-a2 :iso-a3 :iso-n3 :geom]
              (sql/from :natural-earth.countries)) :u))
          (sql/where '(or (= (lower :countries.iso-3166-1-alpha-2) (lower :u.iso-a2))
                          (= (lower :countries.iso-3166-1-alpha-3) (lower :u.iso-a3)))))
        [(str "UPDATE \"countries\" SET \"geom\" = \"u\".\"geom\" FROM (SELECT \"iso-a2\", \"iso-a3\", \"iso-n3\", \"geom\" FROM \"natural-earth\".\"countries\") AS \"u\" "
              "WHERE ((lower(\"countries\".\"iso-3166-1-alpha-2\") = lower(\"u\".\"iso-a2\")) or (lower(\"countries\".\"iso-3166-1-alpha-3\") = lower(\"u\".\"iso-a3\")))")]))

(deftest test-update-with-fn-call
  (sql= (sql/update db :films
          {:name '(lower :name)}
          (sql/where `(= :id 1)))
        [(str "UPDATE \"films\" "
              "SET \"name\" = lower(\"name\") "
              "WHERE (\"id\" = 1)")]))
