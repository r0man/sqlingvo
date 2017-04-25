(ns sqlingvo.select-test
  (:require #?(:clj [sqlingvo.test :refer [db sql=]]
               :cljs [sqlingvo.test :refer [db] :refer-macros [sql=]])
            [clojure.test :refer [are deftest is]]
            [sqlingvo.core :as sql]
            [sqlingvo.db :as db]
            [sqlingvo.expr :as expr]))

(deftest test-select-from
  (sql= (sql/select db [:*]
          (sql/from :continents))
        ["SELECT * FROM \"continents\""]))

(deftest test-select-in-list-of-numbers
  (sql= (sql/select db [:*]
          (sql/from :continents)
          (sql/where '(in 1 (1 2 3))))
        ["SELECT * FROM \"continents\" WHERE 1 IN (1, 2, 3)"]))

(deftest test-select-in-list-of-strings
  (sql= (sql/select db [:*]
          (sql/from :spots)
          (sql/where '(in :name ("Mundaka" "Pipeline"))))
        ["SELECT * FROM \"spots\" WHERE \"name\" IN (?, ?)"
         "Mundaka" "Pipeline"]))

(deftest test-select-in-empty-list
  (sql= (sql/select db [:*]
          (sql/from :continents)
          (sql/where '(in 1 ())))
        ["SELECT * FROM \"continents\" WHERE 1 IN (NULL)"]))

(deftest test-select-1
  (sql= (sql/select db [1])
        ["SELECT 1"]))

(deftest test-select-1-as
  (sql= (sql/select db [(sql/as 1 :n)])
        ["SELECT 1 AS \"n\""]))

(deftest test-select-x-as-x
  (sql= (sql/select db [(sql/as "x" :x)])
        ["SELECT ? AS \"x\"" "x"]))

(deftest test-select-1-2-3
  (sql= (sql/select db [1 2 3])
        ["SELECT 1, 2, 3"]))

(deftest test-select-1-2-3-as
  (sql= (sql/select db [(sql/as 1 :a) (sql/as 2 :b) (sql/as 3 :c)])
        ["SELECT 1 AS \"a\", 2 AS \"b\", 3 AS \"c\""]))

(deftest test-select-select-1
  (sql= (sql/select db [(sql/select db [1])])
        ["SELECT (SELECT 1)"]))

(deftest test-select-1-in-1-2-3
  (sql= (sql/select db [1]
          (sql/where '(in 1 (1 2 3))))
        ["SELECT 1 WHERE 1 IN (1, 2, 3)"]))

(deftest test-select-1-in-1-2-3-backquote
  (sql= (sql/select db [1]
          (sql/where `(in 1 (1 2 3))))
        ["SELECT 1 WHERE 1 IN (1, 2, 3)"]))

(deftest test-select-select-1-select-x
  (sql= (sql/select db [(sql/select db [1]) (sql/select db ["x"])])
        ["SELECT (SELECT 1), (SELECT ?)" "x"]))

(deftest test-select-string
  (sql= (sql/select db [:*]
          (sql/from :continents)
          (sql/where '(= :name "Europe")))
        ["SELECT * FROM \"continents\" WHERE (\"name\" = ?)" "Europe"]))

(deftest test-select-where-single-arg-and
  (sql= (sql/select db [1]
          (sql/where '(and (= 1 1))))
        ["SELECT 1 WHERE (1 = 1)"]))

(deftest test-select-less-2-arity
  (sql= (sql/select db [1]
          (sql/where '(< 1 2)))
        ["SELECT 1 WHERE (1 < 2)"]))

(deftest test-select-less-3-arity
  (sql= (sql/select db [1]
          (sql/where '(< 1 2 3)))
        ["SELECT 1 WHERE (1 < 2) AND (2 < 3)"]))

(deftest test-select-like
  (sql= (sql/select db [:*]
          (sql/from :films)
          (sql/where '(like :title "%Zombie%")))
        ["SELECT * FROM \"films\" WHERE (\"title\" like ?)" "%Zombie%"]))

(deftest test-select-not-like
  (sql= (sql/select db [:*]
          (sql/from :films)
          (sql/where '(not-like :title "%Zombie%")))
        ["SELECT * FROM \"films\" WHERE (\"title\" NOT LIKE ?)" "%Zombie%"]))

(deftest test-select-continents
  (sql= (sql/select db [:*]
          (sql/from :continents))
        ["SELECT * FROM \"continents\""]))

(deftest test-select-continents-qualified
  (sql= (sql/select db [:continents.*]
          (sql/from :continents))
        ["SELECT \"continents\".* FROM \"continents\""]))

(deftest test-select-films
  (sql= (sql/select db [:*] (sql/from :films))
        ["SELECT * FROM \"films\""]))

(deftest test-select-comedy-films
  (sql= (sql/select db [:*]
          (sql/from :films)
          (sql/where '(= :kind "Comedy")))
        ["SELECT * FROM \"films\" WHERE (\"kind\" = ?)" "Comedy"]))

(deftest test-select-is-null
  (sql= (sql/select db [1]
          (sql/where '(is-null nil)))
        ["SELECT 1 WHERE (NULL IS NULL)"]))

(deftest test-select-is-not-null
  (sql= (sql/select db [1]
          (sql/where '(is-not-null nil)))
        ["SELECT 1 WHERE (NULL IS NOT NULL)"]))

(deftest test-select-backquote-date
  (sql= (sql/select db [:*]
          (sql/from :countries)
          (sql/where `(> :created-at #inst "2016-01-01")))
        ["SELECT * FROM \"countries\" WHERE (\"created-at\" > ?)" #inst "2016-01-01"]))

(deftest test-select-star-number-string
  (sql= (sql/select db [:* 1 "x"])
        ["SELECT *, 1, ?" "x"]))

(deftest test-select-column
  (sql= (sql/select db [:created-at]
          (sql/from :continents))
        ["SELECT \"created-at\" FROM \"continents\""]))

(deftest test-select-columns
  (sql= (sql/select db [:name :created-at]
          (sql/from :continents))
        ["SELECT \"name\", \"created-at\" FROM \"continents\""]))

(deftest test-select-column-alias
  (sql= (sql/select db [(sql/as :created-at :c)]
          (sql/from :continents))
        ["SELECT \"created-at\" AS \"c\" FROM \"continents\""]))

(deftest test-select-multiple-fns
  (sql= (sql/select db ['(greatest 1 2) '(lower "X")])
        ["SELECT greatest(1, 2), lower(?)" "X"]))

(deftest test-select-nested-fns
  (sql= (sql/select db ['(+ 1 (greatest 2 3))])
        ["SELECT (1 + greatest(2, 3))"]))

(deftest test-select-fn-alias
  (sql= (sql/select db [(sql/as '(max :created-at) :m)]
          (sql/from :continents))
        ["SELECT max(\"created-at\") AS \"m\" FROM \"continents\""]))

(deftest test-select-limit
  (sql= (sql/select db [:*]
          (sql/from :continents)
          (sql/limit 10))
        ["SELECT * FROM \"continents\" LIMIT 10"]))

(deftest test-select-limit-expr
  (sql= (sql/select db [:*]
          (sql/from :continents)
          (sql/limit '(+ 1 2)))
        ["SELECT * FROM \"continents\" LIMIT (1 + 2)"]))

(deftest test-select-limit-subquery
  (sql= (sql/select db [:*]
          (sql/from :continents)
          (sql/limit
           (sql/select db ['(count :*)]
             (sql/from :continents))))
        [(str "SELECT * FROM \"continents\" "
              "LIMIT (SELECT count(*) FROM \"continents\")")]))

(deftest test-select-limit-nil
  (sql= (sql/select db [:*]
          (sql/from :continents)
          (sql/limit nil))
        ["SELECT * FROM \"continents\""]))

(deftest test-select-offset
  (sql= (sql/select db [:*]
          (sql/from :continents)
          (sql/offset 15))
        ["SELECT * FROM \"continents\" OFFSET 15"]))

(deftest test-select-offset-expr
  (sql= (sql/select db [:*]
          (sql/from :continents)
          (sql/offset '(+ 1 2)))
        ["SELECT * FROM \"continents\" OFFSET (1 + 2)"]))

(deftest test-select-offset-subquery
  (sql= (sql/select db [:*]
          (sql/from :continents)
          (sql/offset
           (sql/select db ['(count :*)]
             (sql/from :continents))))
        [(str "SELECT * FROM \"continents\" "
              "OFFSET (SELECT count(*) FROM \"continents\")")]))

(deftest test-select-limit-offset
  (sql= (sql/select db [:*]
          (sql/from :continents)
          (sql/limit 10)
          (sql/offset 20))
        ["SELECT * FROM \"continents\" LIMIT 10 OFFSET 20"]))

(deftest test-select-column-max
  (sql= (sql/select db ['(max :created-at)]
          (sql/from :continents))
        ["SELECT max(\"created-at\") FROM \"continents\""]))

(deftest test-select-distinct-subquery-alias
  (sql= (sql/select db (sql/distinct [:x.a :x.b])
          (sql/from (sql/as (sql/select db [(sql/as 1 :a) (sql/as 2 :b)]) :x)))
        [(str "SELECT DISTINCT \"x\".\"a\", \"x\".\"b\" "
              "FROM (SELECT 1 AS \"a\", 2 AS \"b\") AS \"x\"")]))

(deftest test-select-distinct-on-subquery-alias
  (sql= (sql/select db (sql/distinct [:x.a :x.b] :on [:x.a :x.b])
          (sql/from (sql/as (sql/select db [(sql/as 1 :a) (sql/as 2 :b)]) :x)))
        [(str "SELECT DISTINCT ON (\"x\".\"a\", \"x\".\"b\") \"x\".\"a\", "
              "\"x\".\"b\" FROM (SELECT 1 AS \"a\", 2 AS \"b\") AS \"x\"")]))

(deftest test-select-most-recent-weather-report
  (sql= (sql/select db (sql/distinct [:location :time :report] :on [:location])
          (sql/from :weather-reports)
          (sql/order-by :location (sql/desc :time)))
        [(str "SELECT DISTINCT ON "
              "(\"location\") \"location\", \"time\", \"report\" "
              "FROM \"weather-reports\" ORDER BY \"location\", \"time\" "
              "DESC")]))

(deftest test-select-order-by-asc
  (sql= (sql/select db [:*]
          (sql/from :continents)
          (sql/order-by (sql/asc :created-at)))
        ["SELECT * FROM \"continents\" ORDER BY \"created-at\" ASC"]))

(deftest test-select-order-by-asc-expr
  (sql= (sql/select db [:*]
          (sql/from :weather.datasets)
          (sql/order-by
           (sql/desc '(abs (* (st_scalex :rast)
                              (st_scaley :rast))))))
        [(str "SELECT * FROM \"weather\".\"datasets\" ORDER BY "
              "abs((st_scalex(\"rast\") * st_scaley(\"rast\"))) DESC")]))

(deftest test-select-order-by-desc
  (sql= (sql/select db [:*]
          (sql/from :continents)
          (sql/order-by (sql/desc :created-at)))
        ["SELECT * FROM \"continents\" ORDER BY \"created-at\" DESC"]))

(deftest test-select-order-by-nulls-first
  (sql= (sql/select db [:*]
          (sql/from :continents)
          (sql/order-by (sql/nulls :created-at :first)))
        ["SELECT * FROM \"continents\" ORDER BY \"created-at\" NULLS FIRST"]))

(deftest test-select-order-by-nulls-last
  (sql= (sql/select db [:*]
          (sql/from :continents)
          (sql/order-by (sql/nulls :created-at :last)))
        ["SELECT * FROM \"continents\" ORDER BY \"created-at\" NULLS LAST"]))

(deftest test-select-order-by-if-true
  (sql= (let [opts {:order-by :name}]
          (sql/select db [:*]
            (sql/from :continents)
            (if (:order-by opts)
              (sql/order-by (:order-by opts)))))
        ["SELECT * FROM \"continents\" ORDER BY \"name\""]))

(deftest test-select-order-by-if-false
  (sql= (let [opts {}]
          (sql/select db [:*]
            (sql/from :continents)
            (if (:order-by opts)
              (sql/order-by (:order-by opts)))))
        ["SELECT * FROM \"continents\""]))

(deftest test-select-order-by-nil
  (sql= (sql/select db [:*]
          (sql/from :continents)
          (sql/order-by nil))
        ["SELECT * FROM \"continents\""]))

(deftest test-select-1-where-1-is-1
  (sql= (sql/select db [1]
          (sql/where '(= 1 1)))
        ["SELECT 1 WHERE (1 = 1)"]))

(deftest test-select-1-where-1-is-2-is-3
  (sql= (sql/select db [1]
          (sql/where '(= 1 2 3)))
        ["SELECT 1 WHERE (1 = 2) AND (2 = 3)"]))

(deftest test-select-subquery-alias
  (sql= (sql/select db [:*]
          (sql/from (sql/as (sql/select db [1 2 3]) :x)))
        ["SELECT * FROM (SELECT 1, 2, 3) AS \"x\""]))

(deftest test-select-subqueries-alias
  (sql= (sql/select db [:*]
          (sql/from (sql/as (sql/select db [1]) :x)
                    (sql/as (sql/select db [2]) :y)))
        ["SELECT * FROM (SELECT 1) AS \"x\", (SELECT 2) AS \"y\""]))

(deftest test-select-parition-by
  (sql= (sql/select db [:id '((lag :close)
                              over
                              (partition by :company-id order by :date desc))]
          (sql/from :quotes))
        [(str "SELECT \"id\", lag(\"close\") over "
              "(partition by \"company-id\" order by \"date\" desc) "
              "FROM \"quotes\"")]))

(deftest test-select-total-return
  (sql= (sql/select db [:id '(/ :close
                                (- ((lag :close)
                                    over
                                    (partition by :company-id
                                               order by :date desc)) 1))]
          (sql/from :quotes))
        [(str "SELECT \"id\", (\"close\" / (lag(\"close\") "
              "over (partition by \"company-id\" order by \"date\" desc) - 1)) "
              "FROM \"quotes\"")]))

(deftest test-select-total-return-alias
  (sql= (sql/select db [:id (sql/as
                             '(/ :close
                                 (- ((lag :close)
                                     over
                                     (partition
                                      by :company-id
                                      order by :date desc)) 1))
                             :daily-return)]
          (sql/from :quotes))
        [(str "SELECT \"id\", (\"close\" / (lag(\"close\") over "
              "(partition by \"company-id\" order by \"date\" desc) - 1)) "
              "AS \"daily-return\" FROM \"quotes\"")]))

(deftest test-select-group-by-a-order-by-1
  (sql= (sql/select db [:a '(max :b)]
          (sql/from :table-1)
          (sql/group-by :a)
          (sql/order-by 1))
        ["SELECT \"a\", max(\"b\") FROM \"table-1\" GROUP BY \"a\" ORDER BY 1"]))

(deftest test-select-order-by-query-select
  (sql= (sql/select db [:a :b]
          (sql/from :table-1)
          (sql/order-by '(+ :a :b) :c))
        ["SELECT \"a\", \"b\" FROM \"table-1\" ORDER BY (\"a\" + \"b\"), \"c\""]))

(deftest test-select-order-by-sum
  (sql= (sql/select db [(sql/as '(+ :a :b) :sum) :c]
          (sql/from :table-1)
          (sql/order-by :sum))
        ["SELECT (\"a\" + \"b\") AS \"sum\", \"c\" FROM \"table-1\" ORDER BY \"sum\""]))

(deftest test-select-setval
  (sql= (sql/select db [`(setval
                          :continent-id-seq
                          ~(sql/select db [`(max :id)]
                             (sql/from :continents)))])
        [(str "SELECT setval(\"continent-id-seq\", "
              "(SELECT max(\"id\") FROM \"continents\"))")]))

(deftest test-select-regex-match
  (sql= (sql/select db [:id :symbol :quote]
          (sql/from :quotes)
          (sql/where `(~(symbol "~")
                       "$AAPL"
                       (concat "(^|\\s)\\$" :symbol "($|\\s)"))))
        [(str "SELECT \"id\", \"symbol\", \"quote\" FROM \"quotes\" "
              "WHERE (? ~ concat(?, \"symbol\", ?))")
         "$AAPL" "(^|\\s)\\$" "($|\\s)"]))

(deftest test-select-join-on-columns
  (sql= (sql/select db [:*]
          (sql/from :countries)
          (sql/join :continents
                    '(on (= :continents.id
                            :countries.continent-id))))
        [(str "SELECT * FROM \"countries\" JOIN \"continents\" ON "
              "(\"continents\".\"id\" = \"countries\".\"continent-id\")")]))

(deftest test-select-join-with-keywords
  (sql= (sql/select db [:*]
          (sql/from :continents)
          (sql/join :countries.continent-id :continents.id))
        [(str "SELECT * FROM \"continents\" "
              "JOIN \"countries\" "
              "ON (\"countries\".\"continent-id\" = \"continents\".\"id\")")]))

(deftest test-select-join-on-columns-alias
  (sql= (sql/select db [:*]
          (sql/from (sql/as :countries :c))
          (sql/join :continents '(on (= :continents.id :c.continent-id))))
        [(str "SELECT * FROM \"countries\" \"c\" JOIN \"continents\" ON "
              "(\"continents\".\"id\" = \"c\".\"continent-id\")")]))

(deftest test-select-join-using-column
  (sql= (sql/select db [:*]
          (sql/from :countries)
          (sql/join :continents '(using :id)))
        ["SELECT * FROM \"countries\" JOIN \"continents\" USING (\"id\")"]))

(deftest test-select-join-using-columns
  (sql= (sql/select db [:*]
          (sql/from :countries)
          (sql/join :continents '(using :id :created-at)))
        ["SELECT * FROM \"countries\" JOIN \"continents\" USING (\"id\", \"created-at\")"]))

(deftest test-select-join-alias
  (sql= (sql/select db [:*]
          (sql/from (sql/as :countries :c))
          (sql/join :continents '(on (= :continents.id :c.continent-id))))
        [(str "SELECT * FROM \"countries\" \"c\" JOIN \"continents\" "
              "ON (\"continents\".\"id\" = \"c\".\"continent-id\")")]))

(deftest test-select-join-syntax-quote
  (sql= (sql/select db [:*]
          (sql/from (sql/as :countries :c))
          (sql/join :continents `(on (= :continents.id :c.continent-id))))
        [(str "SELECT * FROM \"countries\" \"c\" JOIN \"continents\" ON "
              "(\"continents\".\"id\" = \"c\".\"continent-id\")")]))

(deftest test-select-join-subselect-alias
  (sql= (sql/select db [:quotes.* :start-date]
          (sql/from :quotes)
          (sql/join (sql/as (sql/select db [:company-id
                                            (sql/as '(min :date) :start-date)]
                              (sql/from :quotes)
                              (sql/group-by :company-id))
                            :start-dates)
                    '(on (and (= :quotes.company-id :start-dates.company-id)
                              (= :quotes.date :start-dates.start-date)))))
        [(str "SELECT \"quotes\".*, \"start-date\" FROM \"quotes\" JOIN "
              "(SELECT \"company-id\", min(\"date\") AS \"start-date\" "
              "FROM \"quotes\" GROUP BY \"company-id\") AS \"start-dates\" ON "
              "((\"quotes\".\"company-id\" = \"start-dates\".\"company-id\") "
              "and (\"quotes\".\"date\" = \"start-dates\".\"start-date\"))")]))

(deftest test-select-except
  (sql= (sql/except
         (sql/select db [1])
         (sql/select db [2]))
        ["SELECT 1 EXCEPT SELECT 2"]))

(deftest test-select-except-all
  (sql= (sql/except
         {:all true}
         (sql/select db [1])
         (sql/select db [2]))
        ["SELECT 1 EXCEPT ALL SELECT 2"]))

(deftest test-select-intersect
  (sql= (sql/intersect
         (sql/select db [1])
         (sql/select db [2]))
        ["SELECT 1 INTERSECT SELECT 2"]))

(deftest test-select-intersect-all
  (sql= (sql/intersect
         {:all true}
         (sql/select db [1])
         (sql/select db [2]))
        ["SELECT 1 INTERSECT ALL SELECT 2"]))

(deftest test-select-union
  (sql= (sql/union
         (sql/select db [1])
         (sql/select db [2]))
        ["SELECT 1 UNION SELECT 2"]))

(deftest test-select-union-all
  (sql= (sql/union
         {:all true}
         (sql/select db [1])
         (sql/select db [2]))
        ["SELECT 1 UNION ALL SELECT 2"]))

(deftest test-select-where-combine-and-1
  (sql= (sql/select db [1]
          (sql/where '(= 1 1) :and))
        ["SELECT 1 WHERE (1 = 1)"]))

(deftest test-select-where-combine-and-2
  (sql= (sql/select db [1]
          (sql/where '(= 1 1))
          (sql/where '(= 2 2) :and))
        ["SELECT 1 WHERE ((1 = 1) and (2 = 2))"]))

(deftest test-select-where-combine-and-3
  (sql= (sql/select db [1]
          (sql/where '(= 1 1))
          (sql/where '(= 2 2) :and)
          (sql/where '(= 3 3) :and))
        ["SELECT 1 WHERE (((1 = 1) and (2 = 2)) and (3 = 3))"]))

(deftest test-select-where-combine-or-1
  (sql= (sql/select db [1]
          (sql/where '(= 1 1) :or))
        ["SELECT 1 WHERE (1 = 1)"]))

(deftest test-select-where-combine-or-2
  (sql= (sql/select db [1]
          (sql/where '(= 1 1))
          (sql/where '(= 2 2) :or))
        ["SELECT 1 WHERE ((1 = 1) or (2 = 2))"]))

(deftest test-select-where-combine-or-3
  (sql= (sql/select db [1]
          (sql/where '(= 1 1))
          (sql/where '(= 2 2) :or)
          (sql/where '(= 3 3) :or))
        ["SELECT 1 WHERE (((1 = 1) or (2 = 2)) or (3 = 3))"]))

(deftest test-select-where-combine-mixed
  (sql= (sql/select db [1]
          (sql/where '(= 1 1))
          (sql/where '(= 2 2) :and)
          (sql/where '(= 3 3) :or))
        ["SELECT 1 WHERE (((1 = 1) and (2 = 2)) or (3 = 3))"]))

(deftest test-substring-from-to
  (sql= (sql/select db ['(substring "Thomas" from 2 for 3)])
        ["SELECT substring(? from 2 for 3)" "Thomas"]))

(deftest test-substring-from-to-lower
  (sql= (sql/select db ['(lower (substring "Thomas" from 2 for 3))])
        ["SELECT lower(substring(? from 2 for 3))" "Thomas"]))

(deftest test-substring-from-pattern
  (sql= (sql/select db ['(substring "Thomas" from "...$")])
        ["SELECT substring(? from ?)" "Thomas" "...$"]))

(deftest test-substring-from-pattern-for-escape
  (sql= (sql/select db ['(substring "Thomas" from "%##\"o_a#\"_" for "#")])
        ["SELECT substring(? from ? for ?)" "Thomas" "%##\"o_a#\"_" "#"]))

(deftest test-trim
  (sql= (sql/select db ['(trim both "x" from "xTomxx")])
        ["SELECT trim(both ? from ?)" "x" "xTomxx"]))

(deftest test-select-from-fn
  (sql= (sql/select db [:*] (sql/from '(generate_series 0 10)))
        ["SELECT * FROM generate_series(0, 10)"]))

(deftest test-select-from-fn-alias
  (sql= (sql/select db [:n] (sql/from (sql/as '(generate_series 0 200) :n)))
        ["SELECT \"n\" FROM generate_series(0, 200) AS \"n\""]))

(deftest test-select-qualified-column
  (sql= (sql/select db [{:op :column :table :continents :name :id}]
          (sql/from :continents))
        ["SELECT \"continents\".\"id\" FROM \"continents\""]))

(deftest test-select-qualified-keyword-column
  (sql= (sql/select db [:continents.id] (sql/from :continents))
        ["SELECT \"continents\".\"id\" FROM \"continents\""]))

;; QUOTING

(deftest test-db-specifiy-quoting
  (are [db expected]
      (= (sql/sql (sql/select db [:continents.id]
                    (sql/from :continents)))
         expected)
    (db/db :mysql)
    ["SELECT `continents`.`id` FROM `continents`"]
    (db/db :postgresql)
    ["SELECT \"continents\".\"id\" FROM \"continents\""]
    (db/db :oracle)
    ["SELECT \"continents\".\"id\" FROM \"continents\""]
    (db/db :vertica)
    ["SELECT \"continents\".\"id\" FROM \"continents\""]))

;; POSTGRESQL ARRAYS

(deftest test-array
  (sql= (sql/select db [[1 2]])
        ["SELECT ARRAY[1, 2]"]))

(deftest test-array-concat
  (sql= (sql/select db ['(|| [1 2] [3 4] [5 6])])
        ["SELECT (ARRAY[1, 2] || ARRAY[3, 4] || ARRAY[5, 6])"]))

(deftest test-select-array-contains
  (sql= (sql/select db [`(~(keyword "@>") [1 2] [3 4])])
        ["SELECT (ARRAY[1, 2] @> ARRAY[3, 4])"]))

(deftest test-select-array-contained
  (sql= (sql/select db [`(~(keyword "<@") [1 2] [3 4])])
        ["SELECT (ARRAY[1, 2] <@ ARRAY[3, 4])"]))

;; POSTGRESQL FULLTEXT

(deftest test-cast-as-document-1
  (sql= (sql/select db ['(cast (:|| :title " " :author " "
                                    :abstract " " :body)
                               :document)]
          (sql/from :messages)
          (sql/where '(= :mid 12)))
        [(str "SELECT CAST((\"title\" || ? || \"author\" || ? || \"abstract\" "
              "|| ? || \"body\") AS document) FROM \"messages\" "
              "WHERE (\"mid\" = 12)")
         " " " " " "]))

(deftest test-cast-as-document-2
  (sql= (sql/select db ['(cast (:|| :m.title " " :m.author " "
                                    :m.abstract " " :d.body)
                               :document)]
          (sql/from (sql/as :messages :m) (sql/as :docs :d))
          (sql/where '(and (= :mid :did)
                           (= :mid 12))))
        [(str "SELECT CAST((\"m\".\"title\" || ? || \"m\".\"author\" || ? || "
              "\"m\".\"abstract\" || ? || \"d\".\"body\") AS document) FROM "
              "\"messages\" \"m\", \"docs\" \"d\" WHERE ((\"mid\" = \"did\") "
              "and (\"mid\" = 12))")
         " " " " " "]))

(deftest test-basic-text-matching-1
  (sql= (sql/select db [`(~(keyword "@@")
                          (cast "a fat cat sat on a mat and ate a fat rat"
                                :tsvector)
                          (cast "rat & cat" :tsquery))])
        ["SELECT (CAST(? AS tsvector) @@ CAST(? AS tsquery))"
         "a fat cat sat on a mat and ate a fat rat" "rat & cat"]))

(deftest test-basic-text-matching-2
  (sql= (sql/select db [`(~(keyword "@@")
                          (cast "fat & cow" :tsquery)
                          (cast "a fat cat sat on a mat and ate a fat rat" :tsvector))])
        ["SELECT (CAST(? AS tsquery) @@ CAST(? AS tsvector))"
         "fat & cow" "a fat cat sat on a mat and ate a fat rat"]))

(deftest test-basic-text-matching-3
  (sql= (sql/select db [`(~(keyword "@@")
                          (to_tsvector "fat cats ate fat rats")
                          (to_tsquery "fat & rat"))])
        ["SELECT (to_tsvector(?) @@ to_tsquery(?))"
         "fat cats ate fat rats" "fat & rat"]))

(deftest test-basic-text-matching-4
  (sql= (sql/select db [`(~(keyword "@@")
                          (cast "fat cats ate fat rats" :tsvector)
                          (to_tsquery "fat & rat"))])
        ["SELECT (CAST(? AS tsvector) @@ to_tsquery(?))"
         "fat cats ate fat rats" "fat & rat"]))

(deftest test-searching-a-table-1
  (sql= (sql/select db [:title]
          (sql/from :pgweb)
          (sql/where `(~(keyword "@@")
                       (to_tsvector "english" :body)
                       (to_tsquery "english" "friend"))))
        [(str "SELECT \"title\" FROM \"pgweb\" WHERE "
              "(to_tsvector(?, \"body\") @@ to_tsquery(?, ?))")
         "english" "english" "friend"]))

(deftest test-searching-a-table-2
  (sql= (sql/select db [:title]
          (sql/from :pgweb)
          (sql/where `(~(keyword "@@")
                       (to_tsvector :body)
                       (to_tsquery "friend"))))
        [(str "SELECT \"title\" FROM \"pgweb\" WHERE "
              "(to_tsvector(\"body\") @@ to_tsquery(?))")
         "friend"]))

(deftest test-searching-a-table-3
  (sql= (sql/select db [:title]
          (sql/from :pgweb)
          (sql/where `(~(keyword "@@")
                       (to_tsvector (:|| :title " " :body))
                       (to_tsquery "create & table")))
          (sql/order-by (sql/desc :last-mod-date))
          (sql/limit 10))
        [(str "SELECT \"title\" FROM \"pgweb\" WHERE "
              "(to_tsvector((\"title\" || ? || \"body\")) @@ to_tsquery(?)) "
              "ORDER BY \"last-mod-date\" DESC LIMIT 10")
         " " "create & table"]))

(deftest test-case
  (sql= (sql/select db [:a '(case (= :a 1) "one"
                                  (= :a 2) "two"
                                  "other")]
          (sql/from :test))
        [(str "SELECT \"a\", CASE WHEN (\"a\" = 1) THEN ?"
              " WHEN (\"a\" = 2) THEN ? "
              "ELSE ? END FROM \"test\"")
         "one" "two" "other"]))

(deftest test-case-as-alias
  (sql= (sql/select db [:a (sql/as '(case (= :a 1) "one"
                                          (= :a 2) "two"
                                          "other") :c)]
          (sql/from :test))
        [(str "SELECT \"a\", CASE WHEN (\"a\" = 1) THEN ?"
              " WHEN (\"a\" = 2) THEN ? "
              "ELSE ? END AS \"c\" FROM \"test\"")
         "one" "two" "other"]))

(deftest test-full-outer-join
  (sql= (sql/select db [:a]
          (sql/from :test1)
          (sql/join :test2
                    '(on (= :test1.b :test2.b))
                    :type :full :outer true))
        [(str "SELECT \"a\" FROM \"test1\""
              " FULL OUTER JOIN \"test2\" ON (\"test1\".\"b\" = \"test2\".\"b\")")]))

;; Window functions: http://www.postgresql.org/docs/9.4/static/tutorial-window.html

(deftest test-window-compare-salaries
  (sql= (sql/select db [:depname :empno :salary
                        '(over (avg :salary) (partition-by :depname))]
          (sql/from :empsalary))
        [(str "SELECT \"depname\", \"empno\", \"salary\", avg(\"salary\") "
              "OVER (PARTITION BY \"depname\") FROM \"empsalary\"")]))

(deftest test-window-compare-salaries-by-year
  (sql= (sql/select db [:year :depname :empno :salary
                        '(over (avg :salary) (partition-by [:year :depname]))]
          (sql/from :empsalary))
        [(str "SELECT \"year\", \"depname\", \"empno\", \"salary\", "
              "avg(\"salary\") OVER (PARTITION BY \"year\", \"depname\") "
              "FROM \"empsalary\"")]))

(deftest test-window-rank-over-order-by
  (sql= (sql/select db [:depname :empno :salary
                        '(over (rank)
                               (partition-by
                                :depname
                                (sql/order-by (desc :salary))))]
          (sql/from :empsalary))
        [(str "SELECT \"depname\", \"empno\", \"salary\", rank() OVER "
              "(PARTITION BY \"depname\" ORDER BY \"salary\" DESC) "
              "FROM \"empsalary\"")]))

(deftest test-window-rank-over-multiple-cols-order-by
  (sql= (sql/select db [:year :depname :empno :salary
                        '(over (rank)
                               (partition-by [:year :depname]
                                             (sql/order-by (desc :salary))))]
          (sql/from :empsalary))
        [(str "SELECT \"year\", \"depname\", \"empno\", \"salary\", rank() "
              "OVER (PARTITION BY \"year\", \"depname\" ORDER BY "
              "\"salary\" DESC) FROM \"empsalary\"")]))

(deftest test-window-over-empty
  (sql= (sql/select db [:salary '(over (sum :salary))]
          (sql/from :empsalary))
        ["SELECT \"salary\", sum(\"salary\") OVER () FROM \"empsalary\""]))

(deftest test-window-sum-over-order-by
  (sql= (sql/select db [:salary '(over (sum :salary) (sql/order-by :salary))]
          (sql/from :empsalary))
        [(str "SELECT \"salary\", sum(\"salary\") OVER (ORDER BY \"salary\") "
              "FROM \"empsalary\"")]))

(deftest test-window-rank-over-partition-by
  (sql= (sql/select db [:depname :empno :salary :enroll-date]
          (sql/from
           (sql/as
            (sql/select db [:depname :empno :salary :enroll-date
                            (sql/as
                             '(over (rank)
                                    (partition-by
                                     :depname
                                     (sql/order-by (desc :salary) :empno)))
                             :pos)]
              (sql/from :empsalary))
            :ss))
          (sql/where '(< pos 3)))
        [(str "SELECT \"depname\", \"empno\", \"salary\", \"enroll-date\" "
              "FROM (SELECT \"depname\", \"empno\", \"salary\", \"enroll-date\", "
              "rank() OVER (PARTITION BY \"depname\" ORDER BY \"salary\" "
              "DESC, \"empno\") AS \"pos\" FROM \"empsalary\") AS \"ss\" "
              "WHERE (pos < 3)")]))

(deftest test-window-alias
  (sql= (sql/select db ['(over (sum :salary) :w)
                        '(over (avg :salary) :w)]
          (sql/from :empsalary)
          (sql/window
           (sql/as '(partition-by
                     :depname (sql/order-by (sql/desc salary))) :w)))
        [(str "SELECT sum(\"salary\") OVER (\"w\"), "
              "avg(\"salary\") OVER (\"w\") "
              "FROM \"empsalary\" "
              "WINDOW \"w\" AS (PARTITION BY \"depname\" ORDER BY salary DESC)")]))

(deftest test-window-alias-order-by
  (sql= (sql/select db [(sql/as '(over (sum :salary) :w) :sum)]
          (sql/from :empsalary)
          (sql/order-by :sum)
          (sql/window
           (sql/as '(partition-by
                     :depname (sql/order-by (sql/desc salary))) :w)))
        [(str "SELECT sum(\"salary\") OVER (\"w\") AS \"sum\" "
              "FROM \"empsalary\" "
              "WINDOW \"w\" AS (PARTITION BY \"depname\" ORDER BY salary DESC) "
              "ORDER BY \"sum\"")]))

(deftest test-not-expr
  (sql= (sql/select db [:*]
          (sql/where `(not (= :id 1))))
        ["SELECT * WHERE (NOT (\"id\" = 1))"]))

(deftest test-slash-in-function-name
  (sql= (sql/select db [`("m/s->km/h" 10)])
        ["SELECT \"m/s->km/h\"(10)"]))

;; ATTRIBUTES OF COMPOSITE TYPES

(deftest test-attr-composite-type
  (sql= (sql/select db [(sql/as '(.-name (new-emp)) :x)])
        ["SELECT (\"new-emp\"()).\"name\" AS \"x\""]))

(deftest test-nested-attr-composite-type
  (sql= (sql/select db [(sql/as '(.-first (.-name (new-emp))) :x)])
        ["SELECT ((\"new-emp\"()).\"name\").\"first\" AS \"x\""]))

(deftest test-select-as-alias
  (sql= (sql/select db [(sql/as
                         (sql/select db ['(count :*)]
                           (sql/from :continents))
                         :continents)
                        (sql/as
                         (sql/select db ['(count :*)]
                           (sql/from :countries))
                         :countries)])
        [(str "SELECT (SELECT count(*) FROM \"continents\") AS \"continents\", "
              "(SELECT count(*) FROM \"countries\") AS \"countries\"")]))

(deftest test-having
  (sql= (sql/select db [:city '(max :temp-lo)]
          (sql/from :weather)
          (sql/group-by :city)
          (sql/having '(< (max :temp-lo) 40)))
        [(str "SELECT \"city\", max(\"temp-lo\") FROM \"weather\" GROUP BY "
              "\"city\" HAVING (max(\"temp-lo\") < 40)")]))

;; Aggregate Expressions

(deftest test-select-count-as
  (sql= (sql/select db [(sql/as '(count :*) :count)]
          (sql/from :tweets))
        ["SELECT count(*) AS \"count\" FROM \"tweets\""]))

(deftest test-select-count-distinct
  (sql= (sql/select db ['(count distinct :user-id)]
          (sql/from :tweets))
        ["SELECT count(DISTINCT \"user-id\") FROM \"tweets\""]))

(deftest test-select-count-distinct-upper-case
  (sql= (sql/select db ['(count DISTINCT :user-id)]
          (sql/from :tweets))
        ["SELECT count(DISTINCT \"user-id\") FROM \"tweets\""]))

(deftest test-select-count-all
  (sql= (sql/select db ['(count all :user-id)]
          (sql/from :tweets))
        ["SELECT count(ALL \"user-id\") FROM \"tweets\""]))

(deftest test-select-count-all-upper-case
  (sql= (sql/select db ['(count ALL :user-id)]
          (sql/from :tweets))
        ["SELECT count(ALL \"user-id\") FROM \"tweets\""]))

(deftest test-select-array-agg-order-by-desc
  (sql= (sql/select db ['(array_agg :a (order-by (desc :b)))]
          (sql/from :table))
        ["SELECT array_agg(\"a\" ORDER BY \"b\" DESC) FROM \"table\""]))

(deftest test-select-string-agg-order-by
  (sql= (sql/select db ['(string_agg :a "," (order-by :a))]
          (sql/from :table))
        ["SELECT string_agg(\"a\", ? ORDER BY \"a\") FROM \"table\"" ","]))

(deftest test-select-order-by-distance-bounds
  (sql= (sql/select db [:*]
          (sql/from :x)
          (sql/order-by '(<#> :x.a :x.b)))
        ["SELECT * FROM \"x\" ORDER BY (\"x\".\"a\" <#> \"x\".\"b\")"]))

(deftest test-select-order-by-distance-centroids
  (sql= (sql/select db [:*]
          (sql/from :x)
          (sql/order-by '(<-> :x.a :x.b)))
        ["SELECT * FROM \"x\" ORDER BY (\"x\".\"a\" <-> \"x\".\"b\")"]))

(deftest test-select-qualified
  (sql= (sql/select db [(sql/as :spots.id :spot/id)]
          (sql/from :spots))
        ["SELECT \"spots\".\"id\" AS \"spot/id\" FROM \"spots\""]))

(deftest test-select->-number
  (sql= (sql/select db [`(-> (cast "[1,2,3]" :json) 2)])
        ["SELECT CAST(? AS json)->2" "[1,2,3]"]))

(deftest test-select->-string
  (sql= (sql/select db [`(-> (cast "{\"a\":1, \"b\": 2}" :json) "b")])
        ["SELECT CAST(? AS json)->?" "{\"a\":1, \"b\": 2}" "b"]))

(deftest test-select->-alias
  (sql= (sql/select db [(sql/as `(-> (cast "[1,2,3]" :json) 2) :x)])
        ["SELECT CAST(? AS json)->2 AS \"x\"" "[1,2,3]"]))

(deftest test-select->-nested
  (sql= (sql/select db [`(-> (cast "{\"a\":1, \"c\": {\"d\": 1}}" :json) "c" "d")])
        ["SELECT CAST(? AS json)->?->?"
         "{\"a\":1, \"c\": {\"d\": 1}}"
         "c" "d"]))

(deftest test-select->>-number
  (sql= (sql/select db [`(->> (cast "[1,2,3]" :json) 2)])
        ["SELECT CAST(? AS json)->>2" "[1,2,3]"]))

(deftest test-select->>-string
  (sql= (sql/select db [`(->> (cast "{\"a\":1, \"b\": 2}" :json) "b")])
        ["SELECT CAST(? AS json)->>?" "{\"a\":1, \"b\": 2}" "b"]))
