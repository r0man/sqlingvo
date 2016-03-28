(ns sqlingvo.select-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.test :refer :all]
            [sqlingvo.core :refer :all]
            [sqlingvo.db :as db]
            [sqlingvo.expr :refer :all]
            [sqlingvo.test :refer [db sql= with-stmt]])
  (:import java.util.Date))

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
    ["SELECT greatest(1, 2), lower(?)" "X"]
    (select db ['(greatest 1 2) '(lower "X")])
    (is (= :select (:op stmt)))
    (is (= (map parse-expr ['(greatest 1 2) '(lower "X")]) (:exprs stmt)))))

(deftest test-select-nested-fns
  (with-stmt
    ["SELECT (1 + greatest(2, 3))"]
    (select db ['(+ 1 (greatest 2 3))])
    (is (= :select (:op stmt)))
    (is (= [(parse-expr '(+ 1 (greatest 2 3)))] (:exprs stmt)))))

(deftest test-select-fn-alias
  (with-stmt
    ["SELECT max(\"created-at\") AS \"m\" FROM \"continents\""]
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
    ["SELECT max(\"created-at\") FROM \"continents\""]
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
    ["SELECT * FROM \"weather\".\"datasets\" ORDER BY abs((st_scalex(\"rast\") * st_scaley(\"rast\"))) DESC"]
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
    ["SELECT \"id\", lag(\"close\") over (partition by \"company-id\" order by \"date\" desc) FROM \"quotes\""]
    (select db [:id '((lag :close) over (partition by :company-id order by :date desc))]
      (from :quotes))
    (is (= :select (:op stmt)))
    (is (= (map parse-expr [:id '((lag :close) over (partition by :company-id order by :date desc))])
           (:exprs stmt)))
    (is (= [(parse-from :quotes)] (:from stmt)))))

(deftest test-select-total-return
  (with-stmt
    ["SELECT \"id\", (\"close\" / (lag(\"close\") over (partition by \"company-id\" order by \"date\" desc) - 1)) FROM \"quotes\""]
    (select db [:id '(/ :close (- ((lag :close) over (partition by :company-id order by :date desc)) 1))]
      (from :quotes))
    (is (= :select (:op stmt)))
    (is (= (map parse-expr [:id '(/ :close (- ((lag :close) over (partition by :company-id order by :date desc)) 1))])
           (:exprs stmt)))
    (is (= [(parse-from :quotes)] (:from stmt)))))

(deftest test-select-total-return-alias
  (with-stmt
    ["SELECT \"id\", (\"close\" / (lag(\"close\") over (partition by \"company-id\" order by \"date\" desc) - 1)) AS \"daily-return\" FROM \"quotes\""]
    (select db [:id (as '(/ :close (- ((lag :close) over (partition by :company-id order by :date desc)) 1)) :daily-return)]
      (from :quotes))
    (is (= :select (:op stmt)))
    (is (= (map parse-expr [:id (as '(/ :close (- ((lag :close) over (partition by :company-id order by :date desc)) 1)) :daily-return)])
           (:exprs stmt)))
    (is (= [(parse-from :quotes)] (:from stmt)))))

(deftest test-select-group-by-a-order-by-1
  (with-stmt
    ["SELECT \"a\", max(\"b\") FROM \"table-1\" GROUP BY \"a\" ORDER BY 1"]
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
    ["SELECT setval(\"continent-id-seq\", (SELECT max(\"id\") FROM \"continents\"))"]
    (select db [`(setval :continent-id-seq ~(select db [`(max :id)] (from :continents)))])
    (is (= :select (:op stmt)))
    (is (= (map parse-expr [`(setval :continent-id-seq ~(select db [`(max :id)] (from :continents)))])
           (:exprs stmt)))))

(deftest test-select-regex-match
  (with-stmt
    ["SELECT \"id\", \"symbol\", \"quote\" FROM \"quotes\" WHERE (? ~ concat(?, \"symbol\", ?))" "$AAPL" "(^|\\s)\\$" "($|\\s)"]
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
    [(str "SELECT \"quotes\".*, \"start-date\" FROM \"quotes\" JOIN (SELECT \"company-id\", min(\"date\") AS \"start-date\" "
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
    ["SELECT lower(substring(? from 2 for 3))" "Thomas"]
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
    ["SELECT * FROM generate_series(0, 10)"]
    (select db [*] (from '(generate_series 0 10)))))

(deftest test-select-from-fn-alias
  (with-stmt
    ["SELECT \"n\" FROM generate_series(0, 200) AS \"n\""]
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
    ["SELECT (to_tsvector(?) @@ to_tsquery(?))" "fat cats ate fat rats" "fat & rat"]
    (select db [`(~(keyword "@@")
                  (to_tsvector "fat cats ate fat rats")
                  (to_tsquery "fat & rat"))])))

(deftest test-basic-text-matching-4
  (with-stmt
    ["SELECT (CAST(? AS tsvector) @@ to_tsquery(?))" "fat cats ate fat rats" "fat & rat"]
    (select db [`(~(keyword "@@")
                  (cast "fat cats ate fat rats" :tsvector)
                  (to_tsquery "fat & rat"))])))

(deftest test-searching-a-table-1
  (with-stmt
    ["SELECT \"title\" FROM \"pgweb\" WHERE (to_tsvector(?, \"body\") @@ to_tsquery(?, ?))" "english" "english" "friend"]
    (select db [:title]
      (from :pgweb)
      (where `(~(keyword "@@")
               (to_tsvector "english" :body)
               (to_tsquery "english" "friend"))))))

(deftest test-searching-a-table-2
  (with-stmt
    ["SELECT \"title\" FROM \"pgweb\" WHERE (to_tsvector(\"body\") @@ to_tsquery(?))" "friend"]
    (select db [:title]
      (from :pgweb)
      (where `(~(keyword "@@")
               (to_tsvector :body)
               (to_tsquery "friend"))))))

(deftest test-searching-a-table-3
  (with-stmt
    ["SELECT \"title\" FROM \"pgweb\" WHERE (to_tsvector((\"title\" || ? || \"body\")) @@ to_tsquery(?)) ORDER BY \"last-mod-date\" DESC LIMIT 10" " " "create & table"]
    (select db [:title]
      (from :pgweb)
      (where `(~(keyword "@@")
               (to_tsvector (:|| :title " " :body))
               (to_tsquery "create & table")))
      (order-by (desc :last-mod-date))
      (limit 10))))

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
        ["SELECT \"depname\", \"empno\", \"salary\", avg(\"salary\") OVER (PARTITION BY \"depname\") FROM \"empsalary\""]))

(deftest test-window-compare-salaries-by-year
  (sql= (select db [:year :depname :empno :salary '(over (avg :salary) (partition-by [:year :depname]))]
          (from :empsalary))
        ["SELECT \"year\", \"depname\", \"empno\", \"salary\", avg(\"salary\") OVER (PARTITION BY \"year\", \"depname\") FROM \"empsalary\""]))

(deftest test-window-rank-over-order-by
  (sql= (select db [:depname :empno :salary '(over (rank) (partition-by :depname (order-by (desc :salary))))]
          (from :empsalary))
        ["SELECT \"depname\", \"empno\", \"salary\", rank() OVER (PARTITION BY \"depname\" ORDER BY \"salary\" DESC) FROM \"empsalary\""]))

(deftest test-window-rank-over-multiple-cols-order-by
  (sql= (select db [:year :depname :empno :salary '(over (rank) (partition-by [:year :depname] (order-by (desc :salary))))]
          (from :empsalary))
        ["SELECT \"year\", \"depname\", \"empno\", \"salary\", rank() OVER (PARTITION BY \"year\", \"depname\" ORDER BY \"salary\" DESC) FROM \"empsalary\""]))

(deftest test-window-over-empty
  (sql= (select db [:salary '(over (sum :salary))]
          (from :empsalary))
        ["SELECT \"salary\", sum(\"salary\") OVER () FROM \"empsalary\""]))

(deftest test-window-sum-over-order-by
  (sql= (select db [:salary '(over (sum :salary) (order-by :salary))]
          (from :empsalary))
        ["SELECT \"salary\", sum(\"salary\") OVER (ORDER BY \"salary\") FROM \"empsalary\""]))

(deftest test-window-rank-over-partition-by
  (sql= (select db [:depname :empno :salary :enroll-date]
          (from (as (select db [:depname :empno :salary :enroll-date
                                (as '(over (rank) (partition-by :depname (order-by (desc :salary) :empno))) :pos)]
                      (from :empsalary))
                    :ss))
          (where '(< pos 3)))
        [(str "SELECT \"depname\", \"empno\", \"salary\", \"enroll-date\" "
              "FROM (SELECT \"depname\", \"empno\", \"salary\", \"enroll-date\", "
              "rank() OVER (PARTITION BY \"depname\" ORDER BY \"salary\" DESC, \"empno\") AS \"pos\" "
              "FROM \"empsalary\") AS \"ss\" WHERE (pos < 3)")]))

(deftest test-window-alias
  (sql= (select db ['(over (sum :salary) :w)
                    '(over (avg :salary) :w)]
          (from :empsalary)
          (window (as '(partition-by
                        :depname (order-by (desc salary))) :w)))
        [(str "SELECT sum(\"salary\") OVER (\"w\"), "
              "avg(\"salary\") OVER (\"w\") "
              "FROM \"empsalary\" "
              "WINDOW \"w\" AS (PARTITION BY \"depname\" ORDER BY salary DESC)")]))

(deftest test-window-alias-order-by
  (sql= (select db [(as '(over (sum :salary) :w) :sum)]
          (from :empsalary)
          (order-by :sum)
          (window (as '(partition-by
                        :depname (order-by (desc salary))) :w)))
        [(str "SELECT sum(\"salary\") OVER (\"w\") AS \"sum\" "
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

(deftest test-having
  (sql= (select db [:city '(max :temp-lo)]
           (from :weather)
           (group-by :city)
           (having '(< (max :temp-lo) 40)))
        ["SELECT \"city\", max(\"temp-lo\") FROM \"weather\" GROUP BY \"city\" HAVING (max(\"temp-lo\") < 40)"]))


