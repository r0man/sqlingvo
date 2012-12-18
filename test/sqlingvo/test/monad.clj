(ns sqlingvo.test.monad
  (:refer-clojure :exclude [distinct group-by])
  (:use clojure.test
        clojure.pprint
        sqlingvo.compiler
        sqlingvo.util
        sqlingvo.monad))

(defmacro deftest-stmt [name sql stmt & body]
  `(deftest ~name
     (let [~'stmt ~stmt]
       (is (= ~sql (compile-stmt ~stmt)))
       ~@body)))

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

(deftest-stmt test-create-table-like-including-defaults
  ["CREATE TABLE tmp-films (LIKE films INCLUDING DEFAULTS)"]
  (create-table :tmp-films
    (like :films :including [:defaults]))
  (is (= :create-table (:op stmt)))
  (let [like (:like stmt)]
    (is (= :like (:op like)))
    (is (= (parse-table :films) (:table like)))
    (is (= [:defaults] (:including like)))))

(deftest-stmt test-create-table-like-excluding-indexes
  ["CREATE TABLE tmp-films (LIKE films EXCLUDING INDEXES)"]
  (create-table :tmp-films
    (like :films :excluding [:indexes]))
  (is (= :create-table (:op stmt)))
  (let [like (:like stmt)]
    (is (= :like (:op like)))
    (is (= (parse-table :films) (:table like)))
    (is (= [:indexes] (:excluding like)))))

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
  (is (= [(parse-expr '(<> :kind "Musical"))] (:where stmt))))

(deftest-stmt test-delete-completed-tasks-returning-all
  ["DELETE FROM tasks WHERE (status = ?) RETURNING *" "DONE"]
  (delete :tasks
    (where '(= status "DONE"))
    (returning *))
  (is (= :delete (:op stmt)))
  (is (= (parse-table :tasks) (:table stmt)))
  (is (= [(parse-expr '(= status "DONE"))] (:where stmt)))
  (is (= [(parse-expr *)] (:returning stmt))))

(deftest-stmt test-delete-films-by-producer-name
  ["DELETE FROM films WHERE (producer-id in (SELECT id FROM producers WHERE (name = ?)))" "foo"]
  (delete :films
    (where `(in :producer-id
                ~(select [:id]
                   (from :producers)
                   (where '(= name "foo"))))))
  (is (= :delete (:op stmt)))
  (is (= (parse-table :films) (:table stmt)))
  (is (= [(parse-expr `(in :producer-id
                           ~(select [:id]
                              (from :producers)
                              (where '(= name "foo")))))]
         (:where stmt))))

(deftest-stmt test-delete-quotes
  [(str "DELETE FROM quotes WHERE ((company-id = 1) and (date > (SELECT min(date) FROM import)) and "
        "(date > (SELECT max(date) FROM import)))")]
  (delete :quotes
    (where `(and (= :company-id 1)
                 (> :date ~(select ['(min :date)] (from :import)))
                 (> :date ~(select ['(max :date)] (from :import))))))
  (is (= :delete (:op stmt)))
  (is (= (parse-table :quotes) (:table stmt)))
  (is (= [(parse-expr `(and (= :company-id 1)
                            (> :date ~(select ['(min :date)] (from :import)))
                            (> :date ~(select ['(max :date)] (from :import)))))]
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

(deftest-stmt test-drop-continents-if-exists
  ["DROP TABLE IF EXISTS continents"]
  (drop-table [:continents]
    (if-exists true))
  (is (= :drop-table (:op stmt)))
  (is (= {:op :if-exists} (:if-exists stmt)))
  (is (= [(parse-table :continents)] (:tables stmt))))

(deftest-stmt test-drop-continents-countries-if-exists-restrict
  ["DROP TABLE IF EXISTS continents, countries RESTRICT"]
  (drop-table [:continents :countries]
    (if-exists true)
    (restrict true))
  (is (= :drop-table (:op stmt)))
  (is (= {:op :if-exists} (:if-exists stmt)))
  (is (= (map parse-table [:continents :countries]) (:tables stmt)))
  (is (= {:op :restrict} (:restrict stmt))))

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
  ["INSERT INTO films (did, date-prod, kind, title, code) VALUES (?, ?, ?, ?, ?)"
   106 "1961-06-16" "Drama" "Yojimbo" "T_601"]
  (insert :films []
    (values {:code "T_601" :title "Yojimbo" :did 106 :date-prod "1961-06-16" :kind "Drama"}))
  (is (= :insert (:op stmt)))
  (is (= [] (:columns stmt)))
  (is (= [{:code "T_601" :title "Yojimbo" :did 106 :date-prod "1961-06-16" :kind "Drama"}]
         (:values stmt)))
  (is (= (parse-table :films) (:table stmt))))

(deftest-stmt test-insert-single-row-as-seq
  ["INSERT INTO films (did, date-prod, kind, title, code) VALUES (?, ?, ?, ?, ?)"
   106 "1961-06-16" "Drama" "Yojimbo" "T_601"]
  (insert :films []
    (values [{:code "T_601" :title "Yojimbo" :did 106 :date-prod "1961-06-16" :kind "Drama"}]))
  (is (= :insert (:op stmt)))
  (is (= [] (:columns stmt)))
  (is (= [{:code "T_601" :title "Yojimbo" :did 106 :date-prod "1961-06-16" :kind "Drama"}]
         (:values stmt)))
  (is (= (parse-table :films) (:table stmt))))

(deftest-stmt test-insert-multi-row
  ["INSERT INTO films (did, date-prod, kind, title, code) VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)"
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

(deftest-stmt test-select-select-1
  ["SELECT (SELECT 1)"]
  (select [(select [1])])
  (is (= :select (:op stmt)))
  (is (= [(select [1])] (:exprs stmt))))

(deftest-stmt test-select-select-1-select-x
  ["SELECT (SELECT 1), (SELECT ?)" "x"]
  (select [(select [1]) (select ["x"])])
  (is (= :select (:op stmt)))
  (is (= [(select [1]) (select ["x"])] (:exprs stmt))))

(deftest-stmt test-select-where-single-arg-and
  ["SELECT 1 WHERE (1 = 1)"]
  (select [1]
    (where '(and (= 1 1))))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr 1)] (:exprs stmt)))
  (is (= [(parse-expr '(and (= 1 1)))] (:where stmt))))

(deftest-stmt test-select-less-2-arity
  ["SELECT 1 WHERE (1 < 2)"]
  (select [1]
    (where '(< 1 2)))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr 1)] (:exprs stmt)))
  (is (= [(parse-expr '(< 1 2))] (:where stmt))))

(deftest-stmt test-select-less-3-arity
  ["SELECT 1 WHERE (1 < 2) AND (2 < 3)"]
  (select [1]
    (where '(< 1 2 3)))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr 1)] (:exprs stmt)))
  (is (= [(parse-expr '(< 1 2 3))] (:where stmt))))

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
  (is (= [(parse-expr '(= :kind "Comedy"))] (:where stmt)))
  (is (= [(parse-from :films)] (:from stmt))))

(deftest-stmt test-select-is-null
  ["SELECT 1 WHERE (NULL IS NULL)"]
  (select [1]
    (where '(is-null nil)))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr 1)] (:exprs stmt)))
  (is (= [(parse-expr '(is-null nil))] (:where stmt))))

(deftest-stmt test-select-is-not-null
  ["SELECT 1 WHERE (NULL IS NOT NULL)"]
  (select [1]
    (where '(is-not-null nil)))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr 1)] (:exprs stmt)))
  (is (= [(parse-expr '(is-not-null nil))] (:where stmt))))

(deftest-stmt test-select-backquote-date
  ["SELECT * FROM countries WHERE (created-at > ?)" (java.sql.Date. 0)]
  (select [*]
    (from :countries)
    (where `(> :created-at ~(java.sql.Date. 0))))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-expr `(> :created-at ~(java.sql.Date. 0)))] (:where stmt))))

(deftest-stmt test-select-star-number-string
  ["SELECT *, 1, ?" "x"]
  (select [* 1 "x"])
  (is (= :select (:op stmt)))
  (is (= (map parse-expr [* 1 "x"]) (:exprs stmt))))

(deftest-stmt test-select-column
  ["SELECT created-at FROM continents"]
  (select [:created-at]
    (from :continents))
  (is (= :select (:op stmt)))
  (is (= [(parse-table :continents)] (:from stmt)))
  (is (= [(parse-expr :created-at)] (:exprs stmt))))

(deftest-stmt test-select-columns
  ["SELECT name, created-at FROM continents"]
  (select [:name :created-at]
    (from :continents))
  (is (= :select (:op stmt)))
  (is (= [(parse-table :continents)] (:from stmt)))
  (is (= (map parse-expr [:name :created-at]) (:exprs stmt))))

(deftest-stmt test-select-column-alias
  ["SELECT created-at AS c FROM continents"]
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
  ["SELECT max(created-at) AS m FROM continents"]
  (select [(as '(max :created-at) :m)]
    (from :continents))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr (as '(max :created-at) :m))] (:exprs stmt)))
  (is (= [(parse-table :continents)] (:from stmt))))

(deftest-stmt test-select-limit
  ["SELECT * FROM continents LIMIT 1"]
  (select [*]
    (from :continents)
    (limit 1))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr *)] (:exprs stmt)))
  (is (= [(parse-table :continents)] (:from stmt))))

(deftest-stmt test-select-column-max
  ["SELECT max(created-at) FROM continents"]
  (select ['(max :created-at)]
    (from :continents))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr '(max :created-at))] (:exprs stmt)))
  (is (= [(parse-table :continents)] (:from stmt))))

(deftest-stmt test-select-most-recent-weather-report
  ["SELECT DISTINCT ON (location) location, time, report FROM weather-reports ORDER BY location, time DESC"]
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

(deftest-stmt test-select-group-by-a-order-by-1
  ["SELECT a, max(b) FROM table-1 GROUP BY a ORDER BY 1"]
  (select [:a '(max :b)]
    (from :table-1)
    (group-by :a)
    (order-by 1))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr :a) (parse-expr '(max :b))] (:exprs stmt)))
  (is (= [(parse-from :table-1)] (:from stmt)))
  (is (= [(parse-expr 1)] (:order-by stmt))))

(deftest-stmt select-test-order-by-query-select
  ["SELECT a, b FROM table-1 ORDER BY (a + b), c"]
  (select [:a :b]
    (from :table-1)
    (order-by '(+ :a :b) :c))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr :a) (parse-expr :b)] (:exprs stmt)))
  (is (= [(parse-from :table-1)] (:from stmt)))
  (is (= [(parse-expr '(+ :a :b)) (parse-expr :c)] (:order-by stmt))))

(deftest-stmt select-test-order-by-sum
  ["SELECT (a + b) AS sum, c FROM table-1 ORDER BY sum"]
  (select [(as '(+ :a :b) :sum) :c]
    (from :table-1)
    (order-by :sum))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr (as '(+ :a :b) :sum)) (parse-expr :c)] (:exprs stmt)))
  (is (= [(parse-from :table-1)] (:from stmt)))
  (is (= [(parse-expr :sum)] (:order-by stmt))))

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

;; UPDATE

(deftest-stmt test-update-drama-to-dramatic
  ["UPDATE films SET kind = ? WHERE (kind = ?)" "Dramatic" "Drama"]
  (update :films {:kind "Dramatic"}
    (where '(= :kind "Drama")))
  (is (= :update (:op stmt)))
  (is (= (parse-table :films) (:table stmt)))
  (is (= [(parse-expr '(= :kind "Drama"))] (:where stmt)))
  (is (= {:kind "Dramatic"} (:row stmt))))
