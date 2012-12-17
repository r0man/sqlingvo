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

(deftest-stmt test-copy-country
  ["COPY country FROM ?" "/usr1/proj/bray/sql/country_data"]
  (copy :country []
    (from "/usr1/proj/bray/sql/country_data"))
  (is (= :copy (:op stmt)))
  (is (= ["/usr1/proj/bray/sql/country_data"] (:from stmt))))

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

(deftest-stmt test-group-by-a-order-by-1
  ["SELECT a, max(b) FROM table-1 GROUP BY a ORDER BY 1"]
  (select [:a '(max :b)]
    (from :table-1)
    (group-by :a)
    (order-by 1))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr :a) (parse-expr '(max :b))] (:exprs stmt)))
  (is (= [(parse-from :table-1)] (:from stmt)))
  (is (= [(parse-expr 1)] (:order-by stmt))))

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

(deftest-stmt test-update-drama-to-dramatic
  ["UPDATE films SET kind = ? WHERE (kind = ?)" "Dramatic" "Drama"]
  (update :films {:kind "Dramatic"}
    (where '(= :kind "Drama")))
  (is (= :update (:op stmt)))
  (is (= (parse-table :films) (:table stmt)))
  (is (= [(parse-expr '(= :kind "Drama"))] (:where stmt)))
  (is (= {:kind "Dramatic"} (:row stmt))))

(deftest-stmt test-order-by-query-select
  ["SELECT a, b FROM table-1 ORDER BY (a + b), c"]
  (select [:a :b]
    (from :table-1)
    (order-by '(+ :a :b) :c))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr :a) (parse-expr :b)] (:exprs stmt)))
  (is (= [(parse-from :table-1)] (:from stmt)))
  (is (= [(parse-expr '(+ :a :b)) (parse-expr :c)] (:order-by stmt))))

(deftest-stmt test-order-by-sum
  ["SELECT (a + b) AS sum, c FROM table-1 ORDER BY sum"]
  (select [(as '(+ :a :b) :sum) :c]
    (from :table-1)
    (order-by :sum))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr (as '(+ :a :b) :sum)) (parse-expr :c)] (:exprs stmt)))
  (is (= [(parse-from :table-1)] (:from stmt)))
  (is (= [(parse-expr :sum)] (:order-by stmt))))
