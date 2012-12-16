(ns sqlingvo.test.monad
  (:refer-clojure :exclude [group-by])
  (:use clojure.test
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
  (is (= :delete (:op stmt))))

(deftest-stmt test-delete-all-films-but-musicals
  ["DELETE FROM films WHERE (kind <> ?)" "Musical"]
  (delete :films
    (where '(<> :kind "Musical"))))

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

(deftest-stmt test-order-by-a+b-c
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

(deftest-stmt test-order-by-1
  ["SELECT a, max(b) FROM table-1 GROUP BY a ORDER BY 1"]
  (select [:a '(max :b)]
    (from :table-1)
    (group-by :a)
    (order-by 1))
  (is (= :select (:op stmt)))
  (is (= [(parse-expr :a) (parse-expr '(max :b))] (:exprs stmt)))
  (is (= [(parse-from :table-1)] (:from stmt)))
  (is (= [(parse-expr 1)] (:order-by stmt))))
