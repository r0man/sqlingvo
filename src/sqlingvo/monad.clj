(ns sqlingvo.monad
  (:refer-clojure :exclude [distinct group-by])
  (:require [clojure.algo.monads :refer [state-m m-seq with-monad]]
            [sqlingvo.compiler :refer [compile-sql compile-stmt]]
            [sqlingvo.util :refer [as-keyword parse-expr parse-column parse-from parse-table]]))

(defn- concat-in [m ks & args]
  (apply update-in m ks concat args))

(defn as
  "Parse `expr` and return an expr with and AS clause using `alias`."
  [expr alias]
  (assoc (parse-expr expr) :as (as-keyword alias)))

(defn asc
  "Parse `expr` and return an ORDER BY expr using ascending order."
  [expr] (assoc (parse-expr expr) :direction :asc))

(defn cascade
  "Returns a fn that adds a CASCADE clause to an SQL statement."
  [cascade?]
  (fn [stmt]
    (if cascade?
      [nil (assoc stmt :cascade {:op :cascade})]
      [nil stmt])))

(defn continue-identity
  "Returns a fn that adds a CONTINUE IDENTITY clause to an SQL statement."
  [continue-identity?]
  (fn [stmt]
    (if continue-identity?
      [nil (assoc stmt :continue-identity {:op :continue-identity})]
      [nil stmt])))

(defn desc
  "Parse `expr` and return an ORDER BY expr using descending order."
  [expr] (assoc (parse-expr expr) :direction :desc))

(defn distinct
  "Parse `exprs` and retuern a DISTINCT clause."
  [exprs & {:keys [on]}]
  {:op :distinct
   :exprs (map parse-expr exprs)
   :on (map parse-expr on)})

(defn copy
  "Returns a COPY statement."
  [table columns & body]
  (second ((with-monad state-m (m-seq body))
           {:op :copy
            :table (parse-table table)
            :columns (map parse-column columns)})))

(defn delete
  "Returns a DELETE statement."
  [table & body]
  (second ((with-monad state-m (m-seq body))
           {:op :delete
            :table (parse-table table)})))

(defn drop-table
  "Returns a DROP TABLE statement."
  [tables & body]
  (second ((with-monad state-m (m-seq body))
           {:op :drop-table
            :tables (map parse-table tables)})))

(defn from
  "Returns a fn that adds a FROM clause to an SQL statement."
  [& from]
  (fn [stmt]
    [nil (concat-in
          stmt [:from]
          (case (:op stmt)
            :copy [(first from)]
            (map parse-from from)))]))

(defn group-by
  "Returns a fn that adds a GROUP BY clause to an SQL statement."
  [& exprs] (fn [stmt] [nil (concat-in stmt [:group-by] (map parse-expr exprs))]))

(defn if-exists
  "Returns a fn that adds a IF EXISTS clause to an SQL statement."
  [if-exists?]
  (fn [stmt]
    (if if-exists?
      [nil (assoc stmt :if-exists {:op :if-exists})]
      [nil stmt])))

(defn insert
  "Returns a INSERT statement."
  [table columns & body]
  (second ((with-monad state-m (m-seq body))
           {:op :insert
            :table (parse-table table)
            :columns (map parse-column columns)})))

(defn order-by
  "Returns a fn that adds a ORDER BY clause to an SQL statement."
  [& exprs] (fn [stmt] [nil (concat-in stmt [:order-by] (map parse-expr exprs))]))

(defn restart-identity
  "Returns a fn that adds a RESTART IDENTITY clause to an SQL statement."
  [restart-identity?]
  (fn [stmt]
    (if restart-identity?
      [nil (assoc stmt :restart-identity {:op :restart-identity})]
      [nil stmt])))

(defn restrict
  "Returns a fn that adds a RESTRICT clause to an SQL statement."
  [restrict?]
  (fn [stmt]
    (if restrict?
      [nil (assoc stmt :restrict {:op :restrict})]
      [nil stmt])))

(defn returning
  "Add the RETURNING clause the SQL statement."
  [& exprs] (fn [stmt] [nil (concat-in stmt [:returning] (map parse-expr exprs))]))

(defn select
  "Returns a SELECT statement."
  [exprs & body]
  (second ((with-monad state-m (m-seq body))
           {:op :select
            :distinct (if (= :distinct (:op exprs))
                        exprs)
            :exprs (if (sequential? exprs)
                     (map parse-expr exprs))})))

(defn truncate
  "Returns a TRUNCATE statement."
  [tables & body]
  (second ((with-monad state-m (m-seq body))
           {:op :truncate
            :tables (map parse-table tables)})))

(defn update
  "Returns a UPDATE statement."
  [table row & body]
  (second ((with-monad state-m (m-seq body))
           {:op :update
            :table (parse-table table)
            :exprs (if (sequential? row) (map parse-expr row))
            :row (if (map? row) row)})))

(defn values
  "Returns a fn that adds a VALUES clause to an SQL statement."
  [values]
  (fn [stmt]
    [nil (case values
           :default (assoc stmt :default-values true)
           (concat-in
            stmt [:values]
            (if (sequential? values) values [values])))]))

(defn where
  "Returns a fn that adds a WHERE clause to an SQL statement."
  [& exprs]
  (fn [stmt]
    [nil (concat-in stmt [:where] (map parse-expr exprs))]))

(defn sql [stmt]
  (compile-stmt stmt))

;; ;; EXPERIMENTS

;; (defn ast [stmt]
;;   (second (stmt nil)))

;; (defn sql [stmt]
;;   (compile-stmt (ast stmt)))

;; (defn select
;;   "Returns a SELECT statement."
;;   [exprs & body]
;;   (fn [stmt]
;;     [nil (second
;;           ((with-monad state-m (m-seq body))
;;            {:op :select
;;             :distinct (if (= :distinct (:op exprs))
;;                         exprs)
;;             :exprs (if (sequential? exprs)
;;                      (map parse-expr exprs))}))]))

;; (defn insert
;;   "Returns a INSERT statement."
;;   [table columns & body]
;;   (fn [stmt]
;;     [nil (assoc {:op :insert
;;                  :table (parse-table table)
;;                  :columns (map parse-column columns)}
;;            :what (second ((with-monad state-m (m-seq body)) {})))]))

;; (pprint
;;  ((insert :films []
;;     (select [*]
;;       (from :tmp-films)
;;       (where '(< :date-prod "2004-05-07")))) nil))

;; (pprint
;;  ((insert :films []
;;     (values :default)) nil))

;; ((insert :films []
;;    (values {:code "T_601" :title "Yojimbo" :did 106 :date-prod "1961-06-16" :kind "Drama"})) nil)


;; ((select [*]
;;    (from :tmp-films)
;;    (where '(< :date-prod "2004-05-07"))) nil)

;; ((select [1]) nil)

;; ((select [*]
;;    (from :continents)) nil)

;; (sql (select [1]))
;; (sql (select [*]
;;        (from :continents)))

;; (ast (select [*]
;;        (from :continents)))


;; (ast (select [*]
;;        (from :continents)))

;; ((select [1]) nil)
