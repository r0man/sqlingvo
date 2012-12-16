(ns sqlingvo.monad
  (:refer-clojure :exclude [group-by])
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

(defn desc
  "Parse `expr` and return an ORDER BY expr using descending order."
  [expr] (assoc (parse-expr expr) :direction :desc))

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
  [& exprs]
  (fn [stmt]
    [nil (concat-in stmt [:group-by] (map parse-expr exprs))]))

(defn order-by
  "Returns a fn that adds a ORDER BY clause to an SQL statement."
  [& exprs]
  (fn [stmt]
    [nil (concat-in stmt [:order-by] (map parse-expr exprs))]))

(defn returning
  "Add the RETURNING clause the SQL statement."
  [& exprs]
  (fn [stmt]
    [nil (concat-in stmt [:returning] (map parse-expr exprs))]))

(defn select
  "Returns a SELECT statement."
  [exprs & body]
  (second ((with-monad state-m (m-seq body))
           {:op :select
            :exprs (map parse-expr exprs)})))

(defn where
  "Returns a fn that adds a WHERE clause to an SQL statement."
  [& exprs]
  (fn [stmt]
    [nil (concat-in stmt [:where] (map parse-expr exprs))]))

(defn sql [stmt]
  (compile-stmt stmt))