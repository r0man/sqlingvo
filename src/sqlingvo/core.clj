(ns sqlingvo.core
  (:refer-clojure :exclude [distinct group-by])
  (:require [clojure.algo.monads :refer [state-m m-seq with-monad]]
            [clojure.java.jdbc :as jdbc]
            [sqlingvo.compiler :refer [compile-sql compile-stmt]]
            [sqlingvo.util :refer [as-keyword parse-expr parse-exprs parse-column parse-from parse-table]]))

(defn- concat-in [m ks & args]
  (apply update-in m ks concat args))

(defn ast
  "Returns the abstract syntax tree of `stmt`."
  [stmt]
  (if (map? stmt)
    stmt (first (stmt {}))))

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
  (fn [stmt]
    (let [[_ copy]
          ((with-monad state-m (m-seq body))
           {:op :copy
            :table (parse-table table)
            :columns (map parse-column columns)})]
      [copy (assoc stmt (:op copy) copy)])))

(defn create-table
  "Returns a CREATE TABLE statement."
  [table & body]
  (fn [stmt]
    (let [[_ create-table]
          ((with-monad state-m (m-seq body))
           {:op :create-table
            :table (parse-table table)})]
      [create-table (assoc stmt (:op create-table) create-table)])))

(defn delete
  "Returns a DELETE statement."
  [table & body]
  (fn [stmt]
    (let [[_ delete]
          ((with-monad state-m (m-seq body))
           {:op :delete
            :table (parse-table table)})]
      [delete (assoc stmt (:op delete) delete)]))  )

(defn drop-table
  "Returns a DROP TABLE statement."
  [tables & body]
  (fn [stmt]
    (let [[_ drop-table]
          ((with-monad state-m (m-seq body))
           {:op :drop-table
            :tables (map parse-table tables)})]
      [drop-table (assoc stmt (:op drop-table) drop-table)])))

(defn except
  "Returns a fn that adds a EXCEPT clause to an SQL statement."
  [stmt-2 & {:keys [all]}]
  (let [stmt-2 (ast stmt-2)]
    (fn [stmt-1]
      [nil (update-in stmt-1 [:set] conj {:op :except :stmt stmt-2 :all all})])))

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

(defn if-not-exists
  "Returns a fn that adds a IF EXISTS clause to an SQL statement."
  [if-not-exists?]
  (fn [stmt]
    (if if-not-exists?
      [nil (assoc stmt :if-not-exists {:op :if-not-exists})]
      [nil stmt])))

(defn inherits
  [& tables]
  (fn [stmt]
    [nil (assoc stmt :inherits (map parse-table tables))]))

(defn insert
  "Returns a INSERT statement."
  [table columns & body]
  (fn [stmt]
    (let [[_ insert]
          ((with-monad state-m (m-seq body))
           {:op :insert
            :table (parse-table table)
            :columns (map parse-column columns)})]
      [insert (assoc stmt (:op insert) insert)])))

(defn intersect
  "Returns a fn that adds a INTERSECT clause to an SQL statement."
  [stmt-2 & {:keys [all]}]
  (let [stmt-2 (ast stmt-2)]
    (fn [stmt-1]
      [nil (update-in stmt-1 [:set] conj {:op :intersect :stmt stmt-2 :all all})])))

(defn join
  "Returns a fn that adds a JOIN clause to an SQL statement."
  [from [how & condition] & {:keys [type outer]}]
  (let [how (keyword how)]
    (fn [stmt]
      [nil (update-in
            stmt [:joins] conj
            {:op :join
             :from (parse-from from)
             :type type
             :on (if (= :on how) (parse-expr (first condition)))
             :using (if (= :using how) (map parse-expr condition))
             :outer outer})])))

(defn like
  "Returns a fn that adds a LIKE clause to an SQL statement."
  [table & {:as opts}]
  (fn [stmt]
    (let [like (assoc opts :op :like :table (parse-table table))]
      [nil (assoc stmt :like like)])))

(defn limit
  "Returns a fn that adds a LIMIT clause to an SQL statement."
  [count]
  (fn [stmt]
    [nil (assoc stmt :limit {:op :limit :count count})]))

(defn nulls
  "Parse `expr` and return an NULLS FIRST/LAST expr."
  [expr where] (assoc (parse-expr expr) :nulls where))

(defn offset
  "Returns a fn that adds a OFFSET clause to an SQL statement."
  [start]
  (fn [stmt]
    [nil (assoc stmt :offset {:op :offset :start start})]))

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
  "Returns a fn that adds a RETURNING clause to an SQL statement."
  [& exprs] (fn [stmt] [nil (concat-in stmt [:returning] (map parse-expr exprs))]))

(defn select
  "Returns a SELECT statement."
  [exprs & body]
  (fn [stmt]
    (let [[_ select]
          ((with-monad state-m (m-seq body))
           {:op :select
            :distinct (if (= :distinct (:op exprs))
                        exprs)
            :exprs (if (sequential? exprs)
                     (map parse-expr exprs))})]
      [select (assoc stmt (:op select) select)])))

(defn temporary
  "Returns a fn that adds a TEMPORARY clause to an SQL statement."
  [temporary?]
  (fn [stmt]
    (if temporary?
      [nil (assoc stmt :temporary {:op :temporary})]
      [nil stmt])))

(defn truncate
  "Returns a TRUNCATE statement."
  [tables & body]
  (fn [stmt]
    (let [[_ truncate]
          ((with-monad state-m (m-seq body))
           {:op :truncate
            :tables (map parse-table tables)})]
      [truncate (assoc stmt (:op truncate) truncate)])))

(defn union
  "Returns a fn that adds a UNION clause to an SQL statement."
  [stmt-2 & {:keys [all]}]
  (let [stmt-2 (ast stmt-2)]
    (fn [stmt-1]
      [nil (update-in stmt-1 [:set] conj {:op :union :stmt stmt-2 :all all})])))

(defn update
  "Returns a UPDATE statement."
  [table row & body]
  (fn [stmt]
    (let [[_ update]
          ((with-monad state-m (m-seq body))
           {:op :update
            :table (parse-table table)
            :exprs (if (sequential? row) (map parse-expr row))
            :row (if (map? row) row)})]
      [update (assoc stmt (:op update) update)])))

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

(defn sql
  "Compile `stmt` into a clojure.java.jdbc compatible vector."
  [stmt] (compile-stmt (ast stmt)))

(defn run
  "Compile and run `stmt` against the current clojure.java.jdbc
  database connection."
  [stmt]
  (let [ast (ast stmt)
        compiled (apply vector (compile-sql ast))]
    (if (or (= :select (:op ast)) (:returning ast))
      (jdbc/with-query-results results
        compiled (doall results))
      (map #(hash-map :count %1)
           (jdbc/do-prepared (first compiled) (rest compiled))))))

(defn run1
  "Run `stmt` against the current clojure.java.jdbc database
  connection and return the first row."
  [stmt] (first (run stmt)))