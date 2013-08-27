(ns sqlingvo.compiler
  (:refer-clojure :exclude [replace])
  (:require [clojure.core :as core]
            [clojure.java.io :refer [file]]
            [clojure.string :refer [blank? join replace upper-case]]
            [sqlingvo.vendor :refer [->postgresql sql-quote sql-name]]))

(defprotocol SQLType
  (sql-type [arg] "Convert `arg` into an SQL type."))

(extend-type Object
  SQLType
  (sql-type [obj] obj))

(defmulti compile-sql (fn [db ast] (:op ast)))

(defn keyword-sql [k]
  (replace (upper-case (name k)) #"-" " "))

(defn compiled? [arg]
  (and (sequential? arg) (string? (first arg))))

(defn wrap-stmt [stmt]
  (let [[sql & args] stmt]
    (cons (str "(" sql ")") args)))

(defn unwrap-stmt [stmt]
  (let [[sql & args] stmt]
    (cons (replace sql #"^\(|\)$" "") args)))

(defn- join-stmt [db separator & stmts]
  (let [stmts (map #(if (compiled? %1) %1 (compile-sql db %1)) (remove empty? stmts))
        stmts (remove (comp blank? first) stmts)]
    (cons (join separator (map first stmts))
          (apply concat (map rest stmts)))))

(defn- stmt [db & stmts]
  (apply join-stmt db " " (remove empty? stmts)))

(defn- compile-set-op [db op {:keys [stmt all] :as node}]
  (let [[sql & args] (compile-sql db stmt)]
    (cons (str (upper-case (name op)) (if all " ALL") " " sql)
          args)))

;; COMPILE CONSTANTS

(defn compile-inline [db {:keys [form as]}]
  [(str form (if as (str " AS " (sql-quote db as))))])

(defmulti compile-const
  "Compile a SQL constant into a SQL statement."
  (fn [db node] (class (:form node))))

(defmethod compile-const clojure.lang.Symbol [db node]
  (compile-inline db node))

(defmethod compile-const Double [db node]
  (compile-inline db node))

(defmethod compile-const Long [db node]
  (compile-inline db node))

(defmethod compile-const :default [db {:keys [form as]}]
  [(str "?" (if as (str " AS " (sql-quote db as)))) (sql-type form)])

;; COMPILE EXPRESSIONS

(defmulti compile-expr
  "Compile a SQL expression."
  (fn [db ast] (:op ast)))

(defmethod compile-expr :array [db {:keys [children]}]
  (let [children (map #(compile-expr db %1) children)]
    (cons (str "ARRAY[" (join ", " (map first children)) "]")
          (mapcat rest children))))

(defmethod compile-expr :select [db {:keys [as] :as expr}]
  (wrap-stmt (compile-sql db expr)))

(defmethod compile-expr :default [db node]
  (compile-sql db node))

;; COMPILE FN CALL

(defn compile-2-ary
  "Compile a 2-arity SQL function node into a SQL statement."
  [db {:keys [as args name] :as node}]
  (cond
   (> 2 (count args))
   (throw (IllegalArgumentException. "More than 1 arg needed."))
   (= 2 (count args))
   (let [[[s1 & a1] [s2 & a2]] (map #(compile-expr db %1) args)]
     (cons (str "(" s1 " " (core/name name) " " s2 ")"
                (if as (str " AS " (sql-quote db as))))
           (concat a1 a2)))
   :else
   (apply join-stmt db " AND "
          (map #(compile-2-ary db (assoc node :args %1))
               (partition 2 1 args)))))

(defn compile-infix
  "Compile a SQL infix function node into a SQL statement."
  [db {:keys [as args name]}]
  (cond
   (= 1 (count args))
   (compile-expr db (first args))
   :else
   (let [args (map #(compile-expr db %1) args)]
     (cons (str "(" (join (str " " (core/name name) " ") (map first args)) ")"
                (if as (str " AS " (sql-quote db as))))
           (apply concat (map rest args))))))

(defn compile-complex-args [db {:keys [as args name] :as node}]
  (let [[sql & args] (apply join-stmt db " " args)]
    (cons (str "(" (core/name name) " " sql ")"
               (if as (str " AS " (sql-quote db as))))
          args)))

(defn compile-whitespace-args [db {:keys [as args name] :as node}]
  (let [[sql & args] (apply join-stmt db " " args)]
    (cons (str (core/name name) "(" sql ")"
               (if as (str " AS " (sql-quote db as))))
          args)))

(defmulti compile-fn
  "Compile a SQL function node into a SQL statement."
  (fn [db node] (keyword (:name node))))

(defmethod compile-fn :cast [db {[expr type] :args}]
  (let [[sql & args] (compile-expr db expr)
        type (name (:name type))]
    (cons (str "CAST(" sql " AS " type ")") args)))

(defmethod compile-fn :count [db {:keys [args]}]
  (let [distinct? (= 'distinct (:form (first args)))
        args (map #(compile-sql db %1) (remove #(= 'distinct (:form %1)) args))]
    (cons (str "count(" (if distinct? "DISTINCT ")
               (join ", " (map first args)) ")")
          (mapcat rest args))))

(defmethod compile-fn :is-null [db {:keys [args]}]
  (let [[sql & args] (compile-expr db (first args))]
    (cons (str "(" sql " IS NULL)") args)))

(defmethod compile-fn :is-not-null [db {:keys [args]}]
  (let [[sql & args] (compile-expr db (first args))]
    (cons (str "(" sql " IS NOT NULL)") args)))

(defmethod compile-fn :range [db {:keys [args]}]
  (let [args (map #(compile-expr db %1) args)]
    (cons (str "(" (join ", " (map first args)) ")")
          (mapcat rest args))))

(defmethod compile-fn :default [db {:keys [as args name]}]
  (let [args (map #(compile-expr db %1) args)]
    (cons (str (sql-name db name) "(" (join ", " (map first args)) ")"
               (if as (str " AS " (sql-quote db as))))
          (apply concat (map rest args)))))

;; COMPILE FROM CLAUSE

(defmulti compile-from (fn [db ast] (:op ast)))

(defmethod compile-from :fn [db fn]
  (compile-sql db fn))

(defmethod compile-from :select [db node]
  (let [[sql & args] (compile-sql db node)]
    (cons (str "(" sql ") AS " (sql-quote db (:as node))) args)))

(defmethod compile-from :table [db node]
  (compile-sql db node))

;; COMPILE SQL

(defmethod compile-sql :copy [db {:keys [columns encoding from to table]}]
  (let [from (first from)]
    (cons (str "COPY " (first (compile-sql db table))
               (if-not (empty? columns)
                 (str " (" (first (apply join-stmt db ", " columns)) ")"))
               " FROM "
               (cond
                (string? from) "?"
                (= :stdin from) "STDIN")
               (if encoding " ENCODING ?"))
          (cond
           (and (string? from) encoding)
           [from encoding]
           (string? from)
           [(.getAbsolutePath (file from))]
           (= :stdin from) []))))

(defn compile-column [db column]
  [(str (sql-quote db (:name column))
        " " (replace (upper-case (name (:type column))) "-" " ")
        (if-let [length (:length column)]
          (str "(" length ")"))
        (if (:not-null? column)
          " NOT NULL")
        (if (:unique? column)
          " UNIQUE")
        (if (:primary-key? column)
          " PRIMARY KEY")
        (if-let [default (:default column)]
          (str " DEFAULT " default)))])

(defmethod compile-sql :create-table [db {:keys [table if-not-exists inherits like temporary] :as node}]
  (let [columns (map #(compile-column db %1) (map (:column node) (:columns node)))]
    (cons (str "CREATE"
               (if temporary " TEMPORARY")
               " TABLE"
               (if if-not-exists " IF NOT EXISTS")
               (str " " (first (compile-sql db table)))
               " ("
               (cond
                (not (empty? columns))
                (join ", " (map first columns))
                like
                (first (compile-sql db like)))
               ")"
               (if inherits
                 (str " INHERITS (" (join ", " (map (comp first #(compile-sql db %1)) inherits)) ")")))
          [])))

(defmethod compile-sql :delete [db {:keys [where table returning]}]
  (let [returning (map #(compile-sql db %1) returning)
        where (if where (compile-sql db where))]
    (cons (str "DELETE FROM " (first (compile-sql db table))
               (if-not (empty? where)
                 (str " WHERE " (first where)))
               (if-not (empty? returning)
                 (apply str " RETURNING " (join ", " (map first returning)))))
          (concat (rest where)
                  (mapcat rest returning)))))

(defmethod compile-sql :column [db {:keys [as schema name table direction nulls]}]
  [(str (join "." (map #(sql-quote db %1) (remove nil? [schema table name])))
        (if as (str " AS " (sql-quote db as)))
        (if direction (str " " (upper-case (core/name direction))))
        (if nulls (str " NULLS " (keyword-sql nulls))))])

(defmethod compile-sql :column [db {:keys [as schema name table direction nulls]}]
  [(str (->> [(if schema (sql-quote db schema))
              (if table (sql-quote db table))
              (if name (if (= :* name) "*" (sql-quote db name)))]
             (remove nil?)
             (join "."))
        (if as (str " AS " (sql-quote db as)))
        (if direction (str " " (upper-case (core/name direction))))
        (if nulls (str " NULLS " (keyword-sql nulls))))])

(defmethod compile-sql :constant [db node]
  (compile-const db node))

(defmethod compile-sql :condition [db {:keys [condition]}]
  (let [[sql & args] (compile-sql db condition)]
    (cons sql args)))

(defmethod compile-sql :drop-table [db {:keys [cascade if-exists restrict tables]}]
  (join-stmt
   db " " ["DROP TABLE"]
   if-exists (apply join-stmt db ", " tables)
   cascade restrict))

(defmethod compile-sql :except [db node]
  (compile-set-op db :except node))

(defmethod compile-sql :expr-list [db {:keys [as children]}]
  (let [[sql & args] (apply join-stmt db " " children)]
    (cons (str sql (if as (str " AS " (sql-quote db as))))
          args)))

(defmethod compile-sql :fn [db node]
  (let [[sql & args] (compile-fn db node)]
    (cons (if-let [dir (:direction node)]
            (str sql " " (upper-case (name dir)))
            sql) args)))

(defmethod compile-sql :from [db {:keys [clause joins]}]
  (let [from (map #(compile-from db %1) clause)
        joins (map #(compile-sql db %1) joins)]
    (cons (str "FROM "
               (join ", " (map first from))
               (if-not (empty? joins)
                 (str " " (join " " (map first joins)))))
          (apply concat (map rest from)))))

(defmethod compile-sql :group-by [db {:keys [exprs]}]
  (stmt db ["GROUP BY"] exprs))

(defmethod compile-sql :insert [db {:keys [table columns rows default-values values returning select]}]
  (let [[sql & args] (if select (compile-sql db select))
        returning (map #(compile-sql db %1) returning)
        columns (if (and (empty? columns) (not (empty? values)))
                  (map (fn [k] {:op :column :name k})
                       (keys (first values)))
                  columns)]
    (cons (str "INSERT INTO " (first (compile-sql db table))
               (if-not (empty? columns)
                 (str " (" (first (apply join-stmt db ", " columns)) ")"))
               (if-not (empty? values)
                 (let [template (str "(" (join ", " (repeat (count columns) "?")) ")")]
                   (str " VALUES " (join ", " (repeat (count values) template)))))
               (if sql (str " " sql))
               (if default-values " DEFAULT VALUES")
               (if-not (empty? returning)
                 (apply str " RETURNING " (join ", " (map first returning)))))
          (if-not (empty? values)
            (apply concat (map (fn [r] (map r (map :name columns))) values))
            args))))

(defmethod compile-sql :intersect [db node]
  (compile-set-op db :intersect node))

(defmethod compile-sql :join [db {:keys [on using from how type outer]}]
  (let [[on-sql & on-args] (if on (compile-sql db on))
        [from-sql & from-args] (compile-from db from)
        using (map #(compile-sql db %1) using)]
    (cons (str (case type
                 :cross "CROSS "
                 :inner "INNER "
                 :left "LEFT "
                 :right "RIGHT "
                 nil "")
               (if outer "OUTER ")
               "JOIN " from-sql
               (if on
                 (str " ON " on-sql))
               (if-not (empty? using)
                 (str " USING (" (join ", " (map first using)) ")")))
          (concat from-args on-args (mapcat rest using)))))

(defmethod compile-sql :keyword [db {:keys [form]}]
  [(sql-quote db form)])

(defmethod compile-sql :limit [db {:keys [count]}]
  [(str "LIMIT " (if (number? count) count "ALL"))])

(defmethod compile-sql :like [db {:keys [excluding including table]}]
  [(str "LIKE "
        (first (compile-sql db table))
        (if-not (empty? including)
          (str " INCLUDING " (join " " (map keyword-sql including))))
        (if-not (empty? excluding)
          (str " EXCLUDING " (join " " (map keyword-sql excluding)))))])

(defmethod compile-sql :nil [db _] ["NULL"])

(defmethod compile-sql :offset [db {:keys [start]}]
  [(str "OFFSET " (if (number? start) start 0))])

(defmethod compile-sql :order-by [db {:keys [exprs direction nulls using]}]
  (let [[sql & args] (compile-sql db exprs)]
    (cons (str "ORDER BY " sql) args)))

(defmethod compile-sql :table [db {:keys [as schema name]}]
  [(str (join "." (map #(sql-quote db %1) (remove nil? [schema name])))
        (if as (str " AS " (sql-quote db as))))])

(defmethod compile-sql :distinct [db {:keys [exprs on]}]
  (let [exprs (map #(compile-sql db %1) exprs)
        on (map #(compile-sql db %1) on)]
    (cons (str "DISTINCT "
               (if-not (empty? on)
                 (str "ON (" (join ", " (map first on)) ") "))
               (if-not (empty? exprs)
                 (join ", " (map first exprs))))
          (concat (mapcat rest on)
                  (mapcat rest exprs)))))

(defmethod compile-sql :select [db {:keys [exprs distinct joins from where group-by limit offset order-by set]}]
  (let [[distinct-sql & distinct-args] (if distinct (compile-sql db distinct))
        joins (map #(compile-sql db %1) joins)
        exprs (map #(compile-expr db %1) exprs)
        from (map #(compile-from db %1) from)
        where (if where (compile-sql db where))
        group-by (map #(compile-sql db %1) group-by)
        order-by (map #(compile-sql db %1) order-by)
        set (map #(compile-sql db %1) set)]
    (cons (str "SELECT " (join ", " (map first exprs))
               distinct-sql
               (if-not (empty? from)
                 (str " FROM " (join ", " (map first from))))
               (if-not (empty? joins)
                 (str " " (join " " (map first joins))))
               (if-not (empty? where)
                 (str " WHERE " (first where)))
               (if-not (empty? group-by)
                 (str " GROUP BY " (join ", " (map first group-by))))
               (if-not (empty? order-by)
                 (str " ORDER BY " (join ", " (map first order-by))))
               (if limit (str " " (first (compile-sql db limit))))
               (if offset (str " " (first (compile-sql db offset))))
               (if-not (empty? set)
                 (str " " (join ", " (map first set)))))
          (concat (mapcat rest exprs)
                  (mapcat rest from)
                  (mapcat rest joins)
                  (rest where)
                  (mapcat rest group-by)
                  (mapcat rest order-by)
                  (mapcat rest set)))))

(defmethod compile-sql :default [db {:keys [op]}]
  [(keyword-sql op)])

(defmethod compile-sql :list [db {:keys [children]}]
  (let [children (map #(compile-sql db %1) children)]
    (cons (str "(" (join ", " (map first children)) ")")
          (mapcat rest children))))

(defmethod compile-sql :truncate [db {:keys [tables continue-identity restart-identity cascade restrict]}]
  (join-stmt
   db " " ["TRUNCATE TABLE"]
   (apply join-stmt db ", " tables)
   continue-identity restart-identity cascade restrict))

(defmethod compile-sql :union [db node]
  (compile-set-op db :union node))

(defmethod compile-sql :update [db {:keys [where from exprs table row returning]}]
  (let [returning (map #(compile-sql db %1) returning)
        where (if where (compile-sql db where))
        columns (map #(sql-quote db %1) (keys row))
        exprs (map (comp unwrap-stmt #(compile-expr db %1)) exprs)
        from (map #(compile-from db %1) from)]
    (cons (str "UPDATE " (first (compile-sql db table))
               " SET " (if row
                         (apply str (concat (interpose " = ?, " columns) " = ?"))
                         (join ", " (map first exprs)))
               (if-not (empty? from)
                 (str " FROM " (join " " (map first from))))
               (if-not (empty? where)
                 (str " WHERE " (first where)))
               (if-not (empty? returning)
                 (apply str " RETURNING " (join ", " (map first returning)))))
          (concat (vals row)
                  (mapcat rest (concat exprs from))
                  (rest where)
                  (mapcat rest returning)))))

;; DEFINE SQL FN ARITY

(defmacro defarity
  "Define SQL functions in terms of `arity-fn`."
  [arity-fn & fns]
  `(do ~@(for [fn# (map keyword fns)]
           `(defmethod compile-fn ~fn# [db# ~'node]
              (~arity-fn db# ~'node)))))

(defarity compile-2-ary
  "=" "!=" "<>" "<" ">" "<=" ">=" "&&" "/" "^" "~" "~*" "like" "ilike" "in")

(defarity compile-infix
  "+" "-" "*" "&" "!~" "!~*" "%" "and" "or" "union" "||" "overlaps" "@@")

(defarity compile-complex-args
  "partition")

(defarity compile-whitespace-args
  "substring"
  "trim")

(defn compile-stmt
  "Compile `stmt` into a clojure.java.jdbc compatible prepared
  statement vector."
  ([stmt]
     (compile-stmt (->postgresql {}) stmt))
  ([db stmt]
     (assert db "No db given!")
     (apply vector (compile-sql db stmt))))
