(ns sqlingvo.compiler
  (:refer-clojure :exclude [replace])
  (:require [clojure.core :as core]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [blank? join replace upper-case]]
            [sqlingvo.util :refer [as-identifier]]))

(defprotocol SQLType
  (sql-type [arg] "Convert `arg` into an SQL type."))

(extend-type Object
  SQLType
  (sql-type [obj] obj))

(defmulti compile-sql :op)

(defn stmt? [arg]
  (and (sequential? arg) (string? (first arg))))

(defn wrap-stmt [stmt]
  (let [[sql & args] stmt]
    (cons (str "(" sql ")") args)))

(defn unwrap-stmt [stmt]
  (let [[sql & args] stmt]
    (cons (replace sql #"^\(|\)$" "") args)))

(defn- join-stmt [separator & stmts]
  (let [stmts (map #(if (stmt? %1) %1 (compile-sql %1)) stmts)
        stmts (remove (comp blank? first) stmts)]
    (cons (join separator (map first stmts))
          (apply concat (map rest stmts)))))

(defn- stmt [& stmts]
  (apply join-stmt " " (remove nil? stmts)))

(defn- compile-set-op [op {:keys [stmt all] :as node}]
  (let [[sql & args] (compile-sql stmt)]
    (cons (str (upper-case (name op)) (if all " ALL") " " sql)
          args)))

;; COMPILE CONSTANTS

(defn compile-inline [{:keys [form as]}]
  [(str form (if as (str " AS " (as-identifier as))))])

(defmulti compile-const
  "Compile a SQL constant into a SQL statement."
  (fn [node] (class (:form node))))

(defmethod compile-const clojure.lang.Symbol [node]
  (compile-inline node))

(defmethod compile-const Double [node]
  (compile-inline node))

(defmethod compile-const Long [node]
  (compile-inline node))

(defmethod compile-const :default [{:keys [form as]}]
  [(str "?" (if as (str " AS " (as-identifier as)))) (sql-type form)])

;; COMPILE EXPRESSIONS

(defmulti compile-expr
  "Compile a SQL expression."
  :op)

(defmethod compile-expr :select [{:keys [as] :as expr}]
  (wrap-stmt (compile-sql expr)))

(defmethod compile-expr :default [node]
  (compile-sql node))

;; COMPILE FN CALL

(defn compile-2-ary
  "Compile a 2-arity SQL function node into a SQL statement."
  [{:keys [as args name] :as node}]
  (cond
   (> 2 (count args))
   (throw (IllegalArgumentException. "More than 1 arg needed."))
   (= 2 (count args))
   (let [[[s1 & a1] [s2 & a2]] (map compile-expr args)]
     (cons (str "(" s1 " " (core/name name) " " s2 ")"
                (if as (str " AS " (as-identifier as))))
           (concat a1 a2)))
   :else
   (apply join-stmt " AND "
          (map #(compile-2-ary (assoc node :args %1))
               (partition 2 1 args)))))

(defn compile-infix
  "Compile a SQL infix function node into a SQL statement."
  [{:keys [as args name]}]
  (cond
   (= 1 (count args))
   (compile-expr (first args))
   :else
   (let [args (map compile-expr args)]
     (cons (str "(" (join (str " " (core/name name) " ") (map first args)) ")"
                (if as (str " AS " (as-identifier as))))
           (apply concat (map rest args))))))

(defn compile-whitespace-args [{:keys [as args name] :as node}]
  (let [[sql & args] (apply join-stmt " " args)]
    (cons (str "(" (core/name name) " " sql ")"
               (if as (str " AS " (as-identifier as))))
          args)))

(defmulti compile-fn
  "Compile a SQL function node into a SQL statement."
  (fn [node] (keyword (:name node))))

(defmethod compile-fn :default [{:keys [as args name]}]
  (let [args (map compile-expr args)]
    (cons (str (jdbc/as-identifier name) "(" (join ", " (map first args)) ")"
               (if as (str " AS " (as-identifier as))))
          (apply concat (map rest args)))))

(defmethod compile-fn :is-null [{:keys [args]}]
  (let [[sql & args] (compile-expr (first args))]
    (cons (str "(" sql " IS NULL)") args)))

(defmethod compile-fn :is-not-null [{:keys [args]}]
  (let [[sql & args] (compile-expr (first args))]
    (cons (str "(" sql " IS NOT NULL)") args)))

;; COMPILE FROM CLAUSE

(defmulti compile-from :op)

(defmethod compile-from :select [node]
  (let [[sql & args] (compile-sql node)]
    (cons (str "(" sql ") AS " (as-identifier (:as node))) args)))

(defmethod compile-from :table [node]
  (compile-sql node))

;; COMPILE SQL

(defmethod compile-sql :copy [{:keys [columns from to table]}]
  (cons (str "COPY " (first (compile-sql table))
             (if-not (empty? columns)
               (str " (" (first (apply join-stmt ", " columns)) ")"))
             " FROM "
             (cond
              (string? from) "?"
              (= :stdin from) "STDIN"))
        (cond
         (string? from) [from]
         (= :stdin from) [])))

(defmethod compile-sql :create-table [{:keys [columns table if-not-exists inherits like temporary]}]
  (cons (str "CREATE"
             (if temporary " TEMPORARY")
             " TABLE"
             (if if-not-exists " IF NOT EXISTS")
             (str " " (first (compile-sql table)))
             " ("
             (cond
              (not (empty? columns))
              (join ", " (map (comp first compile-sql) columns))
              like
              (first (compile-sql like)))
             ")"
             (if inherits
               (str " INHERITS (" (join ", " (map (comp first compile-sql) inherits)) ")")))
        []))

(defmethod compile-sql :delete [{:keys [condition table returning]}]
  (let [[sql & args] (if condition (compile-sql condition))]
    (cons (str "DELETE FROM " (first (compile-sql table))
               (if sql (str " " sql))
               (if returning
                 (apply str " RETURNING " (first (compile-sql (:exprs returning))))))
          args)))

(defmethod compile-sql :column [{:keys [as schema name table]}]
  [(str (join "." (map as-identifier (remove nil? [schema table name])))
        (if as (str " AS " (as-identifier as))))])

(defmethod compile-sql :constant [node]
  (compile-const node))

(defmethod compile-sql :condition [{:keys [condition]}]
  (let [[sql & args] (compile-sql condition)]
    (cons (str "WHERE " sql) args)))

(defmethod compile-sql :distinct [{:keys [exprs on]}]
  (let [[expr-sql & expr-args] (compile-sql exprs)
        [on-sql & on-args] (if on (compile-sql on))]
    (cons (str "DISTINCT "
               (if on
                 (str "ON (" on-sql ") "))
               expr-sql)
          (concat expr-args on-args))))

(defmethod compile-sql :drop-table [{:keys [cascade if-exists restrict tables]}]
  (let [[sql & args] (apply join-stmt ", " tables)]
    (cons (str "DROP TABLE " (if if-exists "IF EXISTS ") sql
               (if cascade " CASCADE")
               (if restrict " RESTRICT"))
          args)))

(defmethod compile-sql :except [node]
  (compile-set-op :except node))

(defmethod compile-sql :expr-list [{:keys [as children]}]
  (let [[sql & args] (apply join-stmt " " children)]
    (cons (str sql (if as (str " AS " (as-identifier as))))
          args)))

(defmethod compile-sql :exprs [{:keys [children]}]
  (let [children (map compile-expr children)]
    (if (empty? children)
      ["*"]
      (cons (join ", " (map first children))
            (apply concat (map rest children))))))

(defmethod compile-sql :fn [node]
  (compile-fn node))

(defmethod compile-sql :from [{:keys [clause joins]}]
  (let [from (map compile-from clause)
        joins (map compile-sql joins)]
    (cons (str "FROM "
               (join ", " (map first from))
               (if-not (empty? joins)
                 (str " " (join " " (map first joins)))))
          (apply concat (map rest from)))))

(defmethod compile-sql :group-by [{:keys [exprs]}]
  (stmt ["GROUP BY"] exprs))

(defmethod compile-sql :insert [{:keys [table columns rows default-values returning query]}]
  (let [[sql & args] (if query (compile-sql query))]
    (cons (str "INSERT INTO " (first (compile-sql table))
               (if-not (empty? columns)
                 (str " (" (first (apply join-stmt ", " columns)) ")"))
               (if-not (empty? rows)
                 (let [columns (map as-identifier (keys (first rows)))
                       template (str "(" (join ", " (repeat (count columns) "?")) ")")]
                   (str " (" (join ", " columns) ") VALUES "
                        (join ", " (repeat (count rows) template)))))
               (if sql (str " " sql))
               (if default-values " DEFAULT VALUES")
               (if returning
                 (apply str " RETURNING " (first (compile-sql (:exprs returning))))))
          (if rows (apply concat (map vals rows)) args))))

(defmethod compile-sql :intersect [node]
  (compile-set-op :intersect node))

(defmethod compile-sql :join [{:keys [condition from how type outer]}]
  (let [[cond-sql & cond-args] (compile-sql condition)
        [from-sql & from-args] (compile-from from)]
    (cons (str (case type
                 :cross "CROSS "
                 :inner "INNER "
                 :left "LEFT "
                 :right "RIGHT "
                 nil "")
               (if outer "OUTER ")
               "JOIN " from-sql " " (upper-case (name how)) " "
               (case how
                 :on cond-sql
                 :using (str "(" cond-sql ")")))
          (concat from-args cond-args))))

(defmethod compile-sql :keyword [{:keys [form]}]
  [(as-identifier form)])

(defmethod compile-sql :limit [{:keys [count]}]
  [(str "LIMIT " (if (number? count) count "ALL"))])

(defmethod compile-sql :like [{:keys [excluding including table]}]
  (letfn [(options [type opts]
            (str " " (join " " (map #(str (upper-case (name type)) " "
                                          (upper-case (name %1))) opts))))]
    [(str "LIKE "
          (first (compile-sql table))
          (if-not (empty? including)
            (options :including including))
          (if-not (empty? excluding)
            (options :excluding excluding)))]))

(defmethod compile-sql :nil [_] ["NULL"])

(defmethod compile-sql :offset [{:keys [start]}]
  [(str "OFFSET " (if (number? start) start 0))])

(defmethod compile-sql :order-by [{:keys [exprs direction nulls using]}]
  (let [[sql & args] (compile-sql exprs)]
    (cons (str "ORDER BY " sql
               (case direction
                 :asc " ASC"
                 :desc " DESC"
                 nil "")
               (case nulls
                 :first " NULLS FIRST"
                 :last " NULLS LAST"
                 nil ""))
          args)))

(defmethod compile-sql :table [{:keys [as schema name]}]
  [(str (join "." (map as-identifier (remove nil? [schema name])))
        (if as (str " AS " (as-identifier as))))])

(defmethod compile-sql :select [{:keys [exprs from condition group-by limit offset order-by set]}]
  (apply stmt ["SELECT"] exprs from condition group-by order-by limit offset (map compile-sql set)))

(defmethod compile-sql :truncate [{:keys [cascade tables continue-identity restart-identity restrict]}]
  (let [[sql & args] (apply join-stmt ", " tables)]
    (cons (str "TRUNCATE TABLE " sql
               (if restart-identity " RESTART IDENTITY")
               (if continue-identity " CONTINUE IDENTITY")
               (if cascade " CASCADE")
               (if restrict " RESTRICT"))
          args)))

(defmethod compile-sql :union [node]
  (compile-set-op :union node))

(defmethod compile-sql :update [{:keys [condition from exprs table row returning]}]
  (let [[sql & args] (if condition (compile-sql condition))
        columns (if row (map as-identifier (keys row)))
        exprs (if exprs (map (comp unwrap-stmt compile-expr) exprs))
        from (if from (map compile-from (:clause from)))]
    (cons (str "UPDATE " (first (compile-sql table))
               " SET " (if row
                         (apply str (concat (interpose " = ?, " columns) " = ?"))
                         (join ", " (map first exprs)))
               (if from (str " FROM " (join " " (map first from))))
               (if sql (str " " sql))
               (if returning (apply str " RETURNING " (first (compile-sql (:exprs returning))))))
          (concat (vals row) args (mapcat rest (concat exprs from))))))

;; DEFINE SQL FN ARITY

(defmacro defarity
  "Define SQL functions in terms of `arity-fn`."
  [arity-fn & fns]
  `(do ~@(for [fn# (map keyword fns)]
           `(defmethod compile-fn ~fn# [~'node]
              (~arity-fn ~'node)))))

(defarity compile-2-ary
  := :!= :<> :< :> :<= :>= :!~ :!~* :&& "/" "^" "~" "~*" :like :ilike :in)

(defarity compile-infix
  :+ :- :* :& "%" :and :or :union)

(defarity compile-whitespace-args
  :partition)

(defn compile-stmt
  "Compile `stmt` into a clojure.java.jdbc compatible prepared
  statement vector."
  [stmt] (apply vector (compile-sql stmt)))