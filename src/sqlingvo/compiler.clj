2(ns sqlingvo.compiler
  (:refer-clojure :exclude [replace])
  (:require [clojure.core :as core]
            [clojure.string :refer [blank? join replace upper-case]]
            [sqlingvo.util :refer [as-identifier]]))

(defprotocol SQLType
  (sql-type [arg] "Convert `arg` into an SQL type."))

(extend-type Object
  SQLType
  (sql-type [obj] obj))

(defmulti compile-sql :op)

(defn keyword-sql [k]
  (replace (upper-case (name k)) #"-" " "))

(defn stmt? [arg]
  (and (sequential? arg) (string? (first arg))))

(defn wrap-stmt [stmt]
  (let [[sql & args] stmt]
    (cons (str "(" sql ")") args)))

(defn unwrap-stmt [stmt]
  (let [[sql & args] stmt]
    (cons (replace sql #"^\(|\)$" "") args)))

(defn- join-stmt [separator & stmts]
  (let [stmts (map #(if (stmt? %1) %1 (compile-sql %1)) (remove empty? stmts))
        stmts (remove (comp blank? first) stmts)]
    (cons (join separator (map first stmts))
          (apply concat (map rest stmts)))))

(defn- stmt [& stmts]
  (apply join-stmt " " (remove empty? stmts)))

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
    (cons (str (as-identifier name) "(" (join ", " (map first args)) ")"
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
  (let [from (first from)]
    (cons (str "COPY " (first (compile-sql table))
               (if-not (empty? columns)
                 (str " (" (first (apply join-stmt ", " columns)) ")"))
               " FROM "
               (cond
                (string? from) "?"
                (= :stdin from) "STDIN"))
          (cond
           (string? from) [from]
           (= :stdin from) []))))

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

(defmethod compile-sql :delete [{:keys [where table returning]}]
  (let [returning (if returning (map compile-sql returning))
        where (if where (map compile-sql where))]
    (cons (str "DELETE FROM " (first (compile-sql table))
               (if-not (empty? where)
                 (str " WHERE " (join ", " (map first where))))
               (if-not (empty? returning)
                 (apply str " RETURNING " (join ", " (map first returning)))))
          (concat (mapcat rest where)
                  (mapcat rest returning)))))

(defmethod compile-sql :column [{:keys [as schema name table direction nulls]}]
  [(str (join "." (map as-identifier (remove nil? [schema table name])))
        (if as (str " AS " (as-identifier as)))
        (if direction (str " " (upper-case (core/name direction))))
        (if nulls (str " NULLS " (keyword-sql nulls))))])

(defmethod compile-sql :constant [node]
  (compile-const node))

(defmethod compile-sql :condition [{:keys [condition]}]
  (let [[sql & args] (compile-sql condition)]
    (cons (str "WHERE " sql) args)))

(defmethod compile-sql :drop-table [{:keys [cascade if-exists restrict tables]}]
  (join-stmt
   " " ["DROP TABLE"]
   if-exists (apply join-stmt ", " tables)
   cascade restrict))

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

(defmethod compile-sql :insert [{:keys [table columns rows default-values values returning select]}]
  (let [[sql & args] (if select (compile-sql select))
        returning (if returning (map compile-sql returning))]
    (cons (str "INSERT INTO " (first (compile-sql table))
               (if-not (empty? columns)
                 (str " (" (first (apply join-stmt ", " columns)) ")"))
               (if-not (empty? values)
                 (let [columns (map as-identifier (keys (first values)))
                       template (str "(" (join ", " (repeat (count columns) "?")) ")")]
                   (str " (" (join ", " columns) ") VALUES "
                        (join ", " (repeat (count values) template)))))
               (if sql (str " " sql))
               (if default-values " DEFAULT VALUES")
               (if-not (empty? returning)
                 (apply str " RETURNING " (join ", " (map first returning)))))
          (if values (apply concat (map vals values)) args))))

(defmethod compile-sql :intersect [node]
  (compile-set-op :intersect node))

(defmethod compile-sql :join [{:keys [on using from how type outer]}]
  (let [[on-sql & on-args] (if on (compile-sql on))
        [from-sql & from-args] (compile-from from)
        using (if using (map compile-sql using))]
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
               (if using
                 (str " USING (" (join ", " (map first using)) ")")))
          (concat from-args on-args (mapcat rest using)))))

(defmethod compile-sql :keyword [{:keys [form]}]
  [(as-identifier form)])

(defmethod compile-sql :limit [{:keys [count]}]
  [(str "LIMIT " (if (number? count) count "ALL"))])

(defmethod compile-sql :like [{:keys [excluding including table]}]
  [(str "LIKE "
        (first (compile-sql table))
        (if-not (empty? including)
          (str " INCLUDING " (join " " (map keyword-sql including))))
        (if-not (empty? excluding)
          (str " EXCLUDING " (join " " (map keyword-sql excluding)))))])

(defmethod compile-sql :nil [_] ["NULL"])

(defmethod compile-sql :offset [{:keys [start]}]
  [(str "OFFSET " (if (number? start) start 0))])

(defmethod compile-sql :order-by [{:keys [exprs direction nulls using]}]
  (let [[sql & args] (compile-sql exprs)]
    (cons (str "ORDER BY " sql) args)))

(defmethod compile-sql :table [{:keys [as schema name]}]
  [(str (join "." (map as-identifier (remove nil? [schema name])))
        (if as (str " AS " (as-identifier as))))])

(defmethod compile-sql :distinct [{:keys [exprs on]}]
  (let [exprs (if exprs (map compile-sql exprs))
        on (if on (map compile-sql on))]
    (cons (str "DISTINCT "
               (if-not (empty? on)
                 (str "ON (" (join ", " (map first on)) ") "))
               (if-not (empty? exprs)
                 (join ", " (map first exprs))))
          (concat (mapcat rest on)
                  (mapcat rest exprs)))))

(defmethod compile-sql :select [{:keys [exprs distinct joins from where group-by limit offset order-by set]}]
  (let [[distinct-sql & distinct-args] (if distinct (compile-sql distinct))
        joins (if joins (map compile-sql joins))
        exprs (map compile-expr exprs)
        from (map compile-from from)
        where (map compile-sql where)
        group-by (map compile-sql group-by)
        order-by (map compile-sql order-by)
        set (map compile-sql set)]
    (cons (str "SELECT " (join ", " (map first exprs))
               distinct-sql
               (if-not (empty? from)
                 (str " FROM " (join ", " (map first from))))
               (if-not (empty? joins)
                 (str " " (join " " (map first joins))))
               (if-not (empty? where)
                 (str " WHERE " (join ", " (map first where))))
               (if-not (empty? group-by)
                 (str " GROUP BY " (join ", " (map first group-by))))
               (if-not (empty? order-by)
                 (str " ORDER BY " (join ", " (map first order-by))))
               (if limit (str " " (first (compile-sql limit))))
               (if offset (str " " (first (compile-sql offset))))
               (if-not (empty? set)
                 (str " " (join ", " (map first set)))))
          (concat (mapcat rest exprs)
                  (mapcat rest from)
                  (mapcat rest joins)
                  (mapcat rest where)
                  (mapcat rest group-by)
                  (mapcat rest order-by)
                  (mapcat rest set)))))

(defmethod compile-sql :default [{:keys [op]}]
  [(keyword-sql op)])

(defmethod compile-sql :truncate [{:keys [tables continue-identity restart-identity cascade restrict]}]
  (join-stmt
   " " ["TRUNCATE TABLE"]
   (apply join-stmt ", " tables)
   continue-identity restart-identity cascade restrict))

(defmethod compile-sql :union [node]
  (compile-set-op :union node))

(defmethod compile-sql :update [{:keys [where from exprs table row returning]}]
  (let [where (if where (map compile-sql where))
        columns (if row (map as-identifier (keys row)))
        exprs (if exprs (map (comp unwrap-stmt compile-expr) exprs))
        from (if from (map compile-from from))]
    (cons (str "UPDATE " (first (compile-sql table))
               " SET " (if row
                         (apply str (concat (interpose " = ?, " columns) " = ?"))
                         (join ", " (map first exprs)))
               (if from
                 (str " FROM " (join " " (map first from))))
               (if-not (empty? where)
                 (str " WHERE " (join ", " (map first where))))
               (if returning (apply str " RETURNING " (first (compile-sql (:exprs returning))))))
          (concat (vals row)
                  (mapcat rest (concat exprs from))
                  (mapcat rest where)))))

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