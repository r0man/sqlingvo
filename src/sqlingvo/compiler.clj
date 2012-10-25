(ns sqlingvo.compiler
  (:refer-clojure :exclude [replace])
  (:require [clojure.core :as core]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [blank? join replace upper-case]]))

(defmulti compile-sql :op)

(defn stmt? [arg]
  (and (sequential? arg) (string? (first arg))))

(defn- join-stmt [separator & stmts]
  (let [stmts (map #(if (stmt? %1) %1 (compile-sql %1)) stmts)
        stmts (remove (comp blank? first) stmts)]
    (->> (cons (join separator (map first stmts))
               (apply concat (map rest stmts)))
         (apply vector))))

(defn- stmt [& stmts]
  (apply join-stmt " " stmts))

(defn- compile-set-op [op {:keys [children all]}]
  (let [[[s1 a1] [s2 a2]] (map compile-sql children)]
    (cons (str s1 " " (upper-case (name op)) (if all " ALL") " (" s2 ")")
          (concat a1 a2))))

;; COMPILE CONSTANTS

(defmulti compile-const
  "Compile a SQL constant into a SQL statement."
  (fn [node] (class (:form node))))

(defmethod compile-const String [{:keys [form] :as const}]
  (conj (compile-const (assoc const :form '?)) form))

(defmethod compile-const :default [{:keys [form as]}]
  [(str form (if as (str " AS " (jdbc/as-identifier as))))])

;; COMPILE CONSTANTS

(defmulti compile-expr
  "Compile a SQL expression."
  :op)

(defmethod compile-expr :select [expr]
  (let [[sql & args] (compile-sql expr)]
    (cons (str "(" sql ")") args)))

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
   (let [[[s1 & a1] [s2 & a2]] (map compile-sql args)]
     (cons (str "(" s1 " " (core/name name) " " s2 ")")
           (concat a1 a2)))
   :else
   (apply join-stmt " AND "
          (map #(compile-2-ary (assoc node :args %1))
               (partition 2 1 args)))))

(defn compile-infix
  "Compile a SQL infix function node into a SQL statement."
  [{:keys [as args name]}]
  (let [args (map compile-sql args)]
    (cons (str "(" (join (str " " (core/name name) " ") (map first args)) ")"
               (if as (str " AS " (jdbc/as-identifier as))))
          (apply concat (map rest args)))))

(defmulti compile-fn
  "Compile a SQL function node into a SQL statement."
  (fn [node] (keyword (:name node))))

(defmethod compile-fn :default [{:keys [as args name]}]
  (let [args (map compile-sql args)]
    (cons (str (core/name name) "(" (join ", " (map first args)) ")"
               (if as (str " AS " (jdbc/as-identifier as))))
          (apply concat (map rest args)))))

;; COMPILE FROM CLAUSE

(defmulti compile-from :op)

(defmethod compile-from :select [node]
  (let [[sql & args] (compile-sql node)]
    (cons (str "(" sql ") AS " (jdbc/as-identifier (:as node))) args)))

(defmethod compile-from :table [node]
  (compile-sql node))

;; COMPILE SQL

(defmethod compile-sql nil [_]
  nil)

(defmethod compile-sql :column [{:keys [as schema name table]}]
  [(str (join "." (map jdbc/as-identifier (remove nil? [schema table name])))
        (if as (str " AS " (jdbc/as-identifier as))))])

(defmethod compile-sql :constant [node]
  (compile-const node))

(defmethod compile-sql :condition [{:keys [condition]}]
  (let [[sql & args] (compile-sql condition)]
    (cons (str "WHERE " sql) args)))

(defmethod compile-sql :drop-table [{:keys [cascade if-exists restrict tables]}]
  (let [[sql & args] (apply join-stmt ", " tables)]
    (cons (str "DROP TABLE " (if if-exists "IF EXISTS ") sql
               (if cascade " CASCADE")
               (if restrict " RESTRICT"))
          args)))

(defmethod compile-sql :except [node]
  (compile-set-op :except node))

(defmethod compile-sql :exprs [{:keys [children]}]
  (let [children (map compile-expr children)]
    (if (empty? children)
      ["*"]
      (cons (join ", " (map first children))
            (apply concat (map rest children))))))

(defmethod compile-sql :fn [node]
  (compile-fn node))

(defmethod compile-sql :from [{:keys [from]}]
  (let [from (map compile-from from)]
    (cons (str "FROM " (join ", " (map first from)))
          (apply concat (map rest from)))))

(defmethod compile-sql :group-by [{:keys [exprs]}]
  (stmt ["GROUP BY"] exprs))

(defmethod compile-sql :intersect [node]
  (compile-set-op :intersect node))

(defmethod compile-sql :keyword [{:keys [form]}]
  [(jdbc/as-identifier form)])

(defmethod compile-sql :limit [{:keys [count]}]
  [(str "LIMIT " (if (number? count) count "ALL"))])

(defmethod compile-sql :nil [_] ["NULL"])

(defmethod compile-sql :offset [{:keys [start]}]
  [(str "OFFSET " (if (number? start) start 0))])

(defmethod compile-sql :order-by [{:keys [exprs direction nulls using]}]
  (let [[sql & args] (compile-sql exprs)]
    (cons (str "ORDER BY " sql
               ({:asc " ASC" :desc " DESC"} direction)
               ({:first " NULLS FIRST" :last " NULLS LAST"} nulls))
          args)))

(defmethod compile-sql :table [{:keys [as schema name]}]
  [(str (join "." (map jdbc/as-identifier (remove nil? [schema name])))
        (if as (str " AS " (jdbc/as-identifier as))))])

(defmethod compile-sql :select [{:keys [exprs from condition group-by limit offset order-by]}]
  (stmt ["SELECT"] exprs from condition group-by order-by limit offset))

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

;; DEFINE SQL FN ARITY

(defmacro defarity
  "Define SQL functions in terms of `arity-fn`."
  [arity-fn & fns]
  `(do ~@(for [fn# (map keyword fns)]
           `(defmethod compile-fn ~fn# [~'node]
              (~arity-fn ~'node)))))

(defarity compile-2-ary
  := :!= :<> :< :> :<= :>= :!~ :!~* "/" "^" "~*" :like :ilike)

(defarity compile-infix
  :+ :* :& "%" :and :or :union)
