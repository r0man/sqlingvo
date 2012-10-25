(ns sqlingvo.core
  (:refer-clojure :exclude [group-by replace])
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [join replace split]]
            [sqlingvo.compiler :refer [compile-sql]]
            [sqlingvo.util :refer [parse-expr parse-exprs parse-table]]))

(defn sql
  "Compile `stmt` into a vector, where the first element is the
  SQL stmt and the rest are the prepared stmt arguments."
  [stmt] (compile-sql stmt))

(defmulti run
  "Run the SQL statement `stmt`."
  (fn [stmt] (:op stmt)))

(defmethod run :select [stmt]
  (jdbc/with-query-results  results
    (sql stmt)
    (doall results)))

(defmethod run :default [stmt]
  (apply jdbc/do-prepared (sql stmt)))

(deftype Stmt [content]

  clojure.lang.IPersistentMap
  (assoc [_ k v]
    (Stmt. (.assoc content k v)))

  (assocEx [_ k v]
    (Stmt. (.assocEx content k v)))

  (without [_ k]
    (Stmt. (.without content k)))

  java.lang.Iterable
  (iterator [this]
    (.iterator content))

  clojure.lang.Associative
  (containsKey [_ k]
    (.containsKey content k))

  (entryAt [_ k]
    (.entryAt content k))

  clojure.lang.IDeref
  (deref [this]
    (run this))

  clojure.lang.IPersistentCollection
  (count [_]
    (.count content))
  (cons [_ o]
    (Stmt. (.cons content o)))
  (empty [_]
    (.empty content))

  (equiv [this other]
    (and (isa? (class other) Stmt)
         (= (. this content)
            (. other content))))

  clojure.lang.Seqable
  (seq [_]
    (.seq content))

  clojure.lang.ILookup
  (valAt [_ k]
    (.valAt content k))

  (valAt [_ k not-found]
    (.valAt content k not-found))

  (toString [this]
    (first (sql this))))

(defmethod print-method Stmt [node writer]
  (print-dup (sql node) writer))

(defmethod print-dup Stmt [node writer]
  (print-dup (sql node) writer))

(defn make-stmt
  "Make a new AST node for `op`."
  [op & {:as opts}]
  (assoc (Stmt. (or opts {})) :op op))

(defn- parse-from [forms]
  (cond
   (keyword? forms)
   (parse-table forms)
   (and (map? forms) (= :select (:op forms)))
   forms
   :else (throw (IllegalArgumentException. (str "Can't parse FROM form: " forms)))))

(defn- wrap-seq [s]
  (if (sequential? s) s [s]))

(defn as
  "Add an AS clause to the SQL statement."
  [stmt as]
  (assoc (parse-expr stmt) :as as))

(defn except
  "Select the SQL set difference between `stmt-1` and `stmt-2`."
  [stmt-1 stmt-2 & {:keys [all]}]
  (make-stmt :except :children [stmt-1 stmt-2] :all all))

(defn drop-table
  "Drop the database `tables`."
  [tables & {:as opts}]
  (assoc opts
    :op :drop-table
    :tables (map parse-table (wrap-seq tables))))

(defn from
  "Add the FROM item to the SQL statement."
  [stmt & from]
  (assoc stmt
    :from {:op :from :from (map parse-from from)}))

(defn group-by
  "Add the GROUP BY clause to the SQL statement."
  [stmt & exprs]
  (assoc stmt
    :group-by {:op :group-by :exprs (parse-exprs exprs)}))

(defn intersect
  "Select the SQL set intersection between `stmt-1` and `stmt-2`."
  [stmt-1 stmt-2 & {:keys [all]}]
  (make-stmt :intersect :children [stmt-1 stmt-2] :all all))

(defn limit
  "Add the LIMIT clause to the SQL statement."
  [stmt count]
  (assoc stmt :limit {:op :limit :count count}))

(defn offset
  "Add the OFFSET clause to the SQL statement."
  [stmt start]
  (assoc stmt :offset {:op :offset :start start}))

(defn order-by
  "Add the ORDER BY clause to the SQL statement."
  [stmt exprs & {:as opts}]
  (assoc stmt
    :order-by
    (assoc opts
      :op :order-by
      :exprs (parse-exprs (wrap-seq exprs)))))

(defn select
  "Select `exprs` from the database."
  [& exprs]
  (make-stmt :select :exprs (parse-exprs exprs)))

(defn truncate
  "Truncate the database `tables`."
  [tables & {:as opts}]
  (assoc opts
    :op :truncate
    :tables (map parse-table (wrap-seq tables))))

(defn union
  "Select the SQL set union between `stmt-1` and `stmt-2`."
  [stmt-1 stmt-2 & {:keys [all]}]
  (make-stmt :union :children [stmt-1 stmt-2] :all all))

(defn where
  "Add the WHERE `condition` to the SQL statement."
  [stmt condition]
  (assoc stmt
    :condition
    {:op :condition
     :condition (parse-expr condition)}))
