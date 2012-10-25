(ns sqlingvo.core
  (:refer-clojure :exclude [group-by replace])
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [join replace split]]
            [sqlingvo.compiler :refer [compile-sql]]
            [sqlingvo.util :refer [parse-expr parse-exprs parse-table]]))

(defn- parse-from [forms]
  (cond
   (keyword? forms)
   (parse-table forms)
   (and (map? forms) (= :select (:op forms)))
   forms
   :else (throw (IllegalArgumentException. (str "Can't parse FROM form: " forms)))))

(defn- wrap-seq [s]
  (if (sequential? s) s [s]))

(defn sql
  "Compile `stmt` into a vector, where the first element is the
  SQL stmt and the rest are the prepared stmt arguments."
  [stmt] (compile-sql stmt))

(defn as
  "Add an AS clause to the SQL statement."
  [stmt as]
  (assoc (parse-expr stmt) :as as))

(defn except
  "Select the SQL set difference between `stmt-1` and `stmt-2`."
  [stmt-1 stmt-2 & {:keys [all]}]
  {:op :except :children [stmt-1 stmt-2] :all all})

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
  {:op :intersect :children [stmt-1 stmt-2] :all all})

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
  {:op :select :exprs (parse-exprs exprs)})

(defn truncate
  "Truncate the database `tables`."
  [tables & {:as opts}]
  (assoc opts
    :op :truncate
    :tables (map parse-table (wrap-seq tables))))

(defn union
  "Select the SQL set union between `stmt-1` and `stmt-2`."
  [stmt-1 stmt-2 & {:keys [all]}]
  {:op :union :children [stmt-1 stmt-2] :all all})

(defn where
  "Add the WHERE `condition` to the SQL statement."
  [stmt condition]
  (assoc stmt
    :condition
    {:op :condition
     :condition (parse-expr condition)}))

(defmulti run
  "Run the SQL statement `stmt`."
  (fn [stmt] (:op stmt)))

(defmethod run :select [stmt]
  (jdbc/with-query-results  results
    (sql stmt)
    (doall results)))

(defmethod run :default [stmt]
  (apply jdbc/do-prepared (sql stmt)))
