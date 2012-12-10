(ns sqlingvo.core
  (:refer-clojure :exclude [distinct group-by])
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as s]
            [sqlingvo.compiler :refer [compile-stmt]]
            [sqlingvo.util :refer [parse-expr parse-exprs parse-column parse-table]]))

(defmacro sql
  "Compile `stmts` into a vector, where the first element is the SQL
  stmt and the rest are the prepared stmt arguments."
  [& stmts] `(compile-stmt (-> ~@stmts identity)))

(defn run-stmt
  "Compile and run `stmt` against the current clojure.java.jdbc
  database connection."
  [stmt]
  (let [compiled (compile-stmt stmt)]
    (if (or (= :select (:op stmt)) (:returning stmt))
      (jdbc/with-query-results results
        compiled (doall results))
      (map #(hash-map :count %1)
           (jdbc/do-prepared (first compiled) (rest compiled))))))

(defmacro run
  "Run `stmts` against the current clojure.java.jdbc database
  connection and return all rows."
  [& stmts] `(run-stmt (-> ~@stmts identity)))

(defmacro run1
  "Run `stmts` against the current clojure.java.jdbc database
  connection and return the first row."
  [& stmts] `(first (run ~@stmts)))

(defn- assoc-op [stmt op & {:as opts}]
  (assoc stmt op (assoc opts :op op)))

(defn- parse-from [forms]
  (cond
   (keyword? forms)
   (parse-table forms)
   (and (map? forms) (= :select (:op forms)))
   forms
   (and (map? forms) (:as forms))
   (assoc forms :op :table)
   :else (throw (IllegalArgumentException. (str "Can't parse FROM form: " forms)))))

(defn- wrap-seq [s]
  (if (vector? s) s [s]))

(defn as
  "Add an AS clause to the SQL statement."
  [stmt alias]
  (assoc (parse-expr stmt) :as alias))

(defn distinct
  "Add a DISTINCT clause to the SQL statement."
  [exprs & {:keys [on]}]
  {:op :distinct
   :exprs (parse-exprs exprs)
   :on (parse-exprs on)})

(defn create-table
  "Define a new table."
  [table]
  {:op :create-table :table (parse-table table)})

(defn copy
  "Copy data from or to a database table."
  [table & [columns]]
  {:op :copy
   :table (parse-table table)
   :columns (map parse-column columns)})

(defn delete
  "Delete rows of a database table."
  [table] {:op :delete :table (parse-table table)})

(defn except
  "Select the SQL set difference between `stmt-1` and `stmt-2`."
  [stmt-1 stmt-2 & {:keys [all]}]
  (update-in stmt-1 [:set] conj {:op :except :stmt stmt-2 :all all}))

(defn default-values
  "Add the DEFAULT VALUES clause to `stmt`."
  ([stmt]
     (default-values stmt true))
  ([stmt enabled]
     (assoc stmt :default-values enabled)))

(defn drop-table
  "Drop the database `tables`."
  [tables & {:as opts}]
  (merge opts {:op :drop-table :tables (map parse-table (wrap-seq tables))}))

(defn from
  "Add the FROM item to the SQL statement."
  [stmt & from]
  (condp = (:op stmt)
    :copy (assoc stmt :from (first from))
    (assoc-op stmt :from :clause (map parse-from from))))

(defn group-by
  "Add the GROUP BY clause to the SQL statement."
  [stmt & exprs]
  (assoc-op stmt :group-by :exprs (parse-exprs exprs)))

(defn inherits
  [stmt & tables]
  (assoc stmt :inherits (map parse-table tables)))

(defn insert
  "Insert rows into the database `table`."
  ([table]
     (insert table []))
  ([table what]
     (insert table nil what))
  ([table columns what]
     (let [stmt {:op :insert
                 :table (parse-table table)
                 :columns (map parse-column columns)}]
       (cond
        (sequential? what)
        (assoc stmt :rows what)
        (and (map? what)
             (= :select (:op what)))
        (assoc stmt :query what)))))

(defn if-not-exists
  "Add a IF NOT EXISTS clause to statement."
  [stmt bool] (assoc stmt :if-not-exists bool))

(defn intersect
  "Select the SQL set intersection between `stmt-1` and `stmt-2`."
  [stmt-1 stmt-2 & {:keys [all]}]
  (update-in stmt-1 [:set] conj {:op :intersect :stmt stmt-2 :all all}))

(defn join
  "Add a JOIN clause to the SQL statement."
  [stmt from [how & condition] & {:keys [type outer]}]
  (update-in
   stmt [:from :joins] conj
   {:op :join
    :from (parse-from from)
    :type type
    :how (keyword (name how))
    :condition (parse-exprs condition)
    :outer outer}))

(defn limit
  "Add the LIMIT clause to the SQL statement."
  [stmt count]
  (assoc-op stmt :limit :count count))

(defn like
  "Add the LIKE clause to a create table statement."
  [stmt table & {:keys [including excluding]}]
  (assoc stmt
    :like {:op :like
           :including including
           :excluding excluding
           :table (parse-table table)}))

(defn offset
  "Add the OFFSET clause to the SQL statement."
  [stmt start]
  (assoc-op stmt :offset :start start))

(defn order-by
  "Add the ORDER BY clause to the SQL statement."
  [stmt exprs & {:as opts}]
  (let [exprs (map #(if (string? %1)
                      (binding [*read-eval* false]
                        (parse-expr (read-string %1)))
                      %1) (wrap-seq exprs))
        exprs (parse-exprs exprs)]
    (assoc stmt :order-by (merge opts {:op :order-by :exprs exprs}))))

(defn returning
  "Add the RETURNING clause the SQL statement."
  [stmt exprs]
  (assoc-op stmt :returning :exprs (parse-exprs (wrap-seq exprs))))

(defn select
  "Select `exprs` from the database."
  [& exprs]
  {:op :select :exprs (parse-exprs exprs)})

(defn temporary
  "Create a temporary table."
  [stmt temporary]
  (assoc stmt :temporary temporary))

(defn truncate
  "Truncate the database `tables`."
  [tables & {:as opts}]
  (merge opts {:op :truncate :tables (map parse-table (wrap-seq tables))}))

(defn union
  "Select the SQL set union between `stmt-1` and `stmt-2`."
  [stmt-1 stmt-2 & {:keys [all]}]
  (update-in stmt-1 [:set] conj {:op :union :stmt stmt-2 :all all}))

(defn update
  "Update rows of the database `table`."
  [table row]
  {:op :update
   :table (parse-table table)
   :exprs (if (sequential? row) (map parse-expr row))
   :row (if (map? row) row)})

(defn where
  "Add the WHERE `condition` to the SQL statement."
  [stmt condition]
  (assoc-op stmt :condition :condition (parse-expr condition)))
