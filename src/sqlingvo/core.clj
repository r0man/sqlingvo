(ns sqlingvo.core
  (:refer-clojure :exclude [distinct group-by replace])
  (:require [clojure.algo.monads :refer :all]
            [clojure.string :as str]
            [inflections.core :refer [foreign-key hyphenize underscore]]
            [sqlingvo.compiler :refer [compile-stmt]]
            [sqlingvo.util :refer :all]))

(defn chain-state [body]
  (with-monad state-m (m-seq (remove nil? body))))

(defn compose [stmt & body]
  (with-monad state-m
    (m-seq (remove nil? (cons stmt body)))))

(defn ast
  "Returns the abstract syntax tree of `stmt`."
  [stmt]
  (if (map? stmt)
    stmt (second (stmt nil))))

(defn as
  "Parse `expr` and return an expr with and AS clause using `alias`."
  [expr alias]
  (if (sequential? alias)
    (for [alias alias]
      (let [column (parse-column (str expr "." (name alias)))]
        (assoc column
          :as (->> (map column [:schema :table :name])
                   (remove nil?)
                   (map name)
                   (str/join "-")
                   (keyword)))))
    (assoc (parse-expr expr) :as (as-keyword alias))))

(defn asc
  "Parse `expr` and return an ORDER BY expr using ascending order."
  [expr] (assoc (parse-expr expr) :direction :asc))

(defn cascade
  "Returns a fn that adds a CASCADE clause to an SQL statement."
  [cascade?]
  (if cascade?
    (set-val :cascade {:op :cascade})
    (fetch-state)))

(defn column
  "Add a column to `stmt`."
  [name type & {:as options}]
  (let [column (assoc options :op :column :name name :type type)]
    (fn [stmt]
      [nil (-> (update-in stmt [:columns] #(concat %1 [(:name column)]))
               (assoc-in [:column (:name column)]
                         (assoc column
                           :schema (:schema stmt)
                           :table (:name stmt))))])))

(defn continue-identity
  "Returns a fn that adds a CONTINUE IDENTITY clause to an SQL statement."
  [continue-identity?]
  (if continue-identity?
    (set-val :continue-identity {:op :continue-identity})
    (fetch-state)))

(defn desc
  "Parse `expr` and return an ORDER BY expr using descending order."
  [expr] (assoc (parse-expr expr) :direction :desc))

(defn distinct
  "Parse `exprs` and return a DISTINCT clause."
  [exprs & {:keys [on]}]
  {:op :distinct
   :exprs (parse-exprs exprs)
   :on (parse-exprs on)})

(defn encoding
  "Returns a fn that adds a ENCODING clause to an SQL statement."
  [encoding]
  (set-val :encoding encoding))

(defn copy
  "Returns a fn that builds a COPY statement."
  [table columns & body]
  (fn [stmt]
    (with-monad state-m
      ((m-seq (remove nil? body))
       {:op :copy
        :table (parse-table table)
        :columns (map parse-column columns)}))))

(defn create-table
  "Returns a fn that builds a CREATE TABLE statement."
  [table & body]
  (fn [stmt]
    (with-monad state-m
      ((m-seq (remove nil? body))
       {:op :create-table
        :table (parse-table table)}))))

(defn delete
  "Returns a fn that builds a DELETE statement."
  [table & body]
  (fn [stmt]
    (with-monad state-m
      ((m-seq (remove nil? body))
       {:op :delete
        :table (parse-table table)}))))

(defn drop-table
  "Returns a fn that builds a DROP TABLE statement."
  [tables & body]
  (fn [stmt]
    (with-monad state-m
      ((m-seq (remove nil? body))
       {:op :drop-table
        :tables (map parse-table tables)}))))

(defn except
  "Returns a fn that adds a EXCEPT clause to an SQL statement."
  [stmt-2 & {:keys [all]}]
  (let [stmt-2 (ast stmt-2)]
    (fn [stmt-1]
      [nil (update-in stmt-1 [:set] conj {:op :except :stmt stmt-2 :all all})])))

(defn from
  "Returns a fn that adds a FROM clause to an SQL statement."
  [& from]
  (domonad state-m
    [op (fetch-val :op)
     from (concat-val
           :from (case op
                   :copy [(first from)]
                   (map parse-from from)))]
    from))

(defn group-by
  "Returns a fn that adds a GROUP BY clause to an SQL statement."
  [& exprs]
  (concat-val :group-by (parse-exprs exprs)))

(defn if-exists
  "Returns a fn that adds a IF EXISTS clause to an SQL statement."
  [if-exists?]
  (if if-exists?
    (set-val :if-exists {:op :if-exists})
    (fetch-state)))

(defn if-not-exists
  "Returns a fn that adds a IF EXISTS clause to an SQL statement."
  [if-not-exists?]
  (if if-not-exists?
    (set-val :if-not-exists {:op :if-not-exists})
    (fetch-state)))

(defn inherits
  "Returns a fn that adds an INHERITS clause to an SQL statement."
  [& tables]
  (set-val :inherits (map parse-table tables)))

(defn insert
  "Returns a fn that builds a INSERT statement."
  [table columns & body]
  (fn [stmt]
    (with-monad state-m
      ((m-seq (remove nil? body))
       {:op :insert
        :table (parse-table table)
        :columns (map parse-column columns)}))))

(defn intersect
  "Returns a fn that adds a INTERSECT clause to an SQL statement."
  [stmt-2 & {:keys [all]}]
  (let [stmt-2 (ast stmt-2)]
    (fn [stmt-1]
      [nil (update-in stmt-1 [:set] conj {:op :intersect :stmt stmt-2 :all all})])))

(defn- make-join [from condition & {:keys [type outer pk]}]
  (let [join {:op :join
              :from (parse-from from)
              :type type
              :outer outer}]
    (cond
     (and (sequential? condition)
          (= 'on (first condition)))
     (assoc join
       :on (parse-expr (first (rest condition))))
     (and (sequential? condition)
          (= 'using (first condition)))
     (assoc join
       :using (parse-exprs (rest condition)))
     (and (keyword? from)
          (keyword? condition))
     (assoc join
       :from (parse-table (str/join "." (butlast (str/split (name from) #"\."))))
       :on (parse-expr
            `(= ~(as-keyword (parse-column from))
                ~(as-keyword (parse-column condition)))))
     :else (throw (IllegalArgumentException. (format "Invalid JOIN condition: %s" condition))))))

(defn join
  "Returns a fn that adds a JOIN clause to an SQL statement."
  [from condition & {:keys [type outer pk]}]
  (let [join (make-join from condition :type type :outer outer :pk pk)]
    (concat-val :joins [join])))

(defn like
  "Returns a fn that adds a LIKE clause to an SQL statement."
  [table & {:as opts}]
  (set-val :like (assoc opts :op :like :table (parse-table table))))

(defn limit
  "Returns a fn that adds a LIMIT clause to an SQL statement."
  [count]
  (set-val :limit {:op :limit :count count}))

(defn nulls
  "Parse `expr` and return an NULLS FIRST/LAST expr."
  [expr where] (assoc (parse-expr expr) :nulls where))

(defn offset
  "Returns a fn that adds a OFFSET clause to an SQL statement."
  [start]
  (with-monad state-m
    (set-val :offset {:op :offset :start start})))

(defn order-by
  "Returns a fn that adds a ORDER BY clause to an SQL statement."
  [& exprs]
  (let [exprs (parse-exprs exprs)]
    (if-not (empty? exprs)
      (concat-val :order-by exprs)
      (fetch-state))))

(defn restart-identity
  "Returns a fn that adds a RESTART IDENTITY clause to an SQL statement."
  [restart-identity?]
  (if restart-identity?
    (set-val :restart-identity {:op :restart-identity})
    (fetch-state)))

(defn restrict
  "Returns a fn that adds a RESTRICT clause to an SQL statement."
  [restrict?]
  (if restrict?
    (set-val :restrict {:op :restrict})
    (fetch-state)))

(defn returning
  "Returns a fn that adds a RETURNING clause to an SQL statement."
  [& exprs]
  (concat-val :returning (parse-exprs exprs)))

(defn select
  "Returns a fn that builds a SELECT statement."
  [exprs & body]
  (let [[_ select]
        ((chain-state body)
         {:op :select
          :distinct (if (= :distinct (:op exprs))
                      exprs)
          :exprs (if (sequential? exprs)
                   (parse-exprs exprs))})]
    (fn [stmt]
      (case (:op stmt)
        nil [select select]
        :insert (repeat 2 (assoc stmt :select select))))))

(defn temporary
  "Returns a fn that adds a TEMPORARY clause to an SQL statement."
  [temporary?]
  (if temporary?
    (set-val :temporary {:op :temporary})
    (fetch-state)))

(defn truncate
  "Returns a fn that builds a TRUNCATE statement."
  [tables & body]
  (let [[_ truncate]
        ((chain-state body)
         {:op :truncate
          :tables (map parse-table tables)})]
    (fn [stmt]
      [truncate truncate])))

(defn union
  "Returns a fn that adds a UNION clause to an SQL statement."
  [stmt-2 & {:keys [all]}]
  (let [stmt-2 (ast stmt-2)]
    (fn [stmt-1]
      [nil (update-in stmt-1 [:set] conj {:op :union :stmt stmt-2 :all all})])))

(defn update
  "Returns a fn that builds a UPDATE statement."
  [table row & body]
  (fn [stmt]
    (with-monad state-m
      ((chain-state body)
       {:op :update
        :table (parse-table table)
        :exprs (if (sequential? row) (parse-exprs row))
        :row (if (map? row) row)}))))

(defn values
  "Returns a fn that adds a VALUES clause to an SQL statement."
  [values]
  (case values
    :default (set-val :default-values true)
    (concat-val :values (if (sequential? values) values [values]))))

(defn where
  "Returns a fn that adds a WHERE clause to an SQL statement."
  [condition & [combine]]
  (let [condition (parse-condition condition)]
    (fn [stmt]
      (cond
       (or (nil? combine)
           (nil? (:condition (:where stmt))))
       [nil (assoc stmt :where condition)]
       :else
       [nil (assoc-in stmt [:where :condition]
                      {:op :condition
                       :condition {:op :fn
                                   :name combine
                                   :args [(:condition (:where stmt))
                                          (:condition condition)]}})]))))

(defn sql
  "Compile `stmt` into a clojure.java.jdbc compatible vector."
  [stmt & {:keys [entities]}]
  (compile-stmt (ast stmt) :entities entities))
