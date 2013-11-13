(ns sqlingvo.native
  (:refer-clojure :exclude [distinct group-by replace])
  (:require [clojure.string :as str]
            [inflections.core :refer [foreign-key hyphenize underscore]]
            [sqlingvo.compiler :refer [compile-stmt]]
            [sqlingvo.util :refer :all]
            [sqlingvo.vendor :as vendor])
  (:import sqlingvo.util.Stmt))

(defn m-bind [mv mf]
  (fn [state]
    (let [[temp-v temp-state] (mv state)
          new-mv (mf temp-v)]
      (new-mv temp-state))))

(defn m-result [x]
  (fn [state]
    [x state]))

(defn m-seq
  "'Executes' the monadic values in ms and returns a sequence of the
   basic values contained in them."
  [ms]
  (reduce (fn [q p]
            (m-bind p (fn [x]
                        (m-bind q (fn [y]
                                    (m-result (cons x y)))) )))
          (m-result '())
          (reverse ms)))

(defn sql-name [db x]
  (vendor/sql-name db x))

(defn sql-keyword [db x]
  (vendor/sql-keyword db x))

(defn sql-quote [db x]
  (vendor/sql-quote db x))

;; (defn chain-state [body]
;;   (with-monad state-m (m-seq (remove nil? body))))

(defn compose [stmt & body]
  (m-seq (remove nil? (cons stmt body))))

(defn ast
  "Returns the abstract syntax tree of `stmt`."
  [stmt]
  (cond
   (map? stmt)
   stmt
   (instance? Stmt stmt)
   (second ((.f stmt) nil))
   :else (second (stmt nil))))

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
    (assoc (parse-expr expr) :as alias)))

(defn asc
  "Parse `expr` and return an ORDER BY expr using ascending order."
  [expr] (assoc (parse-expr expr) :direction :asc))

(defn cascade
  "Returns a fn that adds a CASCADE clause to an SQL statement."
  [condition]
  (fn [stmt]
    (if condition
      [condition (assoc stmt :cascade {:op :cascade})]
      [condition (dissoc stmt :cascade)])))

(defn column
  "Add a column to `stmt`."
  [name type & {:as options}]
  (let [column (assoc options :op :column :name name :type type)
        column (update-in column [:default] #(if %1 (parse-expr %1)))]
    (fn [stmt]
      [nil (-> (update-in stmt [:columns] #(concat %1 [(:name column)]))
               (assoc-in [:column (:name column)]
                         (assoc column
                           :schema (:schema stmt)
                           :table (:name stmt))))])))

(defn continue-identity
  "Returns a fn that adds a CONTINUE IDENTITY clause to an SQL statement."
  [condition]
  (fn [stmt]
    (if condition
      [condition (assoc stmt :continue-identity {:op :continue-identity})]
      [condition (dissoc stmt :continue-identity)])))

(defn desc
  "Parse `expr` and return an ORDER BY expr using descending order."
  [expr] (assoc (parse-expr expr) :direction :desc))

(defn distinct
  "Parse `exprs` and return a DISTINCT clause."
  [exprs & {:keys [on]}]
  {:op :distinct
   :exprs (parse-exprs exprs)
   :on (parse-exprs on)})

(defn delimiter
  "Returns a fn that adds a DELIMITER clause to an SQL statement."
  [delimiter]
  (fn [stmt]
    [delimiter (assoc stmt :delimiter delimiter)]))

(defn encoding
  "Returns a fn that adds a ENCODING clause to an SQL statement."
  [encoding]
  (fn [stmt]
    [encoding (assoc stmt :encoding encoding)]))

(defn copy
  "Returns a fn that builds a COPY statement."
  [table columns & body]
  (let [table (parse-table table)
        columns (map parse-column columns)]
    (Stmt. (fn [_]
             ((m-seq (remove nil? body))
              {:op :copy :table table :columns columns})))))

(defn create-table
  "Returns a fn that builds a CREATE TABLE statement."
  [table & body]
  (let [table (parse-table table)]
    (Stmt. (fn [_]
             ((m-seq (remove nil? body))
              {:op :create-table :table table})))))

(defn delete
  "Returns a fn that builds a DELETE statement."
  [table & body]
  (let [table (parse-table table)]
    (Stmt. (fn [_]
             ((m-seq (remove nil? body))
              {:op :delete :table table})))))

(defn drop-table
  "Returns a fn that builds a DROP TABLE statement."
  [tables & body]
  (let [tables (map parse-table tables)]
    (Stmt. (fn [stmt]
             ((m-seq (remove nil? body))
              {:op :drop-table :tables tables})))))

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
    (let [from (case (:op stmt)
                 :copy [(first from)]
                 (map parse-from from))]
      [from (update-in stmt [:from] #(concat %1 from))])))

(defn group-by
  "Returns a fn that adds a GROUP BY clause to an SQL statement."
  [& exprs]
  (concat-val :group-by (parse-exprs exprs))
  (let [exprs (parse-exprs exprs)]
    (fn [stmt]
      [exprs (update-in stmt [:group-by] #(concat %1 exprs))])))

(defn if-exists
  "Returns a fn that adds a IF EXISTS clause to an SQL statement."
  [condition]
  (fn [stmt]
    (if condition
      [condition (assoc stmt :if-exists {:op :if-exists})]
      [condition (dissoc stmt :if-exists)])))

(defn if-not-exists
  "Returns a fn that adds a IF EXISTS clause to an SQL statement."
  [condition]
  (fn [stmt]
    (if condition
      [condition (assoc stmt :if-not-exists {:op :if-not-exists})]
      [condition (dissoc stmt :if-not-exists)])))

(defn inherits
  "Returns a fn that adds an INHERITS clause to an SQL statement."
  [& tables]
  (let [tables (map parse-table tables)]
    (fn [stmt]
      [tables (assoc stmt :inherits tables)])))

(defn insert
  "Returns a fn that builds a INSERT statement."
  [table columns & body]
  (let [table (parse-table table)
        columns (map parse-column columns)]
    (fn [_]
      ((m-seq (remove nil? body))
       {:op :insert :table table :columns columns}))))

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
          (= :on (keyword (name (first condition)))))
     (assoc join
       :on (parse-expr (first (rest condition))))
     (and (sequential? condition)
          (= :using (keyword (name (first condition)))))
     (assoc join
       :using (parse-exprs (rest condition)))
     (and (keyword? from)
          (keyword? condition))
     (assoc join
       :from (parse-table (str/join "." (butlast (str/split (name from) #"\."))))
       :on (parse-expr `(= ~from ~condition)))
     :else (throw (IllegalArgumentException. (format "Invalid JOIN condition: %s" condition))))))

(defn join
  "Returns a fn that adds a JOIN clause to an SQL statement."
  [from condition & {:keys [type outer pk]}]
  (let [join (make-join from condition :type type :outer outer :pk pk)]
    (fn [stmt]
      [join (update-in stmt [:joins] #(concat %1 [join]))])))

(defn like
  "Returns a fn that adds a LIKE clause to an SQL statement."
  [table & {:as opts}]
  (let [table (parse-table table)
        like (assoc opts :op :like :table table)]
    (fn [stmt]
      [table (assoc stmt :like like)])))

(defn limit
  "Returns a fn that adds a LIMIT clause to an SQL statement."
  [count]
  (let [limit {:op :limit :count count}]
    (fn [stmt]
      [limit (assoc stmt :limit limit)])))

(defn nulls
  "Parse `expr` and return an NULLS FIRST/LAST expr."
  [expr where] (assoc (parse-expr expr) :nulls where))

(defn offset
  "Returns a fn that adds a OFFSET clause to an SQL statement."
  [start]
  (let [offset {:op :offset :start start}]
    (fn [stmt]
      [offset (assoc stmt :offset offset)])))

(defn order-by
  "Returns a fn that adds a ORDER BY clause to an SQL statement."
  [& exprs]
  (let [exprs (parse-exprs exprs)]
    (fn [stmt]
      (if-not (empty? exprs)
        [exprs (update-in stmt [:order-by] #(concat %1 exprs))]
        [exprs stmt]))))

(defn restart-identity
  "Returns a fn that adds a RESTART IDENTITY clause to an SQL statement."
  [condition]
  (fn [stmt]
    (if condition
      [condition (assoc stmt :restart-identity {:op :restart-identity})]
      [condition (dissoc stmt :restart-identity)])))

(defn restrict
  "Returns a fn that adds a RESTRICT clause to an SQL statement."
  [condition]
  (fn [stmt]
    (if condition
      [condition (assoc stmt :restrict {:op :restrict})]
      [condition stmt])))

(defn returning
  "Returns a fn that adds a RETURNING clause to an SQL statement."
  [& exprs]
  (let [exprs (parse-exprs exprs)]
    (fn [stmt]
      [exprs (update-in stmt [:returning] #(concat %1 exprs))])))

(defn select
  "Returns a fn that builds a SELECT statement."
  [exprs & body]
  (let [[_ select]
        ((m-seq (remove nil? body))
         {:op :select
          :distinct (if (= :distinct (:op exprs))
                      exprs)
          :exprs (if (sequential? exprs)
                   (parse-exprs exprs))})]
    (Stmt. (fn [stmt]
             (case (:op stmt)
               nil [select select]
               :insert (repeat 2 (assoc stmt :select select)))))))

(defn temporary
  "Returns a fn that adds a TEMPORARY clause to an SQL statement."
  [condition]
  (fn [stmt]
    (if condition
      [condition (assoc stmt :temporary {:op :temporary})]
      [condition (dissoc stmt :temporary)])))

(defn truncate
  "Returns a fn that builds a TRUNCATE statement."
  [tables & body]
  (let [tables (map parse-table tables)]
    (Stmt. (fn [_]
             ((m-seq (remove nil? body))
              {:op :truncate :tables tables})))))

(defn union
  "Returns a fn that adds a UNION clause to an SQL statement."
  [stmt-2 & {:keys [all]}]
  (let [stmt-2 (ast stmt-2)]
    (fn [stmt-1]
      [nil (update-in stmt-1 [:set] conj {:op :union :stmt stmt-2 :all all})])))

(defn update
  "Returns a fn that builds a UPDATE statement."
  [table row & body]
  (let [table (parse-table table)
        exprs (if (sequential? row) (parse-exprs row))
        row (if (map? row) row)]
    (Stmt. (fn [_]
             ((m-seq (remove nil? body))
              {:op :update
               :table table
               :exprs exprs
               :row row})))))

(defn values
  "Returns a fn that adds a VALUES clause to an SQL statement."
  [values]
  (fn [stmt]
    (if (= :default values)
      [nil (assoc stmt :default-values true)]
      [nil (update-in stmt [:values] #(concat %1 (if (sequential? values) values [values])))])))

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
  ([stmt]
     (compile-stmt (ast stmt)))
  ([db stmt]
     (compile-stmt db (ast stmt))))

(defmethod print-method Stmt
  [stmt writer]
  (print-method (sql stmt) writer))
