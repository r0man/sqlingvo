(ns sqlingvo.core
  (:require [clojure.string :as str]
            [clojure.pprint :as pprint]
            [sqlingvo.compiler :refer [compile-stmt]]
            [sqlingvo.util :refer :all]
            [sqlingvo.db :as db])
  (:import (sqlingvo.util Stmt))
  (:refer-clojure :exclude [distinct group-by replace]))

(defn sql-name [db x]
  (db/sql-name db x))

(defn sql-keyword [db x]
  (db/sql-keyword db x))

(defn sql-quote [db x]
  (db/sql-quote db x))

(defn chain-state [body]
  (m-seq (remove nil? body)))

(defn compose
  "Compose multiple SQL statements."
  [stmt & body]
  (Stmt. (chain-state (cons stmt body))))

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
  (conditional-clause :cascade condition))

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
  (conditional-clause :continue-identity condition))

(defn desc
  "Parse `expr` and return an ORDER BY expr using descending order."
  [expr] (assoc (parse-expr expr) :direction :desc))

(defn distinct
  "Parse `exprs` and return a DISTINCT clause."
  [exprs & {:keys [on]}]
  (make-node
   :op :distinct
   :children [:exprs :on]
   :exprs (parse-exprs exprs)
   :on (parse-exprs on)))

(defn delimiter
  "Returns a fn that adds a DELIMITER clause to an SQL statement."
  [delimiter]
  (set-val :delimiter delimiter))

(defn encoding
  "Returns a fn that adds a ENCODING clause to an SQL statement."
  [encoding]
  (set-val :encoding encoding))

(defn copy
  "Returns a fn that builds a COPY statement.

Examples:

  (copy db :country []
    (from :stdin))
  ;=> [\"COPY \\\"country\\\" FROM STDIN\"]

  (copy db :country []
    (from \"/usr1/proj/bray/sql/country_data\"))
  ;=> [\"COPY \\\"country\\\" FROM ?\" \"/usr1/proj/bray/sql/country_data\"]
"
  [db table columns & body]
  (let [table (parse-table table)
        columns (map parse-column columns)]
    (Stmt. (fn [_]
             ((m-seq (remove nil? body))
              (make-node
               :op :copy
               :db db
               :children [:table :columns]
               :table table
               :columns columns))))))

(defn create-table
  "Returns a fn that builds a CREATE TABLE statement."
  [db table & body]
  (let [table (parse-table table)]
    (Stmt. (fn [_]
             ((m-seq (remove nil? body))
              (make-node
               :op :create-table
               :db db
               :children [:table]
               :table table))))))

(defn delete
  "Returns a fn that builds a DELETE statement.

Examples:

  (delete db :continents)
  ;=> [\"DELETE FROM \\\"continents\\\"\"]

  (delete db :continents
    (where '(= :id 1)))
  ;=> [\"DELETE FROM \\\"continents\\\" WHERE (\\\"id\\\" = 1)\"]
"
  [db table & body]
  (let [table (parse-table table)]
    (Stmt. (fn [_]
             ((m-seq (remove nil? body))
              (make-node
               :op :delete
               :db db
               :children [:table]
               :table table))))))

(defn drop-table
  "Returns a fn that builds a DROP TABLE statement.

Examples:

  (drop-table db [:continents])
  ;=> [\"DROP TABLE TABLE \\\"continents\\\"\"]

  (drop-table db [:continents :countries])
  ;=> [\"DROP TABLE TABLE \\\"continents\\\", \\\"countries\\\"\"]

"
  [db tables & body]
  (let [tables (map parse-table tables)]
    (Stmt. (fn [stmt]
             ((m-seq (remove nil? body))
              (make-node
               :op :drop-table
               :db db
               :children [:tables]
               :tables tables))))))

(defn except
  "Returns a fn that adds a EXCEPT clause to an SQL statement."
  [stmt-2 & {:keys [all]}]
  (let [stmt-2 (ast stmt-2)]
    (fn [stmt-1]
      [nil (update-in
            stmt-1 [:set] conj
            (make-node
             :op :except
             :children [:stmt :all]
             :stmt stmt-2
             :all all))])))

(defn from
  "Returns a fn that adds a FROM clause to an SQL statement. The
  `from` forms can be one or more tables, :stdin, a filename or an
  other sub query.

Examples:

  (select [:*]
    (from :continents))
  ;=> [\"SELECT * FROM \\\"continents\\\"\"]

  (select [:*]
    (from :continents :countries)
    (where '(= :continents.id :continent-id)))
  ;=> [\"SELECT * FROM \\\"continents\\\", \\\"countries\\\"
  ;=>  WHERE (\\\"continents\\\".\\\"id\\\" = \\\"continent_id\\\")\"]

  (select [*]
    (from (as (select [1 2 3]) :x)))
  ;=> [\"SELECT * FROM (SELECT 1, 2, 3) AS \\\"x\\\"\"]

  (copy :country []
    (from :stdin))
  ;=> [\"COPY \\\"country\\\" FROM STDIN\"]

  (copy :country []
    (from \"/usr1/proj/bray/sql/country_data\"))
  ;=> [\"COPY \\\"country\\\" FROM ?\" \"/usr1/proj/bray/sql/country_data\"]
"
  [& from]
  (fn [stmt]
    (let [from (case (:op stmt)
                 :copy [(first from)]
                 (map parse-from from))]
      [from (update-in stmt [:from] #(concat %1 from))])))

(defn group-by
  "Returns a fn that adds a GROUP BY clause to an SQL statement."
  [& exprs]
  (concat-in [:group-by] (parse-exprs exprs)))

(defn if-exists
  "Returns a fn that adds a IF EXISTS clause to an SQL statement."
  [condition]
  (conditional-clause :if-exists condition))

(defn if-not-exists
  "Returns a fn that adds a IF EXISTS clause to an SQL statement."
  [condition]
  (conditional-clause :if-not-exists condition))

(defn inherits
  "Returns a fn that adds an INHERITS clause to an SQL statement."
  [& tables]
  (let [tables (map parse-table tables)]
    (fn [stmt]
      [tables (assoc stmt :inherits tables)])))

(defn insert
  "Returns a fn that builds a INSERT statement."
  [db table columns & body]
  (let [table (parse-table table)
        columns (map parse-column columns)]
    (Stmt. (fn [_]
                ((m-seq (remove nil? body))
                 (make-node
                  :op :insert
                  :db db
                  :children [:table :columns]
                  :table table
                  :columns columns))))))

(defn intersect
  "Returns a fn that adds a INTERSECT clause to an SQL statement."
  [stmt-2 & {:keys [all]}]
  (let [stmt-2 (ast stmt-2)]
    (fn [stmt-1]
      [nil (update-in
            stmt-1 [:set] conj
            (make-node
             :op :intersect
             :children [:stmt :all]
             :stmt stmt-2
             :all all))])))

(defn join
  "Returns a fn that adds a JOIN clause to an SQL statement."
  [from condition & {:keys [type outer pk]}]
  (concat-in
   [:joins]
   [(let [join (make-node
                :op :join
                :children [:outer :type :from]
                :outer outer
                :type type
                :from (parse-from from))]
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
       :else (throw (ex-info "Invalid JOIN condition." {:condition condition}))))]))

(defn like
  "Returns a fn that adds a LIKE clause to an SQL statement."
  [table & {:as opts}]
  (let [table (parse-table table)
        like (assoc opts :op :like :table table)]
    (set-val :like like)))

(defn limit
  "Returns a fn that adds a LIMIT clause to an SQL statement."
  [count]
  (assoc-op :limit :count count))

(defn nulls
  "Parse `expr` and return an NULLS FIRST/LAST expr."
  [expr where] (assoc (parse-expr expr) :nulls where))

(defn offset
  "Returns a fn that adds a OFFSET clause to an SQL statement."
  [start]
  (assoc-op :offset :start start))

(defn order-by
  "Returns a fn that adds a ORDER BY clause to an SQL statement."
  [& exprs]
  (concat-in [:order-by] (parse-exprs exprs)))

(defn primary-key
  "Returns a fn that adds the primary key to a table."
  [& keys]
  (fn [stmt]
    (assert :table (:op stmt))
    [nil (assoc stmt :primary-key keys)]))

(defn refresh-materialized-view
  "Returns a fn that builds a UPDATE statement."
  [db view & body]
  (let [view (parse-table view)]
    (Stmt. (fn [_]
             ((m-seq (remove nil? body))
              (make-node
               :op :refresh-materialized-view
               :db db
               :children [:view]
               :view view))))))

(defn restart-identity
  "Returns a fn that adds a RESTART IDENTITY clause to an SQL statement."
  [condition]
  (conditional-clause :restart-identity condition))

(defn restrict
  "Returns a fn that adds a RESTRICT clause to an SQL statement."
  [condition]
  (conditional-clause :restrict condition))

(defn returning
  "Returns a fn that adds a RETURNING clause to an SQL statement."
  [& exprs]
  (concat-in [:returning] (parse-exprs exprs)))

(defn select
  "Returns a fn that builds a SELECT statement.

Examples:

  (select db [1])
  ;=> [\"SELECT 1\"]

  (select db [:*]
    (from :continents))
  ;=> [\"SELECT * FROM \\\"continents\\\"\"]

  (select db [:id :name]
    (from :continents))
  ;=> [\"SELECT \\\"id\\\", \\\"name\\\" FROM \\\"continents\\\"\"]

"
  [db exprs & body]
  (let [[_ select]
        ((m-seq (remove nil? body))
         (make-node
          :op :select
          :db db
          :children [:distinct :exprs]
          :distinct (if (= :distinct (:op exprs))
                      exprs)
          :exprs (if (sequential? exprs)
                   (parse-exprs exprs))))]
    (Stmt. (fn [stmt]
             (->> (case (:op stmt)
                    :insert (assoc stmt :select select)
                    :select (assoc stmt :exprs (:exprs select))
                    nil select)
                  (repeat 2))))))

(defn temporary
  "Returns a fn that adds a TEMPORARY clause to an SQL statement."
  [condition]
  (conditional-clause :temporary condition))

(defn truncate
  "Returns a fn that builds a TRUNCATE statement.

Examples:

  (truncate db [:continents])
  ;=> [\"TRUNCATE TABLE \\\"continents\\\"\"]

  (truncate db [:continents :countries])
  ;=> [\"TRUNCATE TABLE \\\"continents\\\", \\\"countries\\\"\"]
"
  [db tables & body]
  (let [tables (map parse-table tables)]
    (Stmt. (fn [_]
             ((m-seq (remove nil? body))
              (make-node
               :op :truncate
               :db db
               :children [:tables]
               :tables tables))))))

(defn union
  "Returns a fn that adds a UNION clause to an SQL statement."
  [stmt-2 & {:keys [all]}]
  (let [stmt-2 (ast stmt-2)]
    (fn [stmt-1]
      [nil (update-in
            stmt-1 [:set] conj
            (make-node
             :op :union
             :children [:stmt :all]
             :stmt stmt-2
             :all all))])))

(defn update
  "Returns a fn that builds a UPDATE statement.

Examples:

  (update db :films {:kind \"Dramatic\"}
    (where '(= :kind \"Drama\")))
  ;=> [\"UPDATE \\\"films\\\" SET \\\"kind\\\" = ? WHERE (\\\"kind\\\" = ?)\"
  ;=>  \"Dramatic\" \"Drama\"]
"
  [db table row & body]
  (let [table (parse-table table)
        exprs (if (sequential? row) (parse-exprs row))
        row (if (map? row) (parse-map-expr row))]
    (Stmt. (fn [_]
             ((m-seq (remove nil? body))
              (make-node
               :op :update
               :db db
               :children [:table :exprs :row]
               :table table
               :exprs exprs
               :row row))))))

(defn values
  "Returns a fn that adds a VALUES clause to an SQL statement."
  [values]
  (if (= :default values)
    (set-val :default-values true)
    (concat-in [:values] (map parse-map-expr (sequential values)))))

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
       [nil (assoc-in
             stmt [:where :condition]
             (make-node
              :op :condition
              :children [:condition]
              :condition (make-node
                          :op :fn
                          :children [:name :args]
                          :name combine
                          :args [(:condition (:where stmt))
                                 (:condition condition)])))]))))

(defn with
  "Returns a fn that builds a WITH (common table expressions) query."
  [db bindings query]
  (assert (even? (count bindings)) "The WITH bindings must be even.")
  (let [bindings (map (fn [[name stmt]]
                        (vector (keyword name)
                                (ast stmt)))
                      (partition 2 bindings))
        query (ast query)]
    (Stmt. (fn [stmt]
             [nil (assoc query
                    :with (make-node
                           :op :with
                           :db db
                           :children [:bindings]
                           :bindings bindings))]))))

(defn sql
  "Compile `stmt` into a clojure.java.jdbc compatible vector."
  [stmt]
  (let [ast (ast stmt)]
    (compile-stmt (:db ast) ast)))

(defmethod print-method Stmt
  [stmt writer]
  (print-method (sql stmt) writer))

(comment

  (insert :x [:a :b]
    (values [{:a 1 :b '(lower "B")}
             {:a 2 :b "b"}]))

  (select ["a"])

  (pprint (ast (values [{:a 1 :b '(lower "B")}
                        {:a 2 :b "b"}])))

  (insert :films [:name]
    (values {:name '(lower "X")}))

  (update :films {:name '(lower "X")}
    (where `(= :id 1)))

  (prn (parse-column :a))

  (pprint (insert :x [:a :b]
            (values [{:a 1 :b '(lower "x")}])))

  (def db (db/postgresql))

  (ast (select db [1 2 3]))

  )
