(ns sqlingvo.core
  (:refer-clojure :exclude [distinct group-by replace update])
  (:require [clojure.string :as str]
            [sqlingvo.compiler :as compiler]
            [sqlingvo.db :as db]
            [sqlingvo.expr :as expr]
            [sqlingvo.util :as util])
  (:import [sqlingvo.db Database]
           [sqlingvo.expr Stmt]))

(defn db?
  "Return true if `x` is a database, otherwise false."
  [x]
  (instance? Database x))

(defn chain-state [body]
  (util/m-seq (remove nil? body)))

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
      (let [column (expr/parse-column (str expr "." (name alias)))]
        (assoc column
               :as (->> (map column [:schema :table :name])
                        (remove nil?)
                        (map name)
                        (str/join "-")
                        (keyword)))))
    (assoc (expr/parse-expr expr) :as alias)))

(defn asc
  "Parse `expr` and return an ORDER BY expr using ascending order."
  [expr] (assoc (expr/parse-expr expr) :direction :asc))

(defn cascade
  "Add a CASCADE clause to an SQL statement."
  [condition]
  (util/conditional-clause :cascade condition))

(defn column
  "Add a column to `stmt`."
  [name type & {:as options}]
  (let [column (assoc options :op :column :name name :type type)
        column (update-in column [:default] #(if %1 (expr/parse-expr %1)))]
    (fn [stmt]
      [nil (-> (update-in stmt [:columns] #(vec (concat %1 [(:name column)])))
               (assoc-in [:column (:name column)]
                         (assoc column
                                :schema (:schema stmt)
                                :table (:name stmt))))])))

(defn continue-identity
  "Add a CONTINUE IDENTITY clause to an SQL statement."
  [condition]
  (util/conditional-clause :continue-identity condition))

(defn concurrently
  "Add a CONCURRENTLY clause to a SQL statement."
  [condition]
  (util/conditional-clause :concurrently condition))

(defn do-constraint
  "Add a DO CONSTRAINT clause to a SQL statement."
  [constraint]
  (util/set-val :do-constraint constraint))

(defn do-nothing
  "Add a DO NOTHING clause to a SQL statement."
  []
  (util/assoc-op :do-nothing))

(defn do-update
  "Add a DO UPDATE clause to a SQL statement."
  [expr]
  (util/assoc-op :do-update :expr (expr/parse-map-expr expr)))

(defn with-data
  "Add a WITH [NO] DATA clause to a SQL statement."
  [data?]
  (util/assoc-op :with-data :data data?))

(defn desc
  "Parse `expr` and return an ORDER BY expr using descending order."
  [expr]
  (assoc (expr/parse-expr expr) :direction :desc))

(defn distinct
  "Parse `exprs` and return a DISTINCT clause."
  [exprs & {:keys [on]}]
  (expr/make-node
   :op :distinct
   :children [:exprs :on]
   :exprs (expr/parse-exprs exprs)
   :on (expr/parse-exprs on)))

(defn delimiter
  "Add a DELIMITER clause to an SQL statement."
  [delimiter]
  (util/set-val :delimiter delimiter))

(defn encoding
  "Add a ENCODING clause to an SQL statement."
  [encoding]
  (util/set-val :encoding encoding))

(defn explain
  "Return an EXPLAIN statement for `stmt`. `opts` can be a map with
  the following key/value pairs:

   - :analyze boolean
   - :buffers boolean
   - :costs   boolean
   - :format  :json, :text, :yaml, :xml
   - :timing  boolean
   - :verbose boolean

  Examples:

  (explain db
    (select db [:*]
      (from :foo)))

  (explain db
    (select db [:*]
      (from :foo))
    {:analyze true})"
  {:style/indent 1}
  [db stmt & [opts]]
  {:pre [(db? db)]}
  (Stmt. (fn [_]
           [_ (expr/make-node
               :op :explain
               :db db
               :children [:stmt]
               :stmt (ast stmt)
               :opts opts)])))

(defn copy
  "Build a COPY statement.

  Examples:

  (copy db :country []
    (from :stdin))

  (copy db :country []
    (from \"/usr1/proj/bray/sql/country_data\"))"
  {:style/indent 3}
  [db table columns & body]
  {:pre [(db? db)]}
  (let [table (expr/parse-table table)
        columns (map expr/parse-column columns)]
    (Stmt. (fn [_]
             ((chain-state body)
              (expr/make-node
               :op :copy
               :db db
               :children [:table :columns]
               :table table
               :columns columns))))))

(defn create-table
  "Build a CREATE TABLE statement."
  {:style/indent 2}
  [db table & body]
  {:pre [(db? db)]}
  (let [table (expr/parse-table table)]
    (Stmt. (fn [_]
             ((chain-state body)
              (expr/make-node
               :op :create-table
               :db db
               :children [:table]
               :table table))))))

(defn delete
  "Build a DELETE statement.

  Examples:

  (delete db :continents)

  (delete db :continents
    (where '(= :id 1)))"
  {:style/indent 2}
  [db table & body]
  {:pre [(db? db)]}
  (let [table (expr/parse-table table)]
    (Stmt. (fn [_]
             ((chain-state body)
              (expr/make-node
               :op :delete
               :db db
               :children [:table]
               :table table))))))

(defn drop-table
  "Build a DROP TABLE statement.

  Examples:

  (drop-table db [:continents])

  (drop-table db [:continents :countries])"
  {:style/indent 2}
  [db tables & body]
  {:pre [(db? db)]}
  (let [tables (map expr/parse-table tables)]
    (Stmt. (fn [stmt]
             ((chain-state body)
              (expr/make-node
               :op :drop-table
               :db db
               :children [:tables]
               :tables tables))))))

(defn- make-set-op
  [op args]
  (let [[[opts] stmts] (split-with map? args)]
    (Stmt. (fn [_]
             [nil (merge
                   (expr/make-node
                    :op op
                    :db (-> stmts first ast :db)
                    :children [:stmts]
                    :stmts (map ast stmts))
                   opts)]))))

(defn except
  "Build an EXCEPT statement.

   Examples:

   (except
    (select db [1])
    (select db [2]))

   (except
    {:all true}
    (select db [1])
    (select db [2]))"
  [& args]
  (make-set-op :except args))

(defn from
  "Add a FROM clause to an SQL statement. The `from` forms can be one
  or more tables, :stdin, a filename or an other sub query.

  Examples:

  (select db [:*]
    (from :continents))

  (select db [:*]
    (from :continents :countries)
    (where '(= :continents.id :continent-id)))

  (select db [:*]
    (from (as (select [1 2 3]) :x)))

  (copy db :country []
    (from :stdin))

  (copy db :country []
    (from \"/usr1/proj/bray/sql/country_data\"))"
  [& from]
  (fn [stmt]
    (let [from (case (:op stmt)
                 :copy [(first from)]
                 (map expr/parse-from from))]
      [from (update-in stmt [:from] #(concat %1 from))])))

(defn group-by
  "Add a GROUP BY clause to an SQL statement."
  [& exprs]
  (util/concat-in [:group-by] (expr/parse-exprs exprs)))

(defn having
  "Add a HAVING clause to an SQL statement.

  Examples:

  (select db [:city '(max :temp-lo)]
    (from :weather)
    (group-by :city)
    (having '(< (max :temp-lo) 40)))"
  [condition & [combine]]
  (util/build-condition :having condition combine))

(defn if-exists
  "Add a IF EXISTS clause to an SQL statement."
  [condition]
  (util/conditional-clause :if-exists condition))

(defn if-not-exists
  "Add a IF EXISTS clause to an SQL statement."
  [condition]
  (util/conditional-clause :if-not-exists condition))

(defn inherits
  "Add an INHERITS clause to an SQL statement."
  [& tables]
  (let [tables (map expr/parse-table tables)]
    (fn [stmt]
      [tables (assoc stmt :inherits tables)])))

(defn insert
  "Build a INSERT statement."
  {:style/indent 3}
  [db table columns & body]
  {:pre [(db? db)]}
  (let [table (expr/parse-table table)
        columns (map expr/parse-column columns)]
    (Stmt. (fn [_]
             ((chain-state body)
              (expr/make-node
               :op :insert
               :db db
               :children [:table :columns]
               :table table
               :columns columns))))))

(defn intersect
  "Build an INTERSECT statement.

   Examples:

   (intersect
    (select db [1])
    (select db [2]))

   (intersect
    {:all true}
    (select db [1])
    (select db [2]))"
  [& args]
  (make-set-op :intersect args))

(defn join
  "Add a JOIN clause to a statement.

  Examples:

  (select db [:*]
    (from :countries)
    (join :continents '(using :id)))

  (select db [:*]
    (from :continents)
    (join :countries.continent-id :continents.id))

  (select db [:*]
    (from :countries)
    (join :continents '(on (= :continents.id :countries.continent-id))))"
  [from condition & {:keys [type outer pk]}]
  (util/concat-in
   [:joins]
   [(let [join (expr/make-node
                :op :join
                :children [:outer :type :from]
                :outer outer
                :type type
                :from (expr/parse-from from))]
      (cond
        (and (sequential? condition)
             (= :on (keyword (name (first condition)))))
        (assoc join
               :on (expr/parse-expr (first (rest condition))))
        (and (sequential? condition)
             (= :using (keyword (name (first condition)))))
        (assoc join
               :using (expr/parse-exprs (rest condition)))
        (and (keyword? from)
             (keyword? condition))
        (assoc join
               :from (expr/parse-table (str/join "." (butlast (str/split (name from) #"\."))))
               :on (expr/parse-expr `(= ~from ~condition)))
        :else (throw (ex-info "Invalid JOIN condition." {:condition condition}))))]))

(defn like
  "Add a LIKE clause to an SQL statement."
  [table & {:as opts}]
  (let [table (expr/parse-table table)
        like (assoc opts :op :like :table table)]
    (util/set-val :like like)))

(defn limit
  "Add a LIMIT clause to an SQL statement."
  [count]
  (util/assoc-op :limit :count count))

(defn nulls
  "Parse `expr` and return an NULLS FIRST/LAST expr."
  [expr where]
  (assoc (expr/parse-expr expr) :nulls where))

(defn on-conflict
  "Add a ON CONFLICT clause to a SQL statement."
  {:style/indent 1}
  [target & body]
  (let [target (map expr/parse-column target)]
    (let [[_ node]
          ((chain-state body)
           (expr/make-node
            :op :on-conflict
            :target target
            :children [:target]))]
      (Stmt. (fn [stmt]
               [_ (assoc stmt :on-conflict node)])))))

(defn on-conflict-on-constraint
  "Add a ON CONFLICT ON CONSTRAINT clause to a SQL statement."
  {:style/indent 1}
  [target & body]
  (let [[_ node]
        ((chain-state body)
         (expr/make-node
          :op :on-conflict-on-constraint
          :target target
          :children [:target]))]
    (Stmt. (fn [stmt]
             [_ (assoc stmt :on-conflict-on-constraint node)]))))

(defn offset
  "Add a OFFSET clause to an SQL statement."
  [start]
  (util/assoc-op :offset :start start))

(defn order-by
  "Add a ORDER BY clause to an SQL statement."
  [& exprs]
  (util/concat-in [:order-by] (expr/parse-exprs exprs)))

(defn window
  "Add a WINDOW clause to an SQL statement."
  [& exprs]
  (util/assoc-op :window :definitions (expr/parse-exprs exprs)))

(defn primary-key
  "Add a PRIMARY KEY clause to a table."
  [& keys]
  (fn [stmt]
    [nil (assoc stmt :primary-key keys)]))

(defn drop-materialized-view
  "Build a DROP MATERIALIZED VIEW statement.

  Examples:

  (drop-materialized-view db :order-summary)"
  {:style/indent 2}
  [db view & body]
  {:pre [(db? db)]}
  (let [view (expr/parse-table view)]
    (Stmt. (fn [_]
             ((chain-state body)
              (expr/make-node
               :op :drop-materialized-view
               :db db
               :children [:view]
               :view view))))))

(defn refresh-materialized-view
  "Build a REFRESH MATERIALIZED VIEW statement.

  Examples:

  (refresh-materialized-view db :order-summary)"
  {:style/indent 2}
  [db view & body]
  {:pre [(db? db)]}
  (let [view (expr/parse-table view)]
    (Stmt. (fn [_]
             ((chain-state body)
              (expr/make-node
               :op :refresh-materialized-view
               :db db
               :children [:view]
               :view view))))))

(defn restart-identity
  "Add a RESTART IDENTITY clause to an SQL statement."
  [condition]
  (util/conditional-clause :restart-identity condition))

(defn restrict
  "Add a RESTRICT clause to an SQL statement."
  [condition]
  (util/conditional-clause :restrict condition))

(defn returning
  "Add a RETURNING clause to an SQL statement.

  Examples:

  (insert db :distributors []
    (values [{:did 106 :dname \"XYZ Widgets\"}])
    (returning :*))

  (update db :films
    {:kind \"Dramatic\"}
    (where '(= :kind \"Drama\"))
    (returning :*))"
  [& exprs]
  (util/concat-in [:returning] (expr/parse-exprs exprs)))

(defn select
  "Build a SELECT statement.

  Examples:

  (select db [1])

  (select db [:*]
    (from :continents))

  (select db [:id :name]
    (from :continents))"
  {:style/indent 2}
  [db exprs & body]
  {:pre [(db? db)]}
  (let [[_ select]
        ((chain-state body)
         (expr/make-node
          :op :select
          :db db
          :children [:distinct :exprs]
          :distinct (if (= :distinct (:op exprs))
                      exprs)
          :exprs (if (sequential? exprs)
                   (expr/parse-exprs exprs))))]
    (Stmt. (fn [stmt]
             (->> (case (:op stmt)
                    :insert (assoc stmt :select select)
                    :select (assoc stmt :exprs (:exprs select))
                    select)
                  (repeat 2))))))

(defn temporary
  "Add a TEMPORARY clause to an SQL statement."
  [condition]
  (util/conditional-clause :temporary condition))

(defn truncate
  "Build a TRUNCATE statement.

  Examples:

  (truncate db [:continents])

  (truncate db [:continents :countries])"
  {:style/indent 2}
  [db tables & body]
  {:pre [(db? db)]}
  (let [tables (map expr/parse-table tables)]
    (Stmt. (fn [_]
             ((chain-state body)
              (expr/make-node
               :op :truncate
               :db db
               :children [:tables]
               :tables tables))))))

(defn union
  "Build a UNION statement.

   Examples:

   (union
    (select db [1])
    (select db [2]))

   (union
    {:all true}
    (select db [1])
    (select db [2]))"
  [& args]
  (make-set-op :union args))

(defn update
  "Build a UPDATE statement.

  Examples:

  (update db :films {:kind \"Dramatic\"}
    (where '(= :kind \"Drama\")))"
  {:style/indent 2}
  [db table row & body]
  {:pre [(db? db)]}
  (let [table (expr/parse-table table)
        exprs (if (sequential? row) (expr/parse-exprs row))
        row (if (map? row) (expr/parse-map-expr row))]
    (Stmt. (fn [_]
             ((chain-state body)
              (expr/make-node
               :op :update
               :db db
               :children [:table :exprs :row]
               :table table
               :exprs exprs
               :row row))))))

(defn values
  "Add a VALUES clause to an SQL statement.

  Examples:

  (insert db :distributors []
    (values [{:did 106 :dname \"XYZ Widgets\"}]))"
  [values]
  (if (= :default values)
    (util/set-val :default-values true)
    (util/concat-in [:values] (map expr/parse-map-expr (util/sequential values)))))

(defn where
  "Add a WHERE clause to an SQL statement.

  Examples:

  (select db [1]
    (where '(in 1 (1 2 3))))

  (select db [*]
    (from :continents)
    (where '(= :name \"Europe\")))

  (delete db :continents
    (where '(= :id 1)))"
  [condition & [combine]]
  (util/build-condition :where condition combine))

(defn with
  "Build a WITH (common table expressions) query."
  {:style/indent 2}
  [db bindings query]
  {:pre [(db? db)]}
  (assert (even? (count bindings)) "The WITH bindings must be even.")
  (let [bindings (map (fn [[name stmt]]
                        (vector (keyword name)
                                (ast stmt)))
                      (partition 2 bindings))
        query (ast query)]
    (Stmt. (fn [stmt]
             [nil (assoc query
                         :with (expr/make-node
                                :op :with
                                :db db
                                :children [:bindings]
                                :bindings bindings))]))))

(defn sql
  "Compile `stmt` into a clojure.java.jdbc compatible vector."
  [stmt]
  (compiler/compile-stmt (ast stmt)))

(defmethod print-method Stmt
  [stmt writer]
  (print-method (sql stmt) writer))
