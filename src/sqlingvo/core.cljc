(ns sqlingvo.core
  (:refer-clojure :exclude [distinct group-by replace update])
  (:require [#?(:clj clojure.pprint :cljs cljs.pprint) :refer [simple-dispatch]]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [sqlingvo.compiler :as compiler]
            [sqlingvo.db :as db]
            [sqlingvo.expr :as expr]
            [sqlingvo.util :as util]))

(defn db
  "Return a new database for `spec`."
  [spec & [opts]]
  (db/db spec opts))

(defn db?
  "Return true if `x` is a database, otherwise false."
  [x]
  (instance? sqlingvo.db.Database x))

(defn chain-state [body]
  (util/m-seq (remove nil? body)))

(defn compose
  "Compose multiple SQL statements."
  [stmt & body]
  (expr/stmt (chain-state (cons stmt body))))

(defn excluded-keyword?
  "Returns true if the keyword `k` is prefixed with \"EXCLUDED.\",
  otherwise false."
  [k]
  (str/starts-with? (name k) "EXCLUDED."))

(defn excluded-keyword
  "Returns the keyword `k`, prefixed with \"EXCLUDED.\"."
  [k]
  (some->> k name (str "EXCLUDED.") keyword))

(s/fdef excluded-keyword
  :args (s/cat :k (s/nilable simple-keyword?))
  :ret (s/nilable excluded-keyword?))

(defn excluded-kw-map
  "Returns a map of EXCLUDED `ks` indexed by `ks`."
  [ks]
  (cond
    (map? ks)
    (excluded-kw-map (keys ks))
    (sequential? ks)
    (zipmap ks (map excluded-keyword ks))))

(s/fdef excluded-kw-map
  :args (s/cat :ks (s/nilable
                    (s/or :map (s/map-of keyword? any?)
                          :seq (s/coll-of simple-keyword?))))
  :ret (s/nilable (s/map-of simple-keyword? excluded-keyword?)))

(defn ast
  "Returns the abstract syntax tree of `stmt`."
  [stmt]
  (expr/ast stmt))

(defn as
  "Parse `expr` and return an expr with and AS clause using `alias`."
  [expr alias & [columns]]
  {:op :alias
   :children [:expr :name]
   :columns (mapv expr/parse-column columns)
   :expr (expr/parse-expr expr)
   :name alias})

(defn asc
  "Parse `expr` and return an ORDER BY expr using ascending order."
  [expr]
  {:op :direction
   :direction :asc
   :expr (expr/parse-expr expr)})

(defn cascade
  "Add a CASCADE clause to an SQL statement."
  [condition]
  (util/conditional-clause :cascade condition))

(defn check
  "Add a CHECK clause to an SQL statement."
  [expr]
  (fn [stmt]
    (let [expr {:op :check
                :expr (expr/parse-expr expr)}
          stmt (update-in stmt [:checks] #(conj (vec %) expr))]
      [expr stmt])))

(defn column
  "Add a column to `stmt`."
  [name type & {:as options}]
  (let [column (merge options (expr/parse-column name) {:type type})
        column (update-in column [:default] #(if %1 (expr/parse-expr %1)))]
    (fn [stmt]
      (let [column (-> (update-in stmt [:columns] #(vec (concat %1 [(:name column)])))
                       (assoc-in [:column (:name column)]
                                 (assoc column
                                        :schema (:schema stmt)
                                        :table (:name stmt))))]
        [column column]))))

(s/def ::not-null boolean?)
(s/def ::primary-key keyword?)

(s/def ::column-opts
  (s/keys* :opt-un [::not-null ::primary-key]))

(s/fdef column
  :args (s/cat :name :sqlingvo.column/name
               :type keyword?
               :opts ::column-opts))

(defn columns
  "Returns the columns of `table`."
  [table]
  (map (:column table) (:columns table)))

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
  {:op :direction
   :direction :desc
   :expr (expr/parse-expr expr)})

(defn distinct
  "Parse `exprs` and return a DISTINCT clause."
  [exprs & {:keys [on]}]
  (expr/make-node
   :op :distinct
   :children [:exprs :on]
   :exprs (expr/parse-exprs exprs)
   :on (expr/parse-exprs on)))

(defn inline-str
  "Compile `s` as an inline string, instead of a prepared statement
  parameter.

  WARNING: You have to make sure the string `s` is safe against SQL
  injection attacks yourself."
  [s]
  {:form s
   :inline? true
   :op :constant
   :type :string
   :val s})

(defn delimiter
  "Add a DELIMITER clause to an SQL statement."
  [delimiter]
  (util/set-val :delimiter delimiter))

(s/fdef delimiter
  :args (s/cat :delimiter string?))

(defn encoding
  "Add a ENCODING clause to an SQL statement."
  [encoding]
  (util/set-val :encoding encoding))

(s/fdef encoding
  :args (s/cat :encoding string?))

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
  (expr/stmt
   (fn [_]
     [_ (expr/make-node
         :op :explain
         :db (db/db db)
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
  (let [table (expr/parse-table table)
        columns (map expr/parse-column columns)]
    (expr/stmt
     (fn [_]
       ((chain-state body)
        (expr/make-node
         :op :copy
         :db (db/db db)
         :children [:table :columns]
         :table table
         :columns columns))))))

(defn create-schema
  "Build a CREATE SCHEMA statement."
  {:style/indent 2}
  [db schema & body]
  (let [schema (expr/parse-schema schema)]
    (expr/stmt
     (fn [_]
       ((chain-state body)
        (expr/make-node
         :op :create-schema
         :db (db/db db)
         :children [:schema]
         :schema schema))))))

(defn create-table
  "Build a CREATE TABLE statement."
  {:style/indent 2}
  [db table & body]
  (let [table (expr/parse-table table)]
    (expr/stmt
     (fn [_]
       ((chain-state body)
        (expr/make-node
         :op :create-table
         :db (db/db db)
         :children [:table]
         :table table))))))

(defn create-type
  "Build a CREATE TYPE sql statement."
  {:style/indent 2}
  [db type & body]
  (expr/stmt
   (fn [_]
     ((chain-state body)
      (expr/make-node
       :children [:name]
       :db (db/db db)
       :op :create-type
       :type (expr/parse-type type))))))

(defn enum
  "Returns the enum ast."
  [labels]
  (util/assoc-op
   :enum :labels (for [label labels]
                   {:op :enum-label
                    :children [:name]
                    :name (name label)})))

(defn delete
  "Build a DELETE statement.

  Examples:

  (delete db :continents)

  (delete db :continents
    (where '(= :id 1)))"
  {:style/indent 2}
  [db table & body]
  (let [table (expr/parse-table table)]
    (expr/stmt
     (fn [_]
       ((chain-state body)
        (expr/make-node
         :op :delete
         :db (db/db db)
         :children [:table]
         :table table))))))

(defn drop-schema
  "Build a DROP SCHEMA statement.

  Examples:

  (drop-schema db [:my-schema])"
  {:style/indent 2}
  [db schemas & body]
  (let [schemas (mapv expr/parse-schema schemas)]
    (expr/stmt
     (fn [_]
       ((chain-state body)
        (expr/make-node
         :children [:name]
         :db (db/db db)
         :op :drop-schema
         :schemas schemas))))))

(defn drop-table
  "Build a DROP TABLE statement.

  Examples:

  (drop-table db [:continents])

  (drop-table db [:continents :countries])"
  {:style/indent 2}
  [db tables & body]
  (let [tables (mapv expr/parse-table tables)]
    (expr/stmt
     (fn [stmt]
       ((chain-state body)
        (expr/make-node
         :op :drop-table
         :db (db/db db)
         :children [:tables]
         :tables tables))))))

(defn drop-type
  "Build a DROP TYPE statement.

  Examples:

  (drop-type db [:mood])

  (drop-table db [:my-schema.mood])"
  {:style/indent 2}
  [db types & body]
  (let [types (mapv expr/parse-type types)]
    (expr/stmt
     (fn [_]
       ((chain-state body)
        (expr/make-node
         :children [:name]
         :db (db/db db)
         :op :drop-type
         :types types))))))

(defn- make-set-op
  [op args]
  (let [[[opts] stmts] (split-with map? args)]
    (expr/stmt
     (fn [_]
       (->> (merge
             (expr/make-node
              :op op
              :db (-> stmts first ast :db)
              :children [:stmts]
              :stmts (map ast stmts))
             opts)
            (repeat 2))))))

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
  (let [tables (mapv expr/parse-table tables)]
    (fn [stmt]
      [tables (assoc stmt :inherits tables)])))

(defn insert
  "Build a INSERT statement."
  {:style/indent 3}
  [db table columns & body]
  (let [table (expr/parse-table table)
        columns (map expr/parse-column columns)]
    (expr/stmt
     (fn [_]
       ((chain-state body)
        (expr/make-node
         :op :insert
         :db (db/db db)
         :children [:table :columns]
         :table table
         :columns
         (when (not-empty columns)
           columns)))))))

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
  [expr]
  (if expr
    (util/assoc-op :limit :expr (expr/parse-expr expr))
    (util/dissoc-op :limit)))

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
      (expr/stmt
       (fn [stmt]
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
    (expr/stmt
     (fn [stmt]
       [_ (assoc stmt :on-conflict-on-constraint node)]))))

(defn offset
  "Add a OFFSET clause to an SQL statement."
  [expr]
  (if expr
    (util/assoc-op :offset :expr (expr/parse-expr expr))
    (util/dissoc-op :offset)))

(defn or-replace
  "Add an OR REPLACE clause to an SQL statement."
  [condition]
  (util/conditional-clause :or-replace condition))

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
    [nil (assoc stmt :primary-key (vec keys))]))

(defn create-materialized-view
  "Build a CREATE MATERIALIZED VIEW statement.

  Examples:

  (sql/create-materialized-view db :pseudo-source [:key :value]
    (sql/values [[\"a\" 1] [\"a\" 2] [\"a\" 3] [\"a\" 4] [\"b\" 5] [\"c\" 6] [\"c\" 7]]))
  "
  {:style/indent 3}
  [db view columns & body]
  (let [view (expr/parse-table view)]
    (expr/stmt
     (fn [_]
       ((chain-state body)
        (expr/make-node
         :op :create-materialized-view
         :db (db/db db)
         :children [:view]
         :columns (mapv expr/parse-column columns)
         :view view))))))

(defn drop-materialized-view
  "Build a DROP MATERIALIZED VIEW statement.

  Examples:

  (drop-materialized-view db :order-summary)"
  {:style/indent 2}
  [db view & body]
  (let [view (expr/parse-table view)]
    (expr/stmt
     (fn [_]
       ((chain-state body)
        (expr/make-node
         :op :drop-materialized-view
         :db (db/db db)
         :children [:view]
         :view view))))))

(defn drop-view
  "Build a DROP VIEW statement.

  Examples:

  (drop-view db :order-summary)"
  {:style/indent 2}
  [db view & body]
  (let [view (expr/parse-table view)]
    (expr/stmt
     (fn [_]
       ((chain-state body)
        (expr/make-node
         :op :drop-view
         :db (db/db db)
         :children [:view]
         :view view))))))

(defn refresh-materialized-view
  "Build a REFRESH MATERIALIZED VIEW statement.

  Examples:

  (refresh-materialized-view db :order-summary)"
  {:style/indent 2}
  [db view & body]
  (let [view (expr/parse-table view)]
    (expr/stmt
     (fn [_]
       ((chain-state body)
        (expr/make-node
         :op :refresh-materialized-view
         :db (db/db db)
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
  (let [[_ select]
        ((chain-state body)
         (expr/make-node
          :op :select
          :db (db/db db)
          :children [:distinct :exprs]
          :distinct (if (= :distinct (:op exprs))
                      exprs)
          :exprs (if (sequential? exprs)
                   (expr/parse-exprs exprs))))]
    (expr/stmt
     (fn [stmt]
       (->> (case (:op stmt)
              :create-materialized-view (assoc stmt :select select)
              :insert (assoc stmt :select select)
              :select (assoc stmt :exprs (:exprs select))
              select)
            (repeat 2))))))

(defn table
  "Make a new table and return it's AST."
  {:style/indent 1}
  [name & body]
  (ast (fn [table]
         [nil (merge
               table
               (second
                ((chain-state body)
                 (expr/parse-table name))))])))

(s/fdef table
  :args (s/cat :name :sqlingvo.table/name :body (s/* any?))
  :ret :sqlingvo/table)

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
  (let [tables (map expr/parse-table tables)]
    (expr/stmt
     (fn [_]
       ((chain-state body)
        (expr/make-node
         :op :truncate
         :db (db/db db)
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
  (let [table (expr/parse-table table)
        exprs (if (sequential? row) (expr/parse-exprs row))
        row (if (map? row) (expr/parse-map-expr row))]
    (expr/stmt
     (fn [_]
       ((chain-state body)
        (expr/make-node
         :op :update
         :db (db/db db)
         :children [:table :exprs :row]
         :table table
         :exprs exprs
         :row row))))))

(defn values
  "Return a VALUES statement or clause.

  Examples:

  (values db [[1 \"one\"] [2 \"two\"] [3 \"three\"]])

  (insert db :distributors []
    (values [{:did 106 :dname \"XYZ Widgets\"}]))"
  ([vals]
   (values nil vals))
  ([db vals]
   (expr/stmt
    (fn [stmt]
      (let [node (cond
                   (= vals :default)
                   {:op :values
                    :db (some-> db db/db)
                    :type :default}
                   (every? map? vals)
                   {:op :values
                    :db (some-> db db/db)
                    :columns (if (not-empty (:columns stmt))
                               (:columns stmt)
                               (->> (mapcat keys vals)
                                    (apply sorted-set)
                                    (mapv expr/parse-column)))
                    :type :records
                    :values (mapv expr/parse-map-expr vals)}
                   :else
                   {:op :values
                    :db (some-> db db/db)
                    :columns (:columns stmt)
                    :type :exprs
                    :values (mapv expr/parse-exprs vals)})]
        (->> (case (:op stmt)
               :create-materialized-view (assoc stmt :values node)
               :insert (assoc stmt :values node)
               node)
             (repeat 2)))))))

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
  (assert (even? (count bindings)) "The WITH bindings must be even.")
  (let [bindings (map (fn [[name stmt]]
                        (vector (keyword name)
                                (ast stmt)))
                      (partition 2 bindings))
        query (ast query)
        node (expr/make-node
              :op :with
              :db (db/db db)
              :children [:bindings]
              :bindings bindings
              :query query)]
    (expr/stmt
     (fn [stmt]
       [node (if stmt
               (assoc stmt :with node)
               node)]))))

(defn sql
  "Compile `stmt` into a clojure.java.jdbc compatible vector."
  [stmt]
  (compiler/compile-stmt (ast stmt)))

#?(:clj (defmethod print-method sqlingvo.expr.Stmt
          [stmt writer]
          (print-method (sql stmt) writer)))

#?(:cljs
   (extend-protocol IPrintWithWriter
     sqlingvo.expr.Stmt
     (-pr-writer [stmt writer opts]
       (-pr-writer (sql stmt) writer opts))))

;; Override deref in pprint
(defmethod simple-dispatch sqlingvo.expr.Stmt [stmt]
  (pr (sql stmt)))
