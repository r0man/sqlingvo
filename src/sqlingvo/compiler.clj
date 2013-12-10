(ns sqlingvo.compiler
  (:refer-clojure :exclude [replace])
  (:require [clojure.core :as core]
            [clojure.java.io :refer [file]]
            [clojure.string :refer [blank? join replace upper-case]]
            [sqlingvo.vendor :refer [->postgresql sql-quote sql-name]]))

(defn to-sql [arg]
  (cond
   (string? arg)
   [arg]
   (sequential? arg)
   arg))

(defn concat-sql [& args]
  (->> (remove nil? args)
       (map to-sql)
       (reduce (fn [stmt [sql & args]]
                 (cons (apply str [(first stmt) sql])
                       (concat (rest stmt) args)))
               [])))

(defn join-sql [separator args]
  (let [args (map to-sql args)]
    (cons (join separator (remove blank? (map first args)))
          (apply concat (map rest args)))))

(defprotocol SQLType
  (sql-type [arg] "Convert `arg` into an SQL type."))

(extend-type Object
  SQLType
  (sql-type [obj] obj))

(defn compile-alias
  "Compile a SQL alias expression."
  [db alias]
  (if alias (str " AS " (sql-quote db alias))))

(defmulti compile-sql (fn [db ast] (:op ast)))

(defn keyword-sql [k]
  (replace (upper-case (name k)) #"-" " "))

(defn compiled? [arg]
  (and (sequential? arg) (string? (first arg))))

(defn wrap-stmt [stmt]
  (let [[sql & args] stmt]
    (cons (str "(" sql ")") args)))

(defn unwrap-stmt [stmt]
  (let [[sql & args] stmt]
    (cons (replace sql #"^\(|\)$" "") args)))

(defn- join-stmt [db separator & stmts]
  (let [stmts (map #(if (compiled? %1) %1 (compile-sql db %1)) (remove empty? stmts))
        stmts (remove (comp blank? first) stmts)]
    (cons (join separator (map first stmts))
          (apply concat (map rest stmts)))))

(defn- compile-set-op [db op {:keys [stmt all] :as node}]
  (concat-sql (upper-case (name op))
              (if all " ALL") " "
              (compile-sql db stmt) ))

;; COMPILE CONSTANTS

(defn compile-inline [db {:keys [form as]}]
  [(str form (compile-alias db as))])

(defmulti compile-const
  "Compile a SQL constant into a SQL statement."
  (fn [db node] (class (:form node))))

(defmethod compile-const clojure.lang.Symbol [db node]
  (compile-inline db node))

(defmethod compile-const Double [db node]
  (compile-inline db node))

(defmethod compile-const Long [db node]
  (compile-inline db node))

(defmethod compile-const :default [db {:keys [form as]}]
  [(str "?" (compile-alias db as)) (sql-type form)])

;; COMPILE EXPRESSIONS

(defmulti compile-expr
  "Compile a SQL expression."
  (fn [db ast] (:op ast)))

(defmethod compile-expr :array [db {:keys [children]}]
  (concat-sql "ARRAY[" (join-sql ", " (map #(compile-expr db %1) children)) "]"))

(defmethod compile-expr :select [db {:keys [as] :as expr}]
  (wrap-stmt (compile-sql db expr)))

(defmethod compile-expr :default [db node]
  (compile-sql db node))

;; COMPILE FN CALL

(defn compile-2-ary
  "Compile a 2-arity SQL function node into a SQL statement."
  [db {:keys [as args name] :as node}]
  (cond
   (> 2 (count args))
   (throw (ex-info "More than 1 arg needed." node))
   (= 2 (count args))
   (let [[[s1 & a1] [s2 & a2]] (map #(compile-expr db %1) args)]
     (cons (str "(" s1 " " (core/name name) " " s2 ")"
                (compile-alias db as))
           (concat a1 a2)))
   :else
   (join-sql " AND "
             (map #(compile-2-ary db (assoc node :args %1))
                  (partition 2 1 args)))))

(defn compile-infix
  "Compile a SQL infix function node into a SQL statement."
  [db {:keys [as args name]}]
  (cond
   (= 1 (count args))
   (compile-expr db (first args))
   :else
   (let [args (map #(compile-expr db %1) args)]
     (cons (str "(" (join (str " " (core/name name) " ") (map first args)) ")"
                (compile-alias db as))
           (apply concat (map rest args))))))

(defn compile-complex-args [db {:keys [as args name] :as node}]
  (concat-sql "(" (core/name name) " "
              (join-sql " " (map #(compile-sql db %1) args)) ")"
              (compile-alias db as)))

(defn compile-whitespace-args [db {:keys [as args name] :as node}]
  (concat-sql (core/name name)
              "(" (join-sql " " (map #(compile-sql db %1) args)) ")"
              (compile-alias db as)))

(defmulti compile-fn
  "Compile a SQL function node into a SQL statement."
  (fn [db node] (keyword (:name node))))

(defmethod compile-fn :cast [db {[expr type] :args}]
  (concat-sql "CAST(" (compile-expr db expr) " AS " (name (:name type)) ")"))

(defmethod compile-fn :count [db {:keys [args]}]
  (concat-sql "count("
              (if (= 'distinct (:form (first args))) "DISTINCT ")
              (join-sql ", " (map #(compile-sql db %1)
                                  (remove #(= 'distinct (:form %1)) args))) ")"))

(defmethod compile-fn :in [db {[member expr] :args}]
  (concat-sql (compile-expr db member) " IN "
              (if (and (= :list (:op expr))
                       (empty? (:children expr)))
                "(NULL)"
                (compile-expr db expr))))

(defmethod compile-fn :is-null [db {:keys [args]}]
  (concat-sql "(" (compile-expr db (first args)) " IS NULL)"))

(defmethod compile-fn :is-not-null [db {:keys [args]}]
  (concat-sql "(" (compile-expr db (first args)) " IS NOT NULL)"))

(defmethod compile-fn :range [db {:keys [args]}]
  (concat-sql "(" (join-sql ", " (map #(compile-expr db %1) args)) ")"))

(defmethod compile-fn :default [db {:keys [as args name]}]
  (concat-sql (sql-name db name)
              "(" (join-sql ", " (map #(compile-expr db %1) args)) ")"
              (compile-alias db as)))

;; COMPILE FROM CLAUSE

(defmulti compile-from (fn [db ast] (:op ast)))

(defmethod compile-from :fn [db fn]
  (compile-sql db fn))

(defmethod compile-from :select [db node]
  (let [[sql & args] (compile-sql db node)]
    (cons (str "(" sql ") AS " (sql-quote db (:as node))) args)))

(defmethod compile-from :table [db node]
  (compile-sql db node))

;; COMPILE SQL

(defmethod compile-sql :copy [db {:keys [columns delimiter encoding from to table]}]
  (concat-sql
   "COPY "
   (compile-sql db table)
   (if-not (empty? columns)
     (concat-sql " (" (join-sql ", " (map #(compile-sql db %1) columns)) ")"))
   " FROM "
   (let [from (first from)]
     (cond
      (instance? java.io.File from)
      ["?" (.getAbsolutePath from)]
      (string? from)
      ["?" from]
      (= :stdin from)
      "STDIN"))
   (if encoding
     [" ENCODING ?" encoding])
   (if delimiter
     [" DELIMITER ?" delimiter])))

(defn compile-column [db column]
  (concat-sql
   (sql-quote db (:name column))
   " " (replace (upper-case (name (:type column))) "-" " ")
   (if-let [length (:length column)]
     (str "(" length ")"))
   (if (:not-null? column)
     " NOT NULL")
   (if (:unique? column)
     " UNIQUE")
   (if (:primary-key? column)
     " PRIMARY KEY")
   (if-let [default (:default column)]
     (concat-sql " DEFAULT " (compile-sql db default)))))

(defmethod compile-sql :create-table [db {:keys [table if-not-exists inherits like primary-key temporary] :as node}]
  (let [columns (map (:column node) (:columns node))]
    (concat-sql
     "CREATE"
     (if temporary
       " TEMPORARY")
     " TABLE"
     (if if-not-exists
       " IF NOT EXISTS")
     (concat-sql " " (compile-sql db table))
     " ("
     (cond
      (not (empty? columns))
      (join-sql ", " (map #(compile-column db %1) columns))
      like
      (compile-sql db like))
     (if-not (empty? primary-key)
       (concat-sql ", PRIMARY KEY(" (join ", " (map #(sql-name db %1) primary-key)) ")"))
     ")"
     (if inherits
       (concat-sql " INHERITS (" (join-sql ", " (map #(compile-sql db %1) inherits)) ")")))))

(defmethod compile-sql :delete [db {:keys [where table returning]}]
  (concat-sql
   "DELETE FROM " (compile-sql db table)
   (if-not (empty? where)
     (concat-sql " WHERE " (compile-sql db where)))
   (if-not (empty? returning)
     (concat-sql " RETURNING " (join-sql ", " (map #(compile-sql db %1) returning)))) ))

(defmethod compile-sql :column [db {:keys [as schema name table direction nulls]}]
  (concat-sql
   (join "." (map #(sql-quote db %1) (remove nil? [schema table name])))
   (compile-alias db as)
   (if direction (str " " (upper-case (core/name direction))))
   (if nulls (str " NULLS " (keyword-sql nulls)))))

(defmethod compile-sql :column [db {:keys [as schema name table direction nulls]}]
  (concat-sql
   (->> [(if schema (sql-quote db schema))
         (if table (sql-quote db table))
         (if name (if (= :* name) "*" (sql-quote db name)))]
        (remove nil?)
        (join "."))
   (compile-alias db as)
   (if direction (str " " (upper-case (core/name direction))))
   (if nulls (str " NULLS " (keyword-sql nulls)))))

(defmethod compile-sql :constant [db node]
  (compile-const db node))

(defmethod compile-sql :condition [db {:keys [condition]}]
  (compile-sql db condition))

(defmethod compile-sql :drop-table [db {:keys [cascade if-exists restrict tables]}]
  (join-sql " " ["DROP TABLE"
                 (compile-sql db if-exists)
                 (join-sql ", " (map #(compile-sql db %1) tables))
                 (compile-sql db cascade)
                 (compile-sql db restrict)]))

(defmethod compile-sql :except [db node]
  (compile-set-op db :except node))

(defmethod compile-sql :expr-list [db {:keys [as children]}]
  (concat-sql (join-sql " " (map #(compile-sql db %1) children))
              (compile-alias db as)))

(defmethod compile-sql :fn [db node]
  (concat-sql (if-let [dir (:direction node)]
                (join-sql " " [(compile-fn db node) (upper-case (name dir))])
                (compile-fn db node))))

(defmethod compile-sql :from [db {:keys [clause joins]}]
  (concat-sql "FROM " (join-sql ", " (map #(compile-from db %1) clause))
              (if-not (empty? joins)
                (join-sql " " (map #(compile-sql db %1) joins)))))

(defmethod compile-sql :group-by [db {:keys [exprs]}]
  (concat-sql "GROUP BY" (compile-sql db exprs)))

(defmethod compile-sql :insert [db {:keys [table columns rows default-values values returning select]}]
  (let [columns (if (and (empty? columns)
                         (not (empty? values)))
                  (map (fn [k] {:op :column :name k})
                       (keys (first values)))
                  columns)]
    (concat-sql
     "INSERT INTO " (compile-sql db table)
     (if-not (empty? columns)
       (concat-sql " (" (join-sql ", " (map #(compile-sql db %1) columns)) ")"))
     (if-not (empty? values)
       (let [template (str "(" (join ", " (repeat (count columns) "?")) ")")]
         (concat-sql
          " VALUES "
          (join-sql
           ", "
           (for [value values]
             (cons template (map value (map :name columns))))))))
     (if select
       (concat-sql " " (compile-sql db select)))
     (if default-values
       " DEFAULT VALUES")
     (if-not (empty? returning)
       (concat-sql " RETURNING " (join-sql ", " (map #(compile-sql db %1) returning)))))))

(defmethod compile-sql :intersect [db node]
  (compile-set-op db :intersect node))

(defmethod compile-sql :join [db {:keys [on using from how type outer]}]
  (concat-sql
   (case type
     :cross "CROSS "
     :inner "INNER "
     :left "LEFT "
     :right "RIGHT "
     nil "")
   (if outer "OUTER ")
   "JOIN " (compile-from db from)
   (if on
     (concat-sql " ON " (compile-sql db on)))
   (if-not (empty? using)
     (concat-sql " USING (" (join-sql ", " (map #(compile-sql db %1) using)) ")"))))

(defmethod compile-sql :keyword [db {:keys [form]}]
  [(sql-quote db form)])

(defmethod compile-sql :limit [db {:keys [count]}]
  (concat-sql "LIMIT " (if (number? count) (str count) "ALL")))

(defmethod compile-sql :like [db {:keys [excluding including table]}]
  (concat-sql
   "LIKE "
   (compile-sql db table)
   (if-not (empty? including)
     (str " INCLUDING " (join " " (map keyword-sql including))))
   (if-not (empty? excluding)
     (str " EXCLUDING " (join " " (map keyword-sql excluding))))))

(defmethod compile-sql :nil [db _] ["NULL"])

(defmethod compile-sql :offset [db {:keys [start]}]
  (concat-sql "OFFSET " (if (number? start) (str start) "0")))

(defmethod compile-sql :order-by [db {:keys [exprs direction nulls using]}]
  (concat-sql "ORDER BY " (compile-sql db exprs)))

(defmethod compile-sql :table [db {:keys [as schema name]}]
  [(str (join "." (map #(sql-quote db %1) (remove nil? [schema name])))
        (compile-alias db as))])

(defmethod compile-sql :distinct [db {:keys [exprs on]}]
  (concat-sql
   "DISTINCT "
   (if-not (empty? on)
     (concat-sql "ON (" (join-sql ", " (map #(compile-sql db %1) on)) ") "))
   (join-sql ", " (map #(compile-sql db %1) exprs))))

(defmethod compile-sql :select [db {:keys [exprs distinct joins from where group-by limit offset order-by set]}]
  (concat-sql
   "SELECT " (join-sql ", " (map #(compile-expr db %1) exprs))
   (if distinct
     (compile-sql db distinct))
   (if-not (empty? from)
     (concat-sql " FROM " (join-sql ", " (map #(compile-from db %1) from))))
   (if-not (empty? joins)
     (concat-sql " " (join-sql " " (map #(compile-sql db %1) joins))))
   (if-not (empty? where)
     (concat-sql " WHERE " (compile-sql db where)))
   (if-not (empty? group-by)
     (concat-sql " GROUP BY " (join-sql ", " (map #(compile-sql db %1) group-by))))
   (if-not (empty? order-by)
     (concat-sql " ORDER BY " (join-sql ", " (map #(compile-sql db %1) order-by))))
   (if limit
     (concat-sql " " (compile-sql db limit)))
   (if offset
     (concat-sql " " (compile-sql db offset)))
   (if-not (empty? set)
     (concat-sql " " (join-sql ", " (map #(compile-sql db %1) set))))))

(defmethod compile-sql :if-exists [db {:keys [op]}]
  ["IF EXISTS"])

(defmethod compile-sql :cascade [db {:keys [op]}]
  ["CASCADE"])

(defmethod compile-sql :restrict [db {:keys [op]}]
  ["RESTRICT"])

(defmethod compile-sql :continue-identity [db {:keys [op]}]
  ["CONTINUE IDENTITY"])

(defmethod compile-sql :restart-identity [db {:keys [op]}]
  ["RESTART IDENTITY"])

(defmethod compile-sql nil [db {:keys [op]}]
  [])

(defmethod compile-sql :list [db {:keys [children]}]
  (concat-sql "(" (join-sql ", " (map #(compile-sql db %1) children)) ")"))

(defmethod compile-sql :truncate [db {:keys [tables continue-identity restart-identity cascade restrict]}]
  (join-sql " " ["TRUNCATE TABLE"
                 (join-sql ", " (map #(compile-sql db %1) tables))
                 (compile-sql db continue-identity)
                 (compile-sql db restart-identity)
                 (compile-sql db cascade)
                 (compile-sql db restrict)]))

(defmethod compile-sql :union [db node]
  (compile-set-op db :union node))

(defmethod compile-sql :update [db {:keys [where from exprs table row returning]}]
  (let [returning (map #(compile-sql db %1) returning)
        where (if where (compile-sql db where))
        columns (map #(sql-quote db %1) (keys row))
        exprs (map (comp unwrap-stmt #(compile-expr db %1)) exprs)
        from (map #(compile-from db %1) from)]
    (cons (str "UPDATE " (first (compile-sql db table))
               " SET " (if row
                         (apply str (concat (interpose " = ?, " columns) " = ?"))
                         (join ", " (map first exprs)))
               (if-not (empty? from)
                 (str " FROM " (join " " (map first from))))
               (if-not (empty? where)
                 (str " WHERE " (first where)))
               (if-not (empty? returning)
                 (apply str " RETURNING " (join ", " (map first returning)))))
          (concat (vals row)
                  (mapcat rest (concat exprs from))
                  (rest where)
                  (mapcat rest returning)))))

(defmethod compile-sql :with [db {:keys [bindings query]}]
  (concat-sql
   "WITH "
   (join-sql
    ", " (map (fn [alias stmt]
                (concat-sql (sql-name db alias) " AS (" (compile-sql db stmt) ")"))
              (map first bindings)
              (map second bindings)))
   " " (compile-sql db query)))

;; DEFINE SQL FN ARITY

(defmacro defarity
  "Define SQL functions in terms of `arity-fn`."
  [arity-fn & fns]
  `(do ~@(for [fn# (map keyword fns)]
           `(defmethod compile-fn ~fn# [db# ~'node]
              (~arity-fn db# ~'node)))))

(defarity compile-2-ary
  "=" "!=" "<>" "<" ">" "<=" ">=" "&&" "/" "^" "~" "~*" "like" "ilike")

(defarity compile-infix
  "+" "-" "*" "&" "!~" "!~*" "%" "and" "or" "union" "||" "overlaps" "@@")

(defarity compile-complex-args
  "partition")

(defarity compile-whitespace-args
  "substring"
  "trim")

(defn compile-stmt
  "Compile `stmt` into a clojure.java.jdbc compatible prepared
  statement vector."
  ([stmt]
     (compile-stmt (->postgresql) stmt))
  ([db stmt]
     (assert db "No db given!")
     (apply vector (compile-sql db stmt))))
