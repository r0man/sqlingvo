(ns sqlingvo.compiler
  (:import java.io.File)
  (:refer-clojure :exclude [replace])
  (:require [clojure.core :as core]
            [clojure.java.io :refer [file]]
            [clojure.string :refer [blank? join replace upper-case]]))

(defprotocol Keywordable
  (sql-keyword [obj x]))

(defprotocol Nameable
  (sql-name [obj x]))

(defprotocol Quoteable
  (sql-quote [obj x]))

(defmulti compile-sql
  "Compile the `ast` into SQL."
  (fn [db ast] (:op ast)))

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

(defn compile-sql-join [db separator args]
  (join-sql separator (map #(compile-sql db %) args)))

(defn compile-alias
  "Compile a SQL alias expression."
  ([db alias]
   (compile-alias db alias true))
  ([db alias include-as?]
   (when alias
     (if include-as?
       (str " AS " (sql-quote db alias))
       (str " " (sql-quote db alias))))))

(defn keyword-sql [k]
  (replace (upper-case (name k)) #"-" " "))

(defn wrap-stmt [stmt]
  (let [[sql & args] stmt]
    (cons (str "(" sql ")") args)))

(defn unwrap-stmt [stmt]
  (let [[sql & args] stmt]
    (cons (replace sql #"^\(|\)$" "") args)))

(defn- compile-set-op [db op {:keys [stmt all] :as node}]
  (concat-sql (upper-case (name op))
              (if all " ALL") " "
              (compile-sql db stmt) ))

;; COMPILE CONSTANTS

(defn compile-inline [db node]
  [(str (:val node) (compile-alias db (:as node)))])

(defmulti compile-const
  "Compile a SQL constant into a SQL statement."
  (fn [db node] (:type node)))

(defmethod compile-const :number [db node]
  (compile-inline db node))

(defmethod compile-const :string [db node]
  [(str "?" (compile-alias db (:as node))) (:val node)])

(defmethod compile-const :symbol [db node]
  (compile-inline db node))

(defmethod compile-const :default [db node]
  [(str "?" (compile-alias db (:as node))) (:form node)])

;; COMPILE EXPRESSIONS

(defmulti compile-expr
  "Compile a SQL expression."
  (fn [db ast] (:op ast)))

(defmethod compile-expr :array [db {:keys [children]}]
  (concat-sql "ARRAY[" (compile-sql-join db ", " children) "]"))

(defmethod compile-expr :select [db {:keys [as] :as expr}]
  (concat-sql (wrap-stmt (compile-sql db expr))
              (if as (compile-alias db as))))

(defmethod compile-expr :default [db node]
  (compile-sql db node))

(defn compile-exprs [db exprs]
  (map #(compile-expr db %1) exprs))

;; COMPILE FN CALL

(defn compile-2-ary
  "Compile a 2-arity SQL function node into a SQL statement."
  [db {:keys [as args name] :as node}]
  (cond
    (> 2 (count args))
    (throw (ex-info "More than 1 arg needed." node))
    (= 2 (count args))
    (let [[[s1 & a1] [s2 & a2]] (compile-exprs db args)]
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
    (let [args (compile-exprs db args)]
      (cons (str "(" (join (str " " (core/name name) " ") (map first args)) ")"
                 (compile-alias db as))
            (apply concat (map rest args))))))

(defn compile-complex-args [db node]
  (concat-sql "(" (name (:name node)) " "
              (compile-sql-join db " " (:args node))
              ")" (compile-alias db (:as node))))

(defn compile-whitespace-args [db node]
  (concat-sql (name (:name node)) "("
              (compile-sql-join db " " (:args node))
              ")" (compile-alias db (:as node))))

(defmulti compile-fn
  "Compile a SQL function node into a SQL statement."
  (fn [db node] (keyword (:name node))))

(defmethod compile-fn :case [db node]
  (let [parts (partition 2 2 nil (:args node))]
    (concat-sql (apply concat-sql "CASE"
                       (concat (for [[test then] (filter #(= 2 (count %1)) parts)]
                                 (concat-sql " WHEN "
                                             (compile-sql db test) " THEN "
                                             (compile-sql db then)))
                               (for [[else] (filter #(= 1 (count %1)) parts)]
                                 (concat-sql " ELSE " (compile-sql db else)))
                               [" END"]))
                (compile-alias db (:as node)))))

(defmethod compile-fn :cast [db {[expr type] :args as :as}]
  (concat-sql "CAST(" (compile-expr db expr) " AS " (name (:name type)) ")"
              (compile-alias db as)))

(defmethod compile-fn :count [db {:keys [args as]}]
  (concat-sql "count("
              (if (= 'distinct (:form (first args))) "DISTINCT ")
              (join-sql ", " (map #(compile-sql db %1)
                                  (remove #(= 'distinct (:form %1)) args))) ")"
                                  (compile-alias db as)))

(defmethod compile-fn :in [db {[member expr] :args}]
  (concat-sql (compile-expr db member) " IN "
              (if (and (= :list (:op expr))
                       (empty? (:children expr)))
                "(NULL)"
                (compile-expr db expr))))

(defmethod compile-fn :exists [db {:keys [args]}]
  (concat-sql "(EXISTS " (compile-expr db (first args)) ")"))

(defmethod compile-fn :not [db {:keys [args]}]
  (concat-sql "(NOT " (compile-expr db (first args)) ")"))

(defmethod compile-fn :not-exists [db {:keys [args]}]
  (concat-sql "(NOT EXISTS " (compile-expr db (first args)) ")"))

(defmethod compile-fn :is-null [db {:keys [args]}]
  (concat-sql "(" (compile-expr db (first args)) " IS NULL)"))

(defmethod compile-fn :is-not-null [db {:keys [args]}]
  (concat-sql "(" (compile-expr db (first args)) " IS NOT NULL)"))

(defmethod compile-fn :not-like [db {:keys [args]}]
  (let [[string pattern] (compile-exprs db args)]
    (concat-sql "(" string " NOT LIKE " pattern ")" )))

(defmethod compile-fn :range [db {:keys [args]}]
  (concat-sql "(" (compile-sql-join db ", " args) ")"))

(defmethod compile-fn :row [db {:keys [args]}]
  (concat-sql "ROW(" (join-sql ", " (compile-exprs db args)) ")"))

(defmethod compile-fn :over [db node]
  (let [args (map #(compile-sql db %) (:args node))]
    (concat-sql (first args) " OVER ("
                (join-sql " " (rest args))
                ")" (compile-alias db (:as node)))))

(defmethod compile-fn :partition-by [db node]
  (let [[expr & more-args] (:args node)]
    (concat-sql "PARTITION BY "
                (if (= :array (:op expr))
                  (compile-sql-join db ", " (:children expr))
                  (compile-expr db expr))
                (when (seq more-args)
                  (concat-sql " " (compile-sql-join db " " more-args))))))

(defmethod compile-fn :order-by [db node]
  (concat-sql "ORDER BY " (compile-sql-join db ", " (:args node))))

(defn- compile-direction [db node]
  (concat-sql (compile-sql db (first (:args node))) " "
              (upper-case (name (:name node)))))

(defmethod compile-fn :asc [db node]
  (compile-direction db node))

(defmethod compile-fn :desc [db node]
  (compile-direction db node))

(defmethod compile-fn :default [db {:keys [as args name]}]
  (concat-sql (sql-quote db name) "("
              (join-sql ", " (compile-exprs db args))
              ")" (compile-alias db as)))

;;  WITH Queries (Common Table Expressions)

(defn compile-with [db node compiled-statement]
  (if-let [bindings (:bindings node)]
    (concat-sql
     "WITH "
     (join-sql
      ", " (map (fn [alias stmt]
                  (concat-sql (sql-name db alias) " AS (" (compile-sql db stmt) ")"))
                (map first bindings)
                (map second bindings)))
     " " compiled-statement)
    compiled-statement))

;; COMPILE FROM CLAUSE

(defmulti compile-from (fn [db ast] (:op ast)))

(defmethod compile-from :fn [db fn]
  (compile-sql db fn))

(defmethod compile-from :select [db node]
  (let [[sql & args] (compile-sql db node)]
    (cons (str "(" sql ") AS " (sql-quote db (:as node))) args)))

(defmethod compile-from :table [db node]
  (compile-sql db node))

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

;; COMPILE SQL

(defmethod compile-sql :cascade [db node]
  ["CASCADE"])

(defmethod compile-sql :concurrently [db node]
  ["CONCURRENTLY"])

(defmethod compile-sql :condition [db {:keys [condition]}]
  (compile-sql db condition))

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

(defmethod compile-sql :continue-identity [db {:keys [op]}]
  ["CONTINUE IDENTITY"])

(defmethod compile-sql :copy [db {:keys [columns delimiter encoding from to table]}]
  (concat-sql
   "COPY "
   (compile-sql db table)
   (if-not (empty? columns)
     (concat-sql " (" (compile-sql-join db ", " columns) ")"))
   " FROM "
   (let [from (first from)]
     (cond
       (instance? File from)
       ["?" (.getAbsolutePath from)]
       (string? from)
       ["?" (.getAbsolutePath (File. from))]
       (= :stdin from)
       "STDIN"))
   (if encoding
     [" ENCODING ?" encoding])
   (if delimiter
     [" DELIMITER ?" delimiter])))

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
       (concat-sql " INHERITS (" (compile-sql-join db ", " inherits) ")")))))

(defmethod compile-sql :delete [db node]
  (let [{:keys [where table returning]} node]
    (compile-with
     db (:with node)
     (concat-sql
      "DELETE FROM " (compile-sql db table)
      (if-not (empty? where)
        (concat-sql " WHERE " (compile-sql db where)))
      (if-not (empty? returning)
        (concat-sql " RETURNING " (compile-sql-join db ", " returning))) ))))

(defmethod compile-sql :distinct [db {:keys [exprs on]}]
  (concat-sql
   "DISTINCT "
   (if-not (empty? on)
     (concat-sql "ON (" (compile-sql-join db ", " on) ") "))
   (compile-sql-join db ", " exprs)))

(defmethod compile-sql :drop-table [db {:keys [cascade if-exists restrict tables]}]
  (join-sql " " ["DROP TABLE"
                 (compile-sql db if-exists)
                 (compile-sql-join db ", " tables)
                 (compile-sql db cascade)
                 (compile-sql db restrict)]))

(defmethod compile-sql :except [db node]
  (compile-set-op db :except node))

(defmethod compile-sql :expr-list [db {:keys [as children]}]
  (concat-sql (compile-sql-join db " " children)
              (compile-alias db as)))

(defmethod compile-sql :attr [db node]
  (concat-sql "(" (compile-sql db (:arg node)) ")." (sql-quote db (:name node))
              (if-let [as (:as node)]
                (compile-alias db as))))

(defmethod compile-sql :fn [db node]
  (concat-sql (if-let [dir (:direction node)]
                (join-sql " " [(compile-fn db node) (upper-case (name dir))])
                (compile-fn db node))))

(defmethod compile-sql :from [db {:keys [clause joins]}]
  (concat-sql "FROM " (join-sql ", " (map #(compile-from db %1) clause))
              (if-not (empty? joins)
                (compile-sql-join db " " joins))))

(defmethod compile-sql :group-by [db {:keys [exprs]}]
  (concat-sql "GROUP BY" (compile-sql db exprs)))

(defmethod compile-sql :if-exists [db {:keys [op]}]
  ["IF EXISTS"])

(defn- compile-value [db columns value]
  (let [columns (map :name columns)
        values (map #(or (get value %) {:op :nil}) columns)
        values (map #(compile-sql db %) values)]
    (concat-sql "(" (join-sql ", " values ) ")")))

(defn- compile-values [db columns values]
  (let [values (map #(compile-value db columns %) values)]
    (concat-sql ["VALUES "] (join-sql ", " values))))

(defmethod compile-sql :insert [db node]
  (let [{:keys [table columns rows default-values values returning select where]} node
        columns (if (and (empty? columns)
                         (not (empty? values)))
                  (map (fn [k] {:op :column :name k})
                       (keys (first values)))
                  columns)]
    (compile-with
     db (:with node)
     (concat-sql
      "INSERT INTO " (compile-sql db table)
      (if-not (empty? columns)
        (concat-sql " (" (compile-sql-join db ", " columns) ")"))
      (if-not (empty? values)
        (concat-sql " " (compile-values db columns values)))
      (if select
        (concat-sql " " (compile-sql db select)))
      (if default-values
        " DEFAULT VALUES")
      (if-not (empty? returning)
        (concat-sql " RETURNING " (compile-sql-join db ", " returning)))
      (if-not (empty? where)
        (concat-sql " WHERE " (compile-sql db where)))))))

(defmethod compile-sql :intersect [db node]
  (compile-set-op db :intersect node))

(defmethod compile-sql :join [db {:keys [on using from how type outer]}]
  (concat-sql
   (case type
     :cross "CROSS "
     :inner "INNER "
     :left "LEFT "
     :right "RIGHT "
     :full "FULL "
     nil "")
   (if outer "OUTER ")
   "JOIN " (compile-from db from)
   (if on
     (concat-sql " ON " (compile-sql db on)))
   (if-not (empty? using)
     (concat-sql " USING (" (compile-sql-join db ", " using) ")"))))

(defmethod compile-sql :keyword [db {:keys [form]}]
  [(sql-quote db form)])

(defmethod compile-sql :limit [db {:keys [count]}]
  (concat-sql (when (number? count) (str "LIMIT " count))))

(defmethod compile-sql :like [db {:keys [excluding including table]}]
  (concat-sql
   "LIKE "
   (compile-sql db table)
   (if-not (empty? including)
     (str " INCLUDING " (join " " (map keyword-sql including))))
   (if-not (empty? excluding)
     (str " EXCLUDING " (join " " (map keyword-sql excluding))))))

(defmethod compile-sql :list [db {:keys [children]}]
  (concat-sql "(" (compile-sql-join db ", " children) ")"))

(defmethod compile-sql :nil [db _] ["NULL"])

(defmethod compile-sql :offset [db {:keys [start]}]
  (concat-sql "OFFSET " (if (number? start) (str start) "0")))

(defmethod compile-sql :order-by [db {:keys [exprs direction nulls using]}]
  (concat-sql "ORDER BY " (compile-sql db exprs)))

(defmethod compile-sql :table [db {:keys [as schema name]}]
  [(str (join "." (map #(sql-quote db %1) (remove nil? [schema name])))
        (compile-alias db as false))])

(defmethod compile-sql :drop-materialized-view [db node]
  (let [{:keys [cascade if-exists restrict view]} node]
    (concat-sql "DROP MATERIALIZED VIEW "
                (if if-exists
                  (concat-sql (compile-sql db if-exists) " "))
                (compile-sql db view)
                (if cascade
                  (concat-sql " " (compile-sql db cascade)))
                (if restrict
                  (concat-sql " " (compile-sql db restrict))))))

(defmethod compile-sql :refresh-materialized-view [db node]
  (let [{:keys [concurrently view with-data]} node]
    (concat-sql "REFRESH MATERIALIZED VIEW "
                (if concurrently
                  (concat-sql (compile-sql db concurrently) " "))
                (compile-sql db view)
                (if with-data
                  (concat-sql " " (compile-sql db with-data))))))

(defmethod compile-sql :restrict [db {:keys [op]}]
  ["RESTRICT"])

(defmethod compile-sql :restart-identity [db {:keys [op]}]
  ["RESTART IDENTITY"])

(defmethod compile-sql :select [db node]
  (let [{:keys [exprs distinct joins from where group-by limit offset order-by set]} node]
    (compile-with
     db (:with node)
     (concat-sql
      "SELECT " (join-sql ", " (map #(compile-expr db %1) exprs))
      (if distinct
        (compile-sql db distinct))
      (if-not (empty? from)
        (concat-sql " FROM " (join-sql ", " (map #(compile-from db %1) from))))
      (if-not (empty? joins)
        (concat-sql " " (compile-sql-join db " " joins)))
      (if-not (empty? where)
        (concat-sql " WHERE " (compile-sql db where)))
      (if-not (empty? group-by)
        (concat-sql " GROUP BY " (compile-sql-join db ", " group-by)))
      (when-let [window (:window node)]
        (concat-sql " " (compile-sql db window)))
      (if-not (empty? order-by)
        (concat-sql " ORDER BY " (compile-sql-join db ", " order-by)))
      (when-let [limit-sql (and limit (seq (compile-sql db limit)))]
        (concat-sql " " limit-sql))
      (if offset
        (concat-sql " " (compile-sql db offset)))
      (if-not (empty? set)
        (concat-sql " " (compile-sql-join db ", " set)))))))

(defmethod compile-sql :truncate [db {:keys [tables continue-identity restart-identity cascade restrict]}]
  (join-sql " " ["TRUNCATE TABLE"
                 (compile-sql-join db ", " tables)
                 (compile-sql db continue-identity)
                 (compile-sql db restart-identity)
                 (compile-sql db cascade)
                 (compile-sql db restrict)]))

(defmethod compile-sql :union [db node]
  (compile-set-op db :union node))

(defmethod compile-sql :update [db node]
  (let [{:keys [where from exprs table row returning]} node]
    (compile-with
     db (:with node)
     (concat-sql
      "UPDATE " (compile-sql db table)
      " SET "
      (join-sql
       ", " (if row
              (for [column (keys row)]
                ;; [(str (sql-quote db column) " = ?") (get row column)]
                (concat-sql
                 (str (sql-quote db column) " = ")
                 (compile-sql db (get row column))))
              (map unwrap-stmt (compile-exprs db exprs))))
      (if-not (empty? from)
        (concat-sql " FROM " (join-sql " " (map #(compile-from db %1) from))))
      (if-not (empty? where)
        (concat-sql " WHERE " (compile-sql db where)))
      (if-not (empty? returning)
        (concat-sql " RETURNING " (compile-sql-join db ", " returning)))))))

(defmethod compile-sql :window [db node]
  (->> (for [definition (:definitions node)]
         (concat-sql
          (sql-quote db (:as definition))
          " AS (" (compile-sql db definition) ")"))
       (join-sql ", ")
       (concat-sql "WINDOW " )))

(defmethod compile-sql :with-data [db node]
  (if (:data node)
    ["WITH DATA"]
    ["WITH NO DATA"]))

(defmethod compile-sql nil [db {:keys [op]}]
  [])

;; DEFINE SQL FN ARITY

(defmacro defarity
  "Define SQL functions in terms of `arity-fn`."
  [arity-fn & fns]
  `(do ~@(for [fn# (map keyword fns)]
           `(defmethod compile-fn ~fn# [db# ~'node]
              (~arity-fn db# ~'node)))))

(defarity compile-2-ary
  "=" "!=" "<>" "<" ">" "<=" ">=" "&&" "<@" "@>" "/" "^" "~" "~*" "like" "ilike")

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
  [db stmt]
  (assert db "No db given!")
  (apply vector (compile-sql db stmt)))
