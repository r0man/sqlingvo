(ns sqlingvo.util
  (:refer-clojure :exclude [replace])
  (:require [clojure.algo.monads :refer :all]
            [clojure.string :refer [blank? join replace]]
            [inflections.core :refer [hyphenize underscore]]))

(deftype Stmt [f]
  clojure.lang.IFn
  (invoke [this n]
    (f n)))

(def ^:dynamic *column-regex*
  #"(([^./]+)\.)?(([^./]+)\.)?([^./]+)(/(.+))?")

(def ^:dynamic *table-regex*
  #"(([^./]+)\.)?([^./]+)(/(.+))?")

(def sql-keyword-hyphenize
  (comp keyword hyphenize))

(defn sql-quote-backtick [x]
  (str "`" x "`"))

(defn sql-quote-double-quote [x]
  (str "\"" x "\""))

(def sql-name-underscore
  (comp underscore name))

(def default-identifiers
  (comp keyword hyphenize))

(def default-quotes
  [\" \"])

(defn as-identifier
  "Given a obj, convert it to a string using the current naming
  strategy."
  [db obj]
  (let [entities (or (:entities db) sql-name-underscore)]
    (cond
     (nil? obj)
     nil
     (= :* obj)
     "*"
     (keyword? obj)
     (entities obj)
     (string? obj)
     (entities obj)
     (symbol? obj)
     (entities obj)
     (map? obj)
     (->> [(:schema obj) (:table obj) (:name obj)]
          (remove nil?)
          (map entities)
          (join ".")))))

(defn as-quoted [db obj]
  (if obj
    (let [[start end] (:quotes db)]
      (str
       (or start)
       (as-identifier db obj)
       (or end start)))))

(defn as-keyword
  "Given a obj, convert it to a keyword using the current naming
  strategy."
  [obj]
  (cond
   (nil? obj)
   nil
   (keyword? obj)
   obj
   (symbol? obj)
   (keyword (hyphenize obj))
   (string? obj)
   (keyword (hyphenize obj))
   (map? obj)
   (->> [(:schema obj) (:table obj) (:name obj)]
        (remove nil?)
        (map name)
        (join ".")
        (keyword))))

(defn concat-in [m ks & args]
  (apply update-in m ks concat args))

(defn concat-val
  "Return a state-monad function that assumes the state to be a map
   and concats `v` onto the value stored under `k` in the state. The
   old value is returned."
  [k v]
  (domonad state-m
    [old-v (fetch-val k)
     old-s (set-val k (concat old-v v))]
    old-s))

(defn qualified-name
  "Returns the qualified name of `k`."
  [k] (replace (str k) #"^:" ""))

(defn parse-column
  "Parse `s` as a column identifier and return a map
  with :op, :schema, :name and :as keys."
  [s]
  (if (map? s)
    s (if-let [matches (re-matches *column-regex* (qualified-name s))]
        (let [[_ _ schema _ table name _ as] matches]
          {:op :column
           :schema (if (and schema table) (keyword schema))
           :table (keyword (or table schema))
           :name (keyword name)
           :as (keyword as)}))))

(defn parse-table
  "Parse `s` as a table identifier and return a map
  with :op, :schema, :name and :as keys."
  [s]
  (if (map? s)
    s (if-let [matches (re-matches *table-regex* (qualified-name s))]
        {:op :table
         :schema (keyword (nth matches 2))
         :name (keyword (nth matches 3))
         :as (keyword (nth matches 5))})))

(defmulti parse-expr class)

(defn- parse-fn-expr [expr]
  {:op :fn
   :name (keyword (name (first expr)))
   :args (map parse-expr (rest expr))})

(defmethod parse-expr nil [expr]
  {:op :nil})

(defmethod parse-expr clojure.lang.Cons [expr]
  (parse-fn-expr expr))

(defmethod parse-expr clojure.lang.ISeq [expr]
  (cond
   (or (keyword? (first expr))
       (symbol? (first expr)))
   (parse-fn-expr expr)
   (list? (first expr))
   {:op :expr-list :children (map parse-expr expr) :as (:as expr)}
   :else {:op :list :children (map parse-expr expr) :as (:as expr)}))

(defmethod parse-expr clojure.lang.IPersistentMap [expr]
  expr)

(defmethod parse-expr clojure.lang.IPersistentVector [expr]
  {:op :array :children (map parse-expr expr)})

(defmethod parse-expr clojure.lang.Keyword [expr]
  (parse-column expr))

(defmethod parse-expr clojure.core$_STAR_ [expr]
  {:op :constant :form '*})

(defmethod parse-expr :default [expr]
  (if (or (fn? expr)
          (instance? Stmt expr))
    (first (expr {}))
    {:op :constant :form expr}))

(defn parse-exprs [exprs]
  (map parse-expr (remove nil? exprs)))

(defn parse-condition [condition]
  {:op :condition :condition (parse-expr condition)})

(defn parse-from [forms]
  (cond
   (keyword? forms)
   (parse-table forms)
   (and (map? forms) (= :fn (:op forms)))
   forms
   (and (map? forms) (= :select (:op forms)))
   forms
   (and (map? forms) (= :table (:op forms)))
   forms
   (and (map? forms) (:as forms))
   {:op :table
    :as (:as forms)
    :schema (:table forms)
    :name (:name forms)}
   (list? forms)
   (parse-expr forms)
   :else (throw (IllegalArgumentException. (str "Can't parse FROM form: " forms)))))
