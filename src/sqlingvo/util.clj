(ns sqlingvo.util
  (:refer-clojure :exclude [replace])
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [blank? join replace]]
            [inflections.core :refer [hyphenize]]))

(def ^:dynamic *column-regex*
  #"(([^./]+)\.)?(([^./]+)\.)?([^./]+)(/(.+))?")

(def ^:dynamic *table-regex*
  #"(([^./]+)\.)?([^./]+)(/(.+))?")

(defn as-identifier
  "Given a obj, convert it to a string using the current naming
  strategy."
  [obj]
  (cond
   (nil? obj)
   nil
   (keyword? obj)
   (jdbc/as-identifier obj)
   (string? obj)
   obj
   (symbol? obj)
   (str obj)
   (map? obj)
   (->> [(:schema obj) (:table obj) (:name obj)]
        (map jdbc/as-identifier)
        (remove blank?)
        (join "."))))

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
        (map jdbc/as-identifier)
        (remove blank?)
        (join ".")
        (keyword))))

(defn concat-in [m ks & args]
  (apply update-in m ks concat args))

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

(defn parse-fn-expr [expr]
  {:op :fn
   :name (keyword (name (first expr)))
   :args (map parse-expr (rest expr))})

(defmethod parse-expr nil [expr]
  {:op :nil})

(defmethod parse-expr clojure.lang.Cons [expr]
  (parse-fn-expr expr))

(defmethod parse-expr clojure.lang.LazySeq [expr]
  (parse-fn-expr expr))

(defmethod parse-expr clojure.lang.PersistentList [expr]
  (cond
   (or (keyword? (first expr))
       (symbol? (first expr)))
   (parse-fn-expr expr)
   (list? (first expr))
   {:op :expr-list :children (map parse-expr expr) :as (:as expr)}
   :else {:op :list :children (map parse-expr expr) :as (:as expr)}))

(defmethod parse-expr clojure.lang.IPersistentMap [expr]
  expr)

(defmethod parse-expr clojure.lang.Keyword [expr]
  (parse-column expr))

(defmethod parse-expr clojure.core$_STAR_ [expr]
  {:op :constant :form '*})

(defmethod parse-expr :default [expr]
  (if (fn? expr)
    (first (expr {}))
    {:op :constant :form expr}))

(defn parse-condition [condition]
  {:op :condition :condition (parse-expr condition)})

(defn parse-from [forms]
  (cond
   (keyword? forms)
   (parse-table forms)
   (and (map? forms) (= :select (:op forms)))
   forms
   (and (map? forms) (= :table (:op forms)))
   forms
   (and (map? forms) (:as forms))
   {:op :table
    :as (:as forms)
    :schema (:table forms)
    :name (:name forms)}
   :else (throw (IllegalArgumentException. (str "Can't parse FROM form: " forms)))))
