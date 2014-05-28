(ns sqlingvo.util
  (:require [clojure.string :refer [join replace split]])
  (:refer-clojure :exclude [replace]))

(deftype Stmt [f]
  clojure.lang.IFn
  (invoke [this n]
    (f n)))

(def ^:dynamic *column-regex*
  #"(([^./]+)\.)?(([^./]+)\.)?([^./]+)(/(.+))?")

(def ^:dynamic *table-regex*
  #"(([^./]+)\.)?([^./]+)(/(.+))?")

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

(defn set-val [k v]
  (fn [stmt]
    [v (assoc stmt k v)]))

(defn assoc-op [op & {:as opts}]
  (set-val op (assoc opts :op op)))

(defn concat-in [ks coll]
  (fn [stmt]
    [nil (if (empty? coll)
           stmt (update-in stmt ks #(concat %1 coll)))]))

(defn dissoc-op [k]
  (fn [stmt]
    [nil (dissoc stmt k)]))

(defn sequential
  "Returns `x` as a sequential data structure."
  [x]
  (if (sequential? x)
    x [x]))

(defn conditional-clause [clause condition]
  (if condition
    (assoc-op clause)
    (dissoc-op clause)))

(defn- split-sql-name [x]
  (if x (split (name x) #"\.")))

(defn- map-sql-name [f x]
  (->> (split-sql-name x)
       (map f)
       (join ".")))

(defn sql-name-underscore [x]
  (map-sql-name #(replace %1 "-" "_") x))

(defn sql-keyword-hyphenate [x]
  (keyword (map-sql-name #(replace (name %1) "_" "-") x)))

(defn- sql-quote-char [x before after]
  (if-not (= "*" x)
    (str before x after) "*" ))

(defn sql-quote-backtick [x]
  (map-sql-name #(sql-quote-char %1 "`" "`") x))

(defn sql-quote-double-quote [x]
  (map-sql-name #(sql-quote-char %1 "\"" "\"") x))

(defn qualified-name
  "Returns the qualified name of `k`."
  [k] (replace (str k) #"^:" ""))

(defn make-node [& {:as node}]
  (assert (:op node) (str "Missing :op in make-node: " (pr-str node)))
  (if-not (empty? (:children node))
    (reduce (fn [node child]
              (if (nil? (get node child))
                (dissoc node child)
                (update-in node [:children] conj child)))
            (assoc node :children [])
            (:children node))
    node))

(defn parse-column
  "Parse `s` as a column identifier and return a map
  with :op, :schema, :name and :as keys."
  [s]
  (if (map? s)
    s (if-let [matches (re-matches *column-regex* (qualified-name s))]
        (let [[_ _ schema _ table name _ as] matches]
          (make-node
           :op :column
           :children [:schema :table :name :as]
           :schema (if (and schema table) (keyword schema))
           :table (keyword (or table schema))
           :name (keyword name)
           :as (keyword as))))))

(defn parse-table
  "Parse `s` as a table identifier and return a map
  with :op, :schema, :name and :as keys."
  [s]
  (if (map? s)
    s (if-let [matches (re-matches *table-regex* (qualified-name s))]
        (make-node
         :op :table
         :children [:schema :name :as]
         :schema (keyword (nth matches 2))
         :name (keyword (nth matches 3))
         :as (keyword (nth matches 5))))))

(defn attribute?
  "Returns true if `form` is an attribute for a composite type."
  [form]
  (and (symbol? form)
       (.startsWith (str form) ".-")))

(defmulti parse-expr class)

(defn- parse-fn-expr [expr]
  (make-node
   :op :fn
   :children [:args]
   :name (keyword (name (first expr)))
   :args (map parse-expr (rest expr))))

(defn- parse-attr-expr [expr]
  (make-node
   :op :attr
   :children [:arg]
   :name (keyword (replace (name (first expr)) ".-" ""))
   :arg (parse-expr (first (rest expr)))))

(defmethod parse-expr nil [expr]
  {:op :nil})

(defmethod parse-expr clojure.lang.Cons [expr]
  (parse-expr (apply list expr)))

(defmethod parse-expr clojure.lang.ISeq [expr]
  (cond
   (attribute? (first expr))
   (parse-attr-expr expr)
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

(defn type-keyword
  "Returns the type of `x` as a keyword."
  [x]
  (cond
   (number? x) :number
   (string? x) :string
   :else :unknown))

(defmethod parse-expr :default [expr]
  (if (or (fn? expr)
          (instance? Stmt expr))
    (first (expr {}))
    (make-node
     :op :constant
     :form expr
     :literal? true
     :type (type-keyword expr)
     :val expr)))

(defn parse-exprs [exprs]
  (map parse-expr (remove nil? exprs)))

(defn parse-condition [condition]
  {:op :condition :condition (parse-expr condition)})

(defn parse-from [forms]
  (cond
   (or (string? forms)
       (keyword? forms))
   (parse-table forms)
   (and (map? forms) (= :fn (:op forms)))
   forms
   (and (map? forms) (= :select (:op forms)))
   forms
   (and (map? forms) (= :table (:op forms)))
   forms
   (and (map? forms) (:as forms))
   (make-node
    :op :table
    :children [:schema :name :as]
    :as (:as forms)
    :schema (:table forms)
    :name (:name forms))
   (list? forms)
   (parse-expr forms)
   :else (throw (ex-info "Can't parse FROM form." {:forms forms}))))

(comment
  (parse-expr 1)
  (require '[clojure.tools.analyzer.jvm :as analyzer])
  (clojure.pprint/pprint (analyzer/analyze '(= 1 2) (analyzer/empty-env)))
  (clojure.pprint/pprint (parse-expr '(= 1 2)))

  )
