(ns sqlingvo.expr
  (:refer-clojure :exclude [replace])
  (:require [clojure.string :refer [replace]]
            [sqlingvo.compiler :refer [compile-stmt]]))

(def ^:dynamic *column-regex*
  #"(([^./]+)\.)?(([^./]+)\.)?([^./]+)(/(.+))?")

(def ^:dynamic *table-regex*
  #"(([^./]+)\.)?([^./]+)(/(.+))?")

(defn stmt-ast
  "Return the abstract syntax tree of `stmt`."
  [stmt]
  (second ((.f stmt) nil)))

(defn eval-stmt
  "Eval the SQL `stmt`. Redefine this fn to do interesting things."
  [stmt]
  (let [tree (stmt-ast stmt)]
    (compile-stmt (:db tree) tree)))

(deftype Stmt [f]
  clojure.lang.IDeref
  (deref [stmt]
    (eval-stmt stmt))
  clojure.lang.IFn
  (invoke [this n]
    (f n)))

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
  (parse-column :*))

(defn type-keyword
  "Returns the type of `x` as a keyword."
  [x]
  (cond
    (number? x) :number
    (string? x) :string
    (symbol? x) :symbol
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

(defn parse-map-expr [m]
  (into {} (for [[k v] m] [k (parse-expr v) ])))

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
