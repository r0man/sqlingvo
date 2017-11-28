(ns sqlingvo.expr
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [sqlingvo.spec :as spec]))

(def ^:dynamic *column-regex*
  "The regular expression used to parse a column identifier."
  #"(([^./]+)\.)?(([^./]+)\.)?([^./]+)")

(def ^:dynamic *table-regex*
  "The regular expression used to parse a table identifier."
  #"(([^./]+)\.)?([^./]+)")

(defprotocol IExpr
  (-parse-expr [x] "Parse `x` and return the AST of a SQL expression."))

(defn attribute?
  "Returns true if `form` is an attribute for a composite type."
  [form]
  (and (symbol? form)
       (str/starts-with? (str form) ".-")))

(defn parse-expr
  "Parse the SQL expression `x` into an AST."
  [x]
  (-parse-expr x))

(defn parse-exprs
  "Parse the SQL expressions `xs` into an AST."
  [xs]
  (mapv parse-expr (remove nil? xs)))

(deftype Stmt [f]
  #?(:clj clojure.lang.IDeref :cljs cljs.core/IDeref)
  (#?(:clj deref :cljs -deref) [stmt]
    (let [ast (second (f nil))]
      ((:eval-fn (:db ast)) stmt)))
  #?(:clj clojure.lang.IFn :cljs cljs.core/IFn)
  (#?(:clj invoke :cljs -invoke) [this n]
    (f n)))

(defn ast
  "Returns the abstract syntax tree of `stmt`."
  [stmt]
  (cond
    (map? stmt)
    stmt
    (instance? Stmt stmt)
    (second (#?(:clj (.f stmt) :cljs (.-f stmt)) nil))
    :else (second (stmt nil))))

(defn stmt [x]
  (Stmt. x))

(defn unintern-name
  "Returns `x` without any namespace."
  [x]
  (cond
    (string? x)
    x
    (or (keyword? x) (symbol? x))
    (name x)))

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
  (cond
    (s/valid? :sqlingvo/alias s) s
    (s/valid? :sqlingvo/column s) s
    (or (string? s) (keyword? s))
    (if-let [matches (re-matches *column-regex* (name s))]
      (let [[_ _ schema _ table name _] matches]
        (cond-> (make-node
                 :op :column
                 :children [:schema :table :name :as]
                 :form s
                 :schema (if (and schema table) (keyword schema))
                 :table (keyword (or table schema))
                 :name (keyword name)
                 :val s)
          (and (keyword? s) (namespace s))
          (assoc :ns (namespace s)))))))

(s/fdef parse-column
  :args (s/cat :s :sqlingvo.column/identifier)
  :ret (s/nilable :sqlingvo/column))

(defn parse-table
  "Parse `s` as a table identifier and return a map
  with :op, :schema, :name and :as keys."
  [s]
  (cond
    (s/valid? :sqlingvo/alias s) s
    (map? s) s
    (or (string? s) (keyword? s))
    (if-let [matches (re-matches *table-regex* (name s))]
      (cond-> (make-node
               :op :table
               :children [:schema :name :as]
               :form s
               :schema (keyword (nth matches 2))
               :name (keyword (nth matches 3))
               :val s)
        (and (keyword? s) (namespace s))
        (assoc :ns (namespace s))))))

(s/fdef parse-table
  :args (s/cat :s :sqlingvo.table/identifier)
  :ret (s/nilable :sqlingvo/table))

(defn- parse-attr-expr [expr]
  (make-node
   :op :attr
   :children [:arg]
   :name (keyword (str/replace (name (first expr)) ".-" ""))
   :arg (parse-expr (first (rest expr)))))

(defn parse-map-expr [m]
  (into {} (for [[k v] m] [k (parse-expr v) ])))

(defn parse-condition [condition]
  {:op :condition
   :condition (parse-expr condition)})

(defn parse-from [forms]
  (cond
    (or (string? forms)
        (keyword? forms))
    (parse-table forms)
    (and (map? forms)
         (= :alias (:op forms))
         (= :column (-> forms :expr :op)))
    (assoc forms :expr (parse-table (-> forms :expr :form)))
    (and (map? forms) (= :alias (:op forms)))
    forms
    (and (map? forms) (= :list (:op forms)))
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

(defn- parse-constant
  "Parse the `constant` of `type`."
  [type constant]
  {:form constant
   :op :constant
   :type type
   :val constant})

(s/fdef parse-constant
  :args (s/cat :type keyword? :constant any?)
  :ret map?)

#?(:clj
   (extend-protocol IExpr

     clojure.lang.IPersistentMap
     (-parse-expr [x]
       x)

     java.lang.Double
     (-parse-expr [x]
       (parse-constant :number x))

     java.lang.Integer
     (-parse-expr [x]
       (parse-constant :number x))

     java.lang.Long
     (-parse-expr [x]
       (parse-constant :number x))

     java.util.Date
     (-parse-expr [x]
       (parse-constant :date x))))

#?(:cljs
   (extend-protocol IExpr
     number
     (-parse-expr [x]
       (parse-constant :number x))

     cljs.core/PersistentArrayMap
     (-parse-expr [x]
       x)

     cljs.core/PersistentHashMap
     (-parse-expr [x]
       x)

     cljs.core/EmptyList
     (-parse-expr [x]
       {:op :list})

     js/Date
     (-parse-expr [x]
       (parse-constant :date x))))

(extend-protocol IExpr

  nil
  (-parse-expr [x]
    {:form x
     :op :nil
     :type :nil
     :val x})

  Stmt
  (-parse-expr [stmt]
    (first (stmt {})))

  #?(:clj Boolean :cljs boolean)
  (-parse-expr [x]
    (parse-constant :boolean x))

  #?(:clj clojure.lang.Cons :cljs cljs.core/Cons)
  (-parse-expr [x]
    (-parse-expr (apply list x)))

  #?(:clj clojure.lang.Keyword :cljs cljs.core/Keyword)
  (-parse-expr [x]
    (parse-column x))

  #?(:clj clojure.lang.Symbol :cljs cljs.core/Symbol)
  (-parse-expr [x]
    {:form x
     :op :constant
     :type :symbol
     :val (symbol (name x))})

  #?(:clj clojure.lang.IPersistentMap :cljs cljs.core/PersistentArrayMap)
  (-parse-expr [x]
    x)

  #?(:clj clojure.lang.PersistentVector :cljs cljs.core/PersistentVector)
  (-parse-expr [x]
    {:op :array
     :children (mapv parse-expr x)})

  #?(:clj java.util.List :cljs cljs.core/List)
  (-parse-expr [expr]
    (cond
      (attribute? (first expr))
      (parse-attr-expr expr)
      (or (list? (first expr))
          (instance? #?(:clj clojure.lang.Cons :cljs cljs.core/Cons) (first expr)))
      {:op :expr-list
       :children (mapv parse-expr expr)
       :as (:as expr)}
      :else
      {:op :list
       :children (mapv parse-expr expr)}))

  #?(:clj String :cljs string)
  (-parse-expr [x]
    (parse-constant :string x))

  #?(:clj Object :cljs object)
  (-parse-expr [x]
    (parse-constant :object x)))
