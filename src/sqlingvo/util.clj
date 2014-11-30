(ns sqlingvo.util
  (:require [clojure.string :refer [join replace split]])
  (:refer-clojure :exclude [replace]))

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
