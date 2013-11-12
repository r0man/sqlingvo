(ns sqlingvo.native
  (:require [sqlingvo.util :refer :all]))

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

(defn from
  "Returns a fn that adds a FROM clause to an SQL statement."
  [& from]
  (let [from (map parse-from from)]
    (fn [stmt]
      [from (assoc stmt :from from)])))

(defn select
  "Returns a fn that builds a SELECT statement."
  [exprs & body]
  (let [exprs (if (sequential? exprs)
                (parse-exprs exprs))]
    (fn [_]
      (let [[_ stmt] ((m-seq (remove nil? body)) {})]
        [exprs (assoc stmt
                 :op :select
                 :exprs exprs)]))))

(defn where
  "Returns a fn that adds a WHERE clause to an SQL statement."
  [condition & [combine]]
  (let [condition (parse-condition condition)]
    (fn [stmt]
      (cond
       (or (nil? combine)
           (nil? (:condition (:where stmt))))
       [nil (assoc stmt :where condition)]
       :else
       [nil (assoc-in stmt [:where :condition]
                      {:op :condition
                       :condition {:op :fn
                                   :name combine
                                   :args [(:condition (:where stmt))
                                          (:condition condition)]}})]))))
