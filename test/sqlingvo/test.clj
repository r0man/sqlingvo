(ns sqlingvo.test
  (:require [clojure.test :refer :all]
            [sqlingvo.core :refer [sql]]
            [sqlingvo.db :as db]))

(def db (db/postgresql))

(defmacro sql=
  "Compile `statement` into SQL and compare it to `expected`."
  [statement expected]
  `(is (= (sql ~statement) ~expected)))

(defmacro with-stmt [sql forms & body]
  `(let [[result# ~'stmt] (~forms {})]
     (is (= ~sql (sqlingvo.core/sql ~'stmt)))
     ~@body))
