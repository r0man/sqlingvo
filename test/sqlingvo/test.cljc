(ns sqlingvo.test
  (:require #?(:clj [clojure.test :refer :all]
               :cljs [cljs.test :refer-macros [is]])
            [sqlingvo.core :refer [sql]]
            [sqlingvo.db :as db]))

(def db (db/postgresql))

(defn cljs-env?
  "Take the &env from a macro, and tell whether we are expanding into cljs."
  [env]
  (boolean (:ns env)))

(defmacro if-cljs
  "Return then if we are generating cljs code and else for Clojure code.
   https://groups.google.com/d/msg/clojurescript/iBY5HaQda4A/w1lAQi9_AwsJ"
  [then else]
  (if (cljs-env? &env) then else))

(defmacro sql=
  "Compile `statement` into SQL and compare it to `expected`."
  [statement expected]
  `(if-cljs
    (cljs.test/is (= (sqlingvo.core/sql ~statement) ~expected))
    (is (= (sql ~statement) ~expected))))
