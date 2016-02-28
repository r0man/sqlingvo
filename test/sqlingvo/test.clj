(ns sqlingvo.test
  (:require [clojure.test :refer :all]
            [sqlingvo.core :refer [sql]]))

(defmacro sql=
  "Compile `statement` into SQL and compare it to `expected`."
  [statement expected]
  `(is (= (sql ~statement) ~expected)))
