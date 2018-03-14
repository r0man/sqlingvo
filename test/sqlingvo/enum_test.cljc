(ns sqlingvo.enum-test
  (:require #?(:clj [sqlingvo.test :refer [db sql=]]
               :cljs [sqlingvo.test :refer [db] :refer-macros [sql=]])
            [clojure.test :refer [deftest is]]
            [sqlingvo.core :as sql]))

(deftest test-create-enum-type
  (sql= (sql/create-type :postgresql :mood
          (sql/enum ["sad" "ok" "happy"]))
        ["CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')"]))

(deftest test-create-enum-table
  (sql= (sql/create-table db :person
          (sql/column :name :text)
          (sql/column :mood :mood))
        ["CREATE TABLE \"person\" (\"name\" TEXT, \"mood\" MOOD)"]))
