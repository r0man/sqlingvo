(ns sqlingvo.types-test
  (:require #?(:clj [sqlingvo.test :refer [db sql=]]
               :cljs [sqlingvo.test :refer [db] :refer-macros [sql=]])
            [clojure.test :refer [deftest is]]
            [sqlingvo.core :as sql]))

(deftest test-create-enum-type
  (sql= (sql/create-type db :mood
          (sql/enum ["sad" "ok" "happy"]))
        ["CREATE TYPE \"mood\" AS ENUM ('sad', 'ok', 'happy')"]))

(deftest test-create-enum-type-schema
  (sql= (sql/create-type db :my-schema.mood
          (sql/enum ["sad" "ok" "happy"]))
        ["CREATE TYPE \"my-schema\".\"mood\" AS ENUM ('sad', 'ok', 'happy')"]))

(deftest test-drop-type
  (sql= (sql/drop-type db [:mood])
        ["DROP TYPE \"mood\""]))

(deftest test-drop-types
  (sql= (sql/drop-type db [:type-1 :type-2])
        ["DROP TYPE \"type-1\", \"type-2\""]))

(deftest test-drop-type-if-exists
  (sql= (sql/drop-type db [:mood]
          (sql/if-exists true))
        ["DROP TYPE IF EXISTS \"mood\""]))

(deftest test-drop-type-cascade
  (sql= (sql/drop-type db [:mood]
          (sql/cascade true))
        ["DROP TYPE \"mood\" CASCADE"]))

(deftest test-drop-type-restrict
  (sql= (sql/drop-type db [:mood]
          (sql/restrict true))
        ["DROP TYPE \"mood\" RESTRICT"]))

(deftest test-create-enum-table
  (sql= (sql/create-table db :person
          (sql/column :name :text)
          (sql/column :mood :mood))
        ["CREATE TABLE \"person\" (\"name\" TEXT, \"mood\" mood)"]))

(deftest test-create-enum-table-schema
  (sql= (sql/create-table db :person
          (sql/column :name :text)
          (sql/column :mood :my-schema.mood))
        ["CREATE TABLE \"person\" (\"name\" TEXT, \"mood\" \"my-schema\".\"mood\")"]))
