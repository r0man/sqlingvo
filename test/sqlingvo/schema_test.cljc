(ns sqlingvo.schema-test
  #?@
   (:clj
    [(:require
      [clojure.test :refer [deftest]]
      [sqlingvo.core :as sql]
      [sqlingvo.test :refer [db sql=]])]
    :cljs
    [(:require
      [clojure.test :refer [deftest]]
      [sqlingvo.core :as sql]
      [sqlingvo.test :refer [db] :refer-macros [sql=]])]))

(deftest test-create-schema
  (sql= (sql/create-schema db :my-schema)
        ["CREATE SCHEMA \"my-schema\""]))

(deftest test-create-schema-iof-no
  (sql= (sql/create-schema db :my-schema
          (sql/if-not-exists true))
        ["CREATE SCHEMA IF NOT EXISTS \"my-schema\""]))

(deftest test-drop-schema
  (sql= (sql/drop-schema db [:mood])
        ["DROP SCHEMA \"mood\""]))

(deftest test-drop-schemas
  (sql= (sql/drop-schema db [:schema-1 :schema-2])
        ["DROP SCHEMA \"schema-1\", \"schema-2\""]))

(deftest test-drop-schema-if-exists
  (sql= (sql/drop-schema db [:my-schema]
          (sql/if-exists true))
        ["DROP SCHEMA IF EXISTS \"my-schema\""]))

(deftest test-drop-schema-cascade
  (sql= (sql/drop-schema db [:my-schema]
          (sql/cascade true))
        ["DROP SCHEMA \"my-schema\" CASCADE"]))

(deftest test-drop-schema-restrict
  (sql= (sql/drop-schema db [:my-schema]
          (sql/restrict true))
        ["DROP SCHEMA \"my-schema\" RESTRICT"]))
