(ns sqlingvo.create-table-test
  (:require #?(:clj [sqlingvo.test :refer [db sql=]]
               :cljs [sqlingvo.test :refer [db] :refer-macros [sql=]])
            [clojure.test :refer [are deftest is]]
            [sqlingvo.core :as sql]))

(deftest test-create-table-keyword-db
  (sql= (sql/create-table :postgresql :measurement-y2006m02
          (sql/inherits :measurement))
        ["CREATE TABLE \"measurement-y2006m02\" () INHERITS (\"measurement\")"]))

(deftest test-create-table-inherits
  (sql= (sql/create-table db :measurement-y2006m02
          (sql/inherits :measurement))
        ["CREATE TABLE \"measurement-y2006m02\" () INHERITS (\"measurement\")"]))

(deftest test-create-table-inherits-check
  (sql= (sql/create-table db :measurement-y2006m02
          (sql/check '(and (>= :logdate (cast "2006-02-01" :date))
                           (< :logdate (cast "2006-03-01" :date))))
          (sql/inherits :measurement))
        [(str "CREATE TABLE \"measurement-y2006m02\" ("
              "CHECK ((\"logdate\" >= CAST(? AS date)) and "
              "(\"logdate\" < CAST(? AS date)))) "
              "INHERITS (\"measurement\")")
         "2006-02-01" "2006-03-01"]))

(deftest test-create-table-inherits-check-multiple
  (sql= (sql/create-table db :measurement-y2006m02
          (sql/check '(>= :logdate (cast "2006-02-01" :date)))
          (sql/check '(< :logdate (cast "2006-03-01" :date)))
          (sql/inherits :measurement))
        [(str "CREATE TABLE \"measurement-y2006m02\" ("
              "CHECK (\"logdate\" >= CAST(? AS date)), "
              "CHECK (\"logdate\" < CAST(? AS date))) "
              "INHERITS (\"measurement\")")
         "2006-02-01" "2006-03-01"]))

(deftest test-create-table-tmp-if-not-exists-inherits
  (sql= (sql/create-table db :import
          (sql/temporary true)
          (sql/if-not-exists true)
          (sql/inherits :quotes))
        [(str "CREATE TEMPORARY TABLE IF NOT EXISTS "
              "\"import\" () INHERITS (\"quotes\")")]))

(deftest test-create-table-tmp-if-not-exists-false
  (sql= (sql/create-table db :import
          (sql/temporary true)
          (sql/if-not-exists false)
          (sql/inherits :quotes))
        ["CREATE TEMPORARY TABLE \"import\" () INHERITS (\"quotes\")"]))

(deftest test-create-table-like-including-defaults
  (sql= (sql/create-table db :tmp-films
          (sql/like :films :including [:defaults]))
        ["CREATE TABLE \"tmp-films\" (LIKE \"films\" INCLUDING DEFAULTS)"]))

(deftest test-create-table-like-excluding-indexes
  (sql= (sql/create-table db :tmp-films
          (sql/like :films :excluding [:indexes]))
        ["CREATE TABLE \"tmp-films\" (LIKE \"films\" EXCLUDING INDEXES)"]))

(deftest test-create-table-films
  (sql= (sql/create-table db :films
          (sql/column :code :char :size 5 :primary-key? true)
          (sql/column :title :varchar :size 40 :not-null? true)
          (sql/column :did :integer :not-null? true)
          (sql/column :date-prod :date)
          (sql/column :kind :varchar :size 10)
          (sql/column :len :interval)
          (sql/column :created-at :timestamp-with-time-zone :not-null? true :default '(now))
          (sql/column :updated-at :timestamp-with-time-zone :not-null? true :default '(now)))
        [(str "CREATE TABLE \"films\" ("
              "\"code\" CHAR(5) PRIMARY KEY, "
              "\"title\" VARCHAR(40) NOT NULL, "
              "\"did\" INTEGER NOT NULL, "
              "\"date-prod\" DATE, "
              "\"kind\" VARCHAR(10), "
              "\"len\" INTERVAL, "
              "\"created-at\" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "
              "\"updated-at\" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now())")]))

(deftest test-create-table-compound-primary-key
  (sql= (sql/create-table db :ratings
          (sql/column :id :serial)
          (sql/column :user-id :integer :not-null? true :references :users/id)
          (sql/column :spot-id :integer :not-null? true :references :spots/id)
          (sql/column :rating :integer :not-null? true)
          (sql/column :created-at :timestamp-with-time-zone :not-null? true :default '(now))
          (sql/column :updated-at :timestamp-with-time-zone :not-null? true :default '(now))
          (sql/primary-key :user-id :spot-id :created-at))
        [(str "CREATE TABLE \"ratings\" ("
              "\"id\" SERIAL, "
              "\"user-id\" INTEGER NOT NULL, "
              "\"spot-id\" INTEGER NOT NULL, "
              "\"rating\" INTEGER NOT NULL, "
              "\"created-at\" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "
              "\"updated-at\" TIMESTAMP WITH "
              "TIME ZONE NOT NULL DEFAULT now(), "
              "PRIMARY KEY(\"user-id\", \"spot-id\", \"created-at\"))")]))

(deftest test-create-table-array-column
  (sql= (sql/create-table db :ratings
          (sql/column :x :text :array? true))
        ["CREATE TABLE \"ratings\" (\"x\" TEXT[])"]))
