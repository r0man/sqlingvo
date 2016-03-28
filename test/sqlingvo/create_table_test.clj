(ns sqlingvo.create-table-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.test :refer :all]
            [sqlingvo.core :refer :all]
            [sqlingvo.expr :refer :all]
            [sqlingvo.test :refer [db sql= with-stmt]]))

(deftest test-create-table-tmp-if-not-exists-inherits
  (with-stmt
    ["CREATE TEMPORARY TABLE IF NOT EXISTS \"import\" () INHERITS (\"quotes\")"]
    (create-table db :import
      (temporary true)
      (if-not-exists true)
      (inherits :quotes))
    (is (= :create-table (:op stmt)))
    (is (= {:op :temporary} (:temporary stmt)))
    (is (= {:op :if-not-exists} (:if-not-exists stmt)))
    (is (= [(parse-table :quotes)] (:inherits stmt)))))

(deftest test-create-table-tmp-if-not-exists-false
  (sql= (create-table db :import
          (temporary true)
          (if-not-exists false)
          (inherits :quotes))
        ["CREATE TEMPORARY TABLE \"import\" () INHERITS (\"quotes\")"]))

(deftest test-create-table-like-including-defaults
  (with-stmt
    ["CREATE TABLE \"tmp-films\" (LIKE \"films\" INCLUDING DEFAULTS)"]
    (create-table db :tmp-films
      (like :films :including [:defaults]))
    (is (= :create-table (:op stmt)))
    (let [like (:like stmt)]
      (is (= :like (:op like)))
      (is (= (parse-table :films) (:table like)))
      (is (= [:defaults] (:including like))))))

(deftest test-create-table-like-excluding-indexes
  (with-stmt
    ["CREATE TABLE \"tmp-films\" (LIKE \"films\" EXCLUDING INDEXES)"]
    (create-table db :tmp-films
      (like :films :excluding [:indexes]))
    (is (= :create-table (:op stmt)))
    (let [like (:like stmt)]
      (is (= :like (:op like)))
      (is (= (parse-table :films) (:table like)))
      (is (= [:indexes] (:excluding like))))))

(deftest test-create-table-films
  (sql= (create-table db :films
          (column :code :char :length 5 :primary-key? true)
          (column :title :varchar :length 40 :not-null? true)
          (column :did :integer :not-null? true)
          (column :date-prod :date)
          (column :kind :varchar :length 10)
          (column :len :interval)
          (column :created-at :timestamp-with-time-zone :not-null? true :default '(now))
          (column :updated-at :timestamp-with-time-zone :not-null? true :default '(now)))
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
  (sql= (create-table db :ratings
          (column :id :serial)
          (column :user-id :integer :not-null? true :references :users/id)
          (column :spot-id :integer :not-null? true :references :spots/id)
          (column :rating :integer :not-null? true)
          (column :created-at :timestamp-with-time-zone :not-null? true :default '(now))
          (column :updated-at :timestamp-with-time-zone :not-null? true :default '(now))
          (primary-key :user-id :spot-id :created-at))
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
  (sql= (create-table db :ratings
          (column :x :text :array? true))
        ["CREATE TABLE \"ratings\" (\"x\" TEXT[])"]))
