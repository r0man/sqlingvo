(ns sqlingvo.driver.core
  (:require [sqlingvo.core :refer [ast]]))

(defmulti close-db
  "Close the connection to `db`."
  (fn [db] (:backend db)))

(defmulti eval-db*
  "Eval the `ast` against a database."
  (fn [ast] (-> ast :db :backend)))

(defn eval-db
  "Eval the `stmt` against a database."
  [stmt]
  (eval-db* (ast stmt)))

(defmulti open-db
  "Open a connection to `db`."
  (fn [db] (:backend db)))
