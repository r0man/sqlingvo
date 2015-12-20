(ns sqlingvo.db
  (:require [sqlingvo.compiler :as compiler]
            [sqlingvo.util :refer :all]))

(defrecord Database [subprotocol])

(defmulti db
  "Return the `Database` record for :adapter or :subprotocol in
  `db-spec`."
  (fn [db-spec] (keyword (:subprotocol db-spec))))

(defmethod db :default [{:keys [subprotocol] :as db-spec}]
  (throw (ex-info (format "Unknown database subprotocol: %s"
                          (some-> db-spec :subprotocol name))
                  db-spec)))

(defmacro defdb
  "Define a database vendor."
  [db-name doc & {:as opts}]
  `(do
     (defmethod db ~(keyword db-name) [~'db-spec]
       (map->Database
        (merge ~'db-spec
               {:classname ~(:classname opts)
                :doc ~doc
                :eval-fn compiler/eval-str
                :sql-keyword ~(:keyword opts)
                :sql-name ~(:name opts)
                :sql-quote ~(:quote opts)
                :subprotocol ~(name db-name)})))
     (defn ~(symbol db-name) [& [~'db-spec]]
       (db (assoc ~'db-spec :subprotocol ~(name db-name))))))

(defdb mysql
  "The world's most popular open source database."
  :classname "com.mysql.jdbc.Driver"
  :quote sql-quote-backtick)

(defdb postgresql
  "The world's most advanced open source database."
  :classname "org.postgresql.Driver"
  :quote sql-quote-double-quote)

(defdb oracle
  "Oracle Database."
  :classname "oracle.jdbc.driver.OracleDriver"
  :quote sql-quote-double-quote)

(defdb sqlite
  "The in-process SQL database engine."
  :classname "org.sqlite.JDBC"
  :quote sql-quote-double-quote)

(defdb sqlserver
  "Microsoft SQL server."
  :classname "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  :quote sql-quote-double-quote)

(defdb vertica
  "The Real-Time Analytics Platform."
  :classname "com.vertica.jdbc.Driver"
  :quote sql-quote-double-quote)
