(ns sqlingvo.db
  (:require [sqlingvo.compiler :as compiler]
            [sqlingvo.util :refer :all]))

(defrecord Database []
  compiler/Keywordable
  (sql-keyword [m x]
    ((or (:sql-keyword m) keyword) x))
  compiler/Nameable
  (sql-name [m x]
    ((or (:sql-name m) name) x))
  compiler/Quoteable
  (sql-quote [m x]
    ((or (:sql-quote m) sql-quote-backtick)
     (compiler/sql-name m x))))

(defmacro defdb [name doc & {:as opts}]
  `(defn ~name [& [~'opts]]
     (let [db# (map->Database
                (merge {:doc ~doc
                        :classname ~(:classname opts)
                        :subprotocol ~(clojure.core/name name)
                        :sql-keyword ~(:keyword opts)
                        :sql-name ~(:name opts)
                        :sql-quote ~(:quote opts)}
                       ~'opts))]
       (assoc db# :eval-fn #(compiler/compile-stmt db# %)))))

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
