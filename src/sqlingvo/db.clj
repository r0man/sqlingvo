(ns sqlingvo.db
  (:require [sqlingvo.util :refer :all]))

(defprotocol Keywordable
  (sql-keyword [db x]))

(defprotocol Nameable
  (sql-name [db x]))

(defprotocol Quoteable
  (sql-quote [db x]))

(defrecord Database [doc name sql-keyword sql-name sql-quote]
  Keywordable
  (sql-keyword [db x]
    ((or sql-keyword sql-name-underscore) x))
  Nameable
  (sql-name [db x]
    ((or sql-name sql-keyword-hyphenate) x))
  Quoteable
  (sql-quote [db x]
    ((or sql-quote sql-quote-backtick)
     (sqlingvo.db/sql-name db x))))

(defmacro defdb [name doc & {:as opts}]
  `(defn ~name [& [~'opts]]
     (map->Database
      (merge {:doc ~doc
              :classname ~(:classname opts)
              :subprotocol ~(clojure.core/name name)
              :sql-keyword ~(:keyword opts)
              :sql-name ~(:name opts)
              :sql-quote ~(:quote opts)}
             ~'opts))))

(defdb mysql
  "The world's most popular open source database."
  :classname "com.mysql.jdbc.Driver"
  :name sql-name-underscore
  :keyword sql-keyword-hyphenate
  :quote sql-quote-backtick)

(defdb postgresql
  "The world's most advanced open source database."
  :classname "org.postgresql.Driver"
  :name sql-name-underscore
  :keyword sql-keyword-hyphenate
  :quote sql-quote-double-quote)

(defdb oracle
  "Oracle Database."
  :classname "oracle.jdbc.driver.OracleDriver"
  :name sql-name-underscore
  :keyword sql-keyword-hyphenate
  :quote identity)

(defdb sqlite
  "The in-process SQL database engine."
  :classname "org.sqlite.JDBC"
  :name sql-name-underscore
  :keyword sql-keyword-hyphenate
  :quote sql-quote-double-quote)

(defdb sqlserver
  "Microsoft SQL server."
  :classname "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  :name sql-name-underscore
  :keyword sql-keyword-hyphenate
  :quote sql-quote-double-quote)

(defdb vertica
  "The Real-Time Analytics Platform."
  :classname "com.vertica.jdbc.Driver"
  :name sql-name-underscore
  :keyword sql-keyword-hyphenate
  :quote sql-quote-double-quote)
