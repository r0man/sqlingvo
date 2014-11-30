(ns sqlingvo.db
  (:require [sqlingvo.util :refer :all]))

(defprotocol Keywordable
  (sql-keyword [obj x]))

(defprotocol Nameable
  (sql-name [obj x]))

(defprotocol Quoteable
  (sql-quote [obj x]))

(defrecord Database [])

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

(extend-protocol Keywordable
  clojure.lang.PersistentArrayMap
  (sql-keyword [m x]
    ((or (:sql-keyword m) sql-name-underscore) x)))

(extend-protocol Nameable
  clojure.lang.PersistentArrayMap
  (sql-name [m x]
    ((or (:sql-name m) sql-keyword-hyphenate) x)))

(extend-protocol Quoteable
  clojure.lang.PersistentArrayMap
  (sql-quote [m x]
    ((or (:sql-quote m) sql-quote-backtick)
     (sqlingvo.db/sql-name m x))))
