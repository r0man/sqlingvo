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
      (merge ~'opts
             {:doc ~doc
              :name ~(keyword name)
              :sql-keyword ~(:keyword opts)
              :sql-name ~(:name opts)
              :sql-quote ~(:quote opts)}))))

(defdb mysql
  "The world's most popular open source database."
  :name sql-name-underscore
  :keyword sql-keyword-hyphenate
  :quote sql-quote-backtick)

(defdb postgresql
  "The world's most advanced open source database."
  :name sql-name-underscore
  :keyword sql-keyword-hyphenate
  :quote sql-quote-double-quote)

(defdb oracle
  "Oracle Database."
  :name sql-name-underscore
  :keyword sql-keyword-hyphenate
  :quote identity)

(defdb sqlite
  "The in-process SQL database engine."
  :name sql-name-underscore
  :keyword sql-keyword-hyphenate
  :quote sql-quote-double-quote)

(defdb sqlserver
  "Microsoft SQL server."
  :name sql-name-underscore
  :keyword sql-keyword-hyphenate
  :quote sql-quote-double-quote)

(defdb vertica
  "The Real-Time Analytics Platform."
  :name sql-name-underscore
  :keyword sql-keyword-hyphenate
  :quote sql-quote-double-quote)
