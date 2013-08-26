(ns sqlingvo.vendor
  (:require [inflections.core :refer [hyphenize underscore]]))

(defprotocol Keywordable
  (sql-keyword [vendor x]))

(defprotocol Nameable
  (sql-name [vendor x]))

(defprotocol Quoteable
  (sql-quote [vendor x]))

(def sql-name-underscore
  (comp underscore name))

(def sql-keyword-hyphenize
  (comp keyword hyphenize))

(defn sql-quote-backtick [x]
  (str "`" x "`"))

(defn sql-quote-double-quote [x]
  (str "\"" x "\""))

(defmacro defvendor [name doc & {:as opts}]
  `(defrecord ~name [~'spec]
     Keywordable
     (sql-keyword [~'vendor ~'x]
       ((or ~(:keyword opts) sql-name-underscore) ~'x))
     Nameable
     (sql-name [~'vendor ~'x]
       ((or ~(:name opts) sql-keyword-hyphenize) ~'x))
     Quoteable
     (sql-quote [~'vendor ~'x]
       (~(or (:quote opts) sql-quote-backtick)
        (sql-name ~'vendor ~'x)))))

(defvendor mysql
  "The world's most popular open source database."
  :name sql-name-underscore
  :keyword sql-keyword-hyphenize
  :quote sql-quote-backtick)

(defvendor postgresql
  "The world's most advanced open source database."
  :name sql-name-underscore
  :keyword sql-keyword-hyphenize
  :quote sql-quote-double-quote)

(defvendor sqlite
  "The in-process SQL database engine."
  :name sql-name-underscore
  :keyword sql-keyword-hyphenize
  :quote sql-quote-double-quote)

(defvendor vertica
  "The Real-Time Analytics Platform."
  :name sql-name-underscore
  :keyword sql-keyword-hyphenize
  :quote sql-quote-double-quote)
