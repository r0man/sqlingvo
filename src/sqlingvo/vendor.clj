(ns sqlingvo.vendor
  (:require [inflections.core :refer [hyphenize underscore]]))

(def sql-name-underscore
  (comp underscore name))

(def sql-keyword-hyphenize
  (comp keyword hyphenize))

(defprotocol ISqlKeyword
  (sql-keyword [vendor x]))

(defprotocol ISqlName
  (sql-name [vendor x]))

(defprotocol ISqlQuote
  (sql-quote [vendor x]))

(defrecord MySQL [spec]
  ISqlKeyword
  (sql-keyword [vendor x]
    (sql-keyword-hyphenize x))
  ISqlName
  (sql-name [vendor x]
    (sql-name-underscore x))
  ISqlQuote
  (sql-quote [vendor x]
    (str "`" (sql-name vendor x) "`")))

(defrecord PostgreSQL [spec]
  ISqlKeyword
  (sql-keyword [vendor x]
    (sql-keyword-hyphenize x))
  ISqlName
  (sql-name [vendor x]
    (sql-name-underscore x))
  ISqlQuote
  (sql-quote [vendor x]
    (str \" (sql-name vendor x) \")))

(defrecord Vertica [spec]
  ISqlKeyword
  (sql-keyword [vendor x]
    (sql-keyword-hyphenize x))
  ISqlName
  (sql-name [vendor x]
    (sql-name-underscore x))
  ISqlQuote
  (sql-quote [vendor x]
    (str \" (sql-name vendor x) \")))

(defn mysql [spec]
  (->MySQL spec))

(defn postgresql [spec]
  (->PostgreSQL spec))

(defn vertica [spec]
  (->Vertica spec))

(comment
  (sql-name (mysql {}) "a-1")
  (sql-name (mysql {}) :a-1)
  (sql-keyword (mysql {}) :a-1)
  (sql-quote (mysql {}) :a-1))
