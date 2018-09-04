(ns sqlingvo.db
  (:require [clojure.spec.alpha :as s]
            [sqlingvo.compiler :as compiler]
            [sqlingvo.url :as url]
            [sqlingvo.util :as util]))

(s/def ::classname string?)
(s/def ::eval-fn ifn?)
(s/def ::sql-quote ifn?)

(s/def ::db
  (s/keys :req-un [::classname ::eval-fn ::sql-quote]))

(defprotocol IDatabase
  (-db [db] "Convert `db` to a database."))

(defrecord Database [scheme]
  IDatabase
  (-db [db] db))

(defmulti vendor
  "Returns a map of `vendor` specific database options."
  (fn [vendor] (keyword vendor)))

(defmethod vendor :mysql [_]
  {:classname "com.mysql.cj.jdbc.Driver"
   :sql-quote util/sql-quote-backtick})

(defmethod vendor :postgres [_]
  {:classname "org.postgresql.Driver"
   :sql-quote util/sql-quote-double-quote})

(defmethod vendor :postgresql [_]
  {:classname "org.postgresql.Driver"
   :sql-quote util/sql-quote-double-quote})

(defmethod vendor :oracle [_]
  {:classname "oracle.jdbc.driver.OracleDriver"
   :sql-quote util/sql-quote-double-quote})

(defmethod vendor :sqlite [_]
  {:classname "org.sqlite.JDBC"
   :sql-quote util/sql-quote-double-quote})

(defmethod vendor :sqlserver [_]
  {:classname "com.microsoft.sqlserver.jdbc.SQLServerDriver"
   :sql-quote util/sql-quote-double-quote})

(defmethod vendor :vertica [_]
  {:classname "com.vertica.jdbc.Driver"
   :sql-quote util/sql-quote-double-quote})

(defmethod vendor :default [vendor]
  (throw (ex-info (str "Unsupported database vendor: " (name vendor))
                  {:vendor vendor})))

(defn db
  "Return a database for `spec`."
  [spec & [opts]]
  (merge (-db spec) opts))

(s/fdef db
  :args (s/cat :spec any? :opts (s/? (s/nilable map?)))
  :ret ::db)

(extend-protocol IDatabase

  #?(:clj clojure.lang.Keyword :cljs cljs.core/Keyword)
  (-db [k]
    (->> {:eval-fn compiler/compile-stmt :scheme k}
         (merge (vendor k))
         (map->Database)))

  #?(:clj clojure.lang.IPersistentMap :cljs cljs.core/PersistentArrayMap)
  (-db [{:keys [scheme] :as spec}]
    (or (some->> scheme keyword -db (merge spec) map->Database)
        (throw (ex-info (str "Unsupported database spec." (pr-str spec))
                        {:spec spec}))))

  #?(:clj String :cljs string)
  (-db [url]
    (-db (url/parse! url))))
