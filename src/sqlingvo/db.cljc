(ns sqlingvo.db
  #?(:cljs (:require-macros [sqlingvo.db :refer [defdb]] ))
  (:require [sqlingvo.compiler :as compiler]
            [sqlingvo.util :as util]))

(defrecord Database [subprotocol])

(defmulti db
  "Return the `Database` record for :adapter or :subprotocol in
  `db-spec`."
  (fn [db-spec]
    (keyword (or (:scheme db-spec)
                 (:subprotocol db-spec)))))

(defmethod db :default [{:keys [subprotocol] :as db-spec}]
  (throw (ex-info (str "Unknown database subprotocol: "
                       (some-> db-spec :subprotocol name))
                  db-spec)))

(defmacro defdb
  "Define a database specification."
  [db-name doc & {:as opts}]
  `(do
     (defmethod db ~(keyword db-name) [~'db-spec]
       (map->Database
        (merge {:doc ~doc
                :eval-fn compiler/compile-stmt
                :subprotocol ~(name db-name)}
               ~opts
               ~'db-spec)))
     (defn ~(symbol db-name) [& [~'db-spec]]
       (db (assoc ~'db-spec :subprotocol ~(name db-name))))))
