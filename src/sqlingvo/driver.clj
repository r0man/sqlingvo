(ns sqlingvo.driver
  (:require [sqlingvo.driver.core :refer [eval-db]]))

(defn make-db [db-spec]
  (let [db (sqlingvo.db/postgresql db-spec)]
    (assoc db :eval-fn eval-db)))

(try
  (doseq [ns '[sqlingvo.driver.clojure
               sqlingvo.driver.funcool]]
    (try (require ns)
         (catch Exception _))))
