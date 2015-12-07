(ns sqlingvo.driver.clojure
  (:require [clojure.java.jdbc :as jdbc]
            [sqlingvo.driver.core :refer :all]
            [sqlingvo.compiler :refer [compile-stmt]]))

(defmethod close-db 'clojure.java.jdbc [db]
  (.close (:connection db)))

(defmethod eval-db* 'clojure.java.jdbc
  [{:keys [db] :as ast}]
  (let [sql (compile-stmt db ast)]
    (case (:op ast)
      :create-table (jdbc/execute! db sql)
      :drop-table (jdbc/execute! db sql)
      :insert (jdbc/execute! db sql)
      :select (jdbc/query db sql))))

(defmethod open-db 'clojure.java.jdbc [db]
  (assoc db :connection (jdbc/get-connection db)))
