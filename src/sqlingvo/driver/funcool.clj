(ns sqlingvo.driver.funcool
  (:require [jdbc.core :as jdbc]
            [sqlingvo.driver.core :refer :all]
            [sqlingvo.compiler :refer [compile-stmt]]))

(defmethod close-db 'jdbc.core [db]
  (.close (:connection db)))

(defmethod eval-db* 'jdbc.core
  [{:keys [db] :as ast}]
  (let [sql (compile-stmt db ast)]
    (case (:op ast)
      :create-table
      (jdbc/execute db sql)
      :drop-table
      (jdbc/execute db sql)
      :insert
      (if (:returning ast)
        (jdbc/fetch db sql)
        (jdbc/execute db sql))
      :select
      (jdbc/fetch db sql))))

(defmethod open-db 'jdbc.core [db]
  (assoc db :connection (jdbc/connection db)))
