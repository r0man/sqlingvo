(ns sqlingvo.delete-test
  (:require #?(:clj [sqlingvo.test :refer [db sql=]]
               :cljs [sqlingvo.test :refer [db] :refer-macros [sql=]])
            [clojure.test :refer [deftest is]]
            [sqlingvo.core :as sql]))

(deftest test-delete-keyword-db
  (sql= (sql/delete :postgresql :films)
        ["DELETE FROM \"films\""]))

(deftest test-delete-films
  (sql= (sql/delete db :films)
        ["DELETE FROM \"films\""]))

(deftest test-delete-all-films-but-musicals
  (sql= (sql/delete db :films
          (sql/where '(<> :kind "Musical")))
        ["DELETE FROM \"films\" WHERE (\"kind\" <> ?)" "Musical"]))

(deftest test-delete-completed-tasks-returning-all
  (sql= (sql/delete db :tasks
          (sql/where '(= :status "DONE"))
          (sql/returning :*))
        ["DELETE FROM \"tasks\" WHERE (\"status\" = ?) RETURNING *" "DONE"]))

(deftest test-delete-films-by-producer-name
  (sql= (sql/delete db :films
          (sql/where `(in :producer-id
                          ~(sql/select db [:id]
                             (sql/from :producers)
                             (sql/where '(= :name "foo"))))))
        [(str "DELETE FROM \"films\" WHERE \"producer-id\" IN (SELECT \"id\" "
              "FROM \"producers\" WHERE (\"name\" = ?))")
         "foo"]))

(deftest test-delete-quotes
  (sql= (sql/delete db :quotes
          (sql/where `(and (= :company-id 1)
                           (> :date ~(sql/select db ['(min :date)]
                                       (sql/from :import)))
                           (> :date ~(sql/select db ['(max :date)]
                                       (sql/from :import))))))
        [(str "DELETE FROM \"quotes\" WHERE ((\"company-id\" = 1) and "
              "(\"date\" > (SELECT min(\"date\") FROM \"import\")) and "
              "(\"date\" > (SELECT max(\"date\") FROM \"import\")))")]))
