(ns sqlingvo.delete-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.test :refer :all]
            [sqlingvo.core :refer :all]
            [sqlingvo.expr :refer :all]
            [sqlingvo.test :refer [db sql= with-stmt]]))

(deftest test-delete-films
  (with-stmt
    ["DELETE FROM \"films\""]
    (delete db :films)
    (is (= :delete (:op stmt)))
    (is (= (parse-table :films) (:table stmt)))))

(deftest test-delete-all-films-but-musicals
  (with-stmt
    ["DELETE FROM \"films\" WHERE (\"kind\" <> ?)" "Musical"]
    (delete db :films
      (where '(<> :kind "Musical")))
    (is (= :delete (:op stmt)))
    (is (= (parse-table :films) (:table stmt)))
    (is (= (parse-condition '(<> :kind "Musical")) (:where stmt)))))

(deftest test-delete-completed-tasks-returning-all
  (with-stmt
    ["DELETE FROM \"tasks\" WHERE (\"status\" = ?) RETURNING *" "DONE"]
    (delete db :tasks
      (where '(= :status "DONE"))
      (returning *))
    (is (= :delete (:op stmt)))
    (is (= (parse-table :tasks) (:table stmt)))
    (is (= (parse-condition '(= :status "DONE")) (:where stmt)))
    (is (= [(parse-expr *)] (:returning stmt)))))

(deftest test-delete-films-by-producer-name
  (with-stmt
    ["DELETE FROM \"films\" WHERE \"producer-id\" IN (SELECT \"id\" FROM \"producers\" WHERE (\"name\" = ?))" "foo"]
    (delete db :films
      (where `(in :producer-id
                  ~(select db [:id]
                     (from :producers)
                     (where '(= :name "foo"))))))
    (is (= :delete (:op stmt)))
    (is (= (parse-table :films) (:table stmt)))
    (is (= (parse-condition `(in :producer-id
                                 ~(select db [:id]
                                    (from :producers)
                                    (where '(= :name "foo")))))
           (:where stmt)))))

(deftest test-delete-quotes
  (with-stmt
    [(str "DELETE FROM \"quotes\" WHERE ((\"company-id\" = 1) and (\"date\" > (SELECT min(\"date\") FROM \"import\")) and "
          "(\"date\" > (SELECT max(\"date\") FROM \"import\")))")]
    (delete db :quotes
      (where `(and (= :company-id 1)
                   (> :date ~(select db ['(min :date)] (from :import)))
                   (> :date ~(select db ['(max :date)] (from :import))))))
    (is (= :delete (:op stmt)))
    (is (= (parse-table :quotes) (:table stmt)))
    (is (= (parse-condition `(and (= :company-id 1)
                                  (> :date ~(select db ['(min :date)] (from :import)))
                                  (> :date ~(select db ['(max :date)] (from :import)))))
           (:where stmt)))))
