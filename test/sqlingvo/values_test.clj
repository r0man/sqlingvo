(ns sqlingvo.values-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [sqlingvo.core :refer :all]
            [sqlingvo.test :refer [ast= db sql=]]))

(deftest test-values
  (sql= (values db [[1 "one"] [2 "two"] [3 "three"]])
        ["VALUES (1, ?), (2, ?), (3, ?)"
         "one" "two" "three"]))

(deftest test-values-with-default
  (sql= (values db :default)
        ["DEFAULT VALUES"]))

(deftest test-values-with-maps
  (sql= (values db [{:code "B6717"
                     :title "Tampopo"}
                    {:code "HG120"
                     :title "The Dinner Game"}])
        ["VALUES (?, ?), (?, ?)"
         "B6717" "Tampopo"
         "HG120" "The Dinner Game"]))

(deftest test-values-with-exprs
  (sql= (values db [['(cast "192.168.0.1" :inet)
                     "192.168.0.10"
                     "192.168.1.43"]])
        ["VALUES (CAST(? AS inet), ?, ?)"
         "192.168.0.1"
         "192.168.0.10"
         "192.168.1.43"]))

(deftest test-select-in-values
  (sql= (select db [:*]
          (from :machines)
          (where `(in :ip-address
                      ~(values [['(cast "192.168.0.1" :inet)]
                                ["192.168.0.10"]
                                ["192.168.1.43"]]))))
        [(str "SELECT * FROM \"machines\" WHERE \"ip-address\" "
              "IN (VALUES (CAST(? AS inet)), (?), (?))")
         "192.168.0.1"
         "192.168.0.10"
         "192.168.1.43"]))

(deftest test-select-from-values
  (sql= (select db [:f.*]
          (from (as :films :f)
                (as (values [["MGM" "Horror"]
                             ["UA" "Sci-Fi"]])
                    :t [:studio :kind]))
          (where '(and (= :f.studio :t.studio)
                       (= :f.kind :t.kind))))
        [(str "SELECT \"f\".* FROM \"films\" \"f\", "
              "(VALUES (?, ?), (?, ?)) "
              "AS \"t\" (\"studio\", \"kind\") "
              "WHERE ((\"f\".\"studio\" = \"t\".\"studio\") "
              "and (\"f\".\"kind\" = \"t\".\"kind\"))")
         "MGM" "Horror" "UA" "Sci-Fi"]))

(deftest test-update-from-values
  (sql= (update db :employees
          {:salary '(* :salary :v.increase)}
          (from (as (values [[1 200000 1.2]
                             [2 400000 1.4]])
                    :v [:depno :target :increase]))
          (where '(and (= :employees.depno :v.depno)
                       (>= :employees.sales :v.target))))
        [(str "UPDATE \"employees\" "
              "SET \"salary\" = (\"salary\" * \"v\".\"increase\") "
              "FROM (VALUES (1, 200000, 1.2), (2, 400000, 1.4)) "
              "AS \"v\" (\"depno\", \"target\", \"increase\") "
              "WHERE ((\"employees\".\"depno\" = \"v\".\"depno\") "
              "and (\"employees\".\"sales\" >= \"v\".\"target\"))")]))
