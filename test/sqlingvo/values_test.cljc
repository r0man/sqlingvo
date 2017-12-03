(ns sqlingvo.values-test
  (:require #?(:clj [sqlingvo.test :refer [db sql=]]
               :cljs [sqlingvo.test :refer [db] :refer-macros [sql=]])
            [clojure.test :refer [deftest is]]
            [sqlingvo.core :as sql]))

(deftest test-values-keyword-db
  (sql= (sql/values :postgresql [[1 "one"] [2 "two"] [3 "three"]])
        ["VALUES (1, ?), (2, ?), (3, ?)"
         "one" "two" "three"]))

(deftest test-values
  (sql= (sql/values db [[1 "one"] [2 "two"] [3 "three"]])
        ["VALUES (1, ?), (2, ?), (3, ?)"
         "one" "two" "three"]))

(deftest test-values-with-default
  (sql= (sql/values db :default)
        ["DEFAULT VALUES"]))

(deftest test-values-with-maps
  (sql= (sql/values db [{:code "B6717"
                         :title "Tampopo"}
                        {:code "HG120"
                         :title "The Dinner Game"}])
        ["VALUES (?, ?), (?, ?)"
         "B6717" "Tampopo"
         "HG120" "The Dinner Game"]))

(deftest test-values-with-namespaced-keys
  (sql= (sql/values db [{:sqlingvo/code "B6717"
                         :sqlingvo/title "Tampopo"}
                        {:sqlingvo/code "HG120"
                         :sqlingvo/title "The Dinner Game"}])
        ["VALUES (?, ?), (?, ?)"
         "B6717" "Tampopo"
         "HG120" "The Dinner Game"]))

(deftest test-values-with-exprs
  (sql= (sql/values db [['(cast "192.168.0.1" :inet)
                         "192.168.0.10"
                         "192.168.1.43"]])
        ["VALUES (CAST(? AS INET), ?, ?)"
         "192.168.0.1"
         "192.168.0.10"
         "192.168.1.43"]))

(deftest test-select-in-values
  (sql= (sql/select db [:*]
          (sql/from :machines)
          (sql/where `(in :ip-address
                          ~(sql/values [['(cast "192.168.0.1" :inet)]
                                        ["192.168.0.10"]
                                        ["192.168.1.43"]]))))
        [(str "SELECT * FROM \"machines\" WHERE \"ip-address\" "
              "IN (VALUES (CAST(? AS INET)), (?), (?))")
         "192.168.0.1"
         "192.168.0.10"
         "192.168.1.43"]))

(deftest test-select-from-values
  (sql= (sql/select db [:f.*]
          (sql/from
           (sql/as :films :f)
           (sql/as (sql/values
                    [["MGM" "Horror"]
                     ["UA" "Sci-Fi"]])
                   :t [:studio :kind]))
          (sql/where
           '(and (= :f.studio :t.studio)
                 (= :f.kind :t.kind))))
        [(str "SELECT \"f\".* FROM \"films\" \"f\", "
              "(VALUES (?, ?), (?, ?)) "
              "AS \"t\" (\"studio\", \"kind\") "
              "WHERE ((\"f\".\"studio\" = \"t\".\"studio\") "
              "and (\"f\".\"kind\" = \"t\".\"kind\"))")
         "MGM" "Horror" "UA" "Sci-Fi"]))

(deftest test-update-from-values
  (sql= (sql/update db :employees
          {:salary '(* :salary :v.increase)}
          (sql/from (sql/as
                     (sql/values
                      [[1 200000 1.2]
                       [2 400000 1.4]])
                     :v [:depno :target :increase]))
          (sql/where
           '(and (= :employees.depno :v.depno)
                 (>= :employees.sales :v.target))))
        [(str "UPDATE \"employees\" "
              "SET \"salary\" = (\"salary\" * \"v\".\"increase\") "
              "FROM (VALUES (1, 200000, 1.2), (2, 400000, 1.4)) "
              "AS \"v\" (\"depno\", \"target\", \"increase\") "
              "WHERE ((\"employees\".\"depno\" = \"v\".\"depno\") "
              "and (\"employees\".\"sales\" >= \"v\".\"target\"))")]))
