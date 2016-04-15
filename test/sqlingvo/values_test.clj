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
        ["DEFAULT VALUES"])
  (ast= (values :default)
        {:type :default
         :db nil
         :op :values}))

(deftest test-values-with-maps
  (sql= (values db [{:code "B6717"
                     :title "Tampopo"}
                    {:code "HG120"
                     :title "The Dinner Game"}])
        ["VALUES (?, ?), (?, ?)"
         "B6717" "Tampopo"
         "HG120" "The Dinner Game"])
  (ast= (values [{:code "B6717"
                  :title "Tampopo"}
                 {:code "HG120"
                  :title "The Dinner Game"}])
        {:op :values,
         :db nil
         :columns
         [{:children [:name], :name :code, :op :column}
          {:children [:name], :name :title, :op :column}],
         :type :records
         :values
         [{:code
           {:val "B6717",
            :type :string,
            :op :constant,
            :literal? true,
            :form "B6717"},
           :title
           {:val "Tampopo",
            :type :string,
            :op :constant,
            :literal? true,
            :form "Tampopo"}}
          {:code
           {:val "HG120",
            :type :string,
            :op :constant,
            :literal? true,
            :form "HG120"},
           :title
           {:val "The Dinner Game",
            :type :string,
            :op :constant,
            :literal? true,
            :form "The Dinner Game"}}]}))

(deftest test-values-with-exprs
  (sql= (values db [['(cast "192.168.0.1" :inet)
                     "192.168.0.10"
                     "192.168.1.43"]])
        ["VALUES (CAST(? AS inet), ?, ?)"
         "192.168.0.1"
         "192.168.0.10"
         "192.168.1.43"])
  (ast= (values [['(cast "192.168.0.1" :inet)
                  "192.168.0.10"
                  "192.168.1.43"]])
        {:op :values,
         :db nil
         :columns nil,
         :type :exprs
         :values
         [[{:args
            [{:val "192.168.0.1",
              :type :string,
              :op :constant,
              :literal? true,
              :form "192.168.0.1"}
             {:children [:name], :name :inet, :op :column}],
            :children [:args],
            :name "cast",
            :op :fn}
           {:val "192.168.0.10",
            :type :string,
            :op :constant,
            :literal? true,
            :form "192.168.0.10"}
           {:val "192.168.1.43",
            :type :string,
            :op :constant,
            :literal? true,
            :form "192.168.1.43"}]]}))

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
