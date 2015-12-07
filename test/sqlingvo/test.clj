(ns sqlingvo.test
  (:require [clojure.test :refer :all]
            [sqlingvo.driver :refer [make-db]]
            [sqlingvo.driver.core :refer [close-db open-db]]))

(def db
  (make-db
   {:vendor "postgresql"
    :name "sqlingvo"
    :host "localhost"
    :user "tiger"
    :password "scotch"
    :subname "//localhost/sqlingvo"}))

(defmacro with-backends [[db-sym] & body]
  `(doseq [backend# ['clojure.java.jdbc 'jdbc.core]]
     (if (find-ns backend#)
       (let [db# (open-db (assoc ~db :backend backend#)), ~db-sym db#]
         (try (testing (str "Testing backend " (str backend#)) ~@body)
              (finally (close-db db#))))
       (.println *err* (format "WARNING: Can't find %s backend, skipping tests." backend#)))))
