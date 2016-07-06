(ns sqlingvo.url-test
  (:require #?(:clj [clojure.test :refer :all]
               :cljs [cljs.test :refer-macros [are deftest is]])
            [clojure.string :as str]
            [clojure.test.check :as tc]
            [clojure.test.check.clojure-test #?(:clj :refer :cljs :refer-macros) [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties #?(:clj :refer :cljs :refer-macros) [for-all]]
            [sqlingvo.url :as url]))

(def invalid-urls
  "The generator for invalid database URLs."
  (gen/elements [nil "" "x"]))

(def urls
  "The generator for database URLs."
  (gen/elements
   ["mysql://localhost/datumbazo"
    "postgresql://tiger:scotch@localhost:5432/datumbazo?a=1&b=2"]))

(def pools
  "The generator for database pool names."
  (gen/elements ["bonecp" "c3p0" "hikaricp"]))

(deftest test-parse
  (let [db (url/parse "mysql://localhost:5432/datumbazo")]
    (is (nil? (:pool db)))
    (is (= "localhost" (:server-name db)))
    (is (= 5432 (:server-port db)))
    (is (= "datumbazo" (:name db)))
    (is (nil? (:query-params db)))
    (is (= :mysql (:scheme db)))))

(defspec test-parse-scheme
  (for-all [url urls] (keyword? (:scheme (url/parse url)))))

(defspec test-parse-server-name
  (for-all [url urls] (not (str/blank? (:server-name (url/parse url))))))

(defspec test-parse-name
  (for-all [url urls] (not (str/blank? (:name (url/parse url))))))

(defspec test-parse-invalid-url
  (for-all [url invalid-urls] (nil? (url/parse url))))

(defspec test-parse!-invalid-url
  (for-all
   [url invalid-urls]
   (try (url/parse! url)
        (assert false (str "Expected invalid URL, but was valid:" url))
        (catch #?(:clj clojure.lang.ExceptionInfo
                  :cljs js/Error) _ true))))

(defspec test-parse-with-pool
  (for-all
   [pool pools, url urls]
   (= (:pool (url/parse (str pool ":" url)))
      (keyword pool))))

(deftest test-parse-with-query-params
  (let [db (url/parse "postgresql://tiger:scotch@localhost:5432/datumbazo?a=1&b=2")]
    (is (nil? (:pool db)))
    (is (= "tiger" (:username db)))
    (is (= "scotch" (:password db)))
    (is (= "localhost" (:server-name db)))
    (is (= 5432 (:server-port db)))
    (is (= "datumbazo" (:name db)))
    (is (= {:a "1" :b "2"} (:query-params db)))
    (is (= :postgresql (:scheme db)))))

(deftest test-format-url
  (let [url "postgresql://tiger:scotch@localhost/datumbazo?a=1&b=2"]
    (is (= (url/format (url/parse url)) url)))
  (let [url "postgresql://tiger:scotch@localhost:5432/datumbazo?a=1&b=2"]
    (is (= (url/format (url/parse "postgresql://tiger:scotch@localhost:5432/datumbazo?a=1&b=2"))
           "postgresql://tiger:scotch@localhost/datumbazo?a=1&b=2"))))
