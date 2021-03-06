(ns sqlingvo.create-table-test
  (:require #?(:clj [sqlingvo.test :refer [db sql=]]
               :cljs [sqlingvo.test :refer [db] :refer-macros [sql=]])
            [clojure.test :refer [are deftest is]]
            [sqlingvo.core :as sql]))

(deftest test-create-table-keyword-db
  (sql= (sql/create-table :postgresql :measurement-y2006m02
          (sql/inherits :measurement))
        ["CREATE TABLE \"measurement-y2006m02\" () INHERITS (\"measurement\")"]))

(deftest test-create-table-inherits
  (sql= (sql/create-table db :measurement-y2006m02
          (sql/inherits :measurement))
        ["CREATE TABLE \"measurement-y2006m02\" () INHERITS (\"measurement\")"]))

(deftest test-create-table-inherits-check
  (sql= (sql/create-table db :measurement-y2006m02
          (sql/check '(and (>= :logdate (cast "2006-02-01" :date))
                           (< :logdate (cast "2006-03-01" :date))))
          (sql/inherits :measurement))
        [(str "CREATE TABLE \"measurement-y2006m02\" ("
              "CHECK ((\"logdate\" >= CAST(? AS DATE)) and "
              "(\"logdate\" < CAST(? AS DATE)))) "
              "INHERITS (\"measurement\")")
         "2006-02-01" "2006-03-01"]))

(deftest test-create-table-inherits-check-multiple
  (sql= (sql/create-table db :measurement-y2006m02
          (sql/check '(and (>= :logdate (cast "2006-02-01" :date))
                           (< :logdate (cast "2006-03-01" :date))))
          (sql/inherits :measurement))
        [(str "CREATE TABLE \"measurement-y2006m02\" ("
              "CHECK ((\"logdate\" >= CAST(? AS DATE)) and "
              "(\"logdate\" < CAST(? AS DATE)))) "
              "INHERITS (\"measurement\")")
         "2006-02-01" "2006-03-01"]))

(deftest test-create-table-inherits-check-like
  (sql= (sql/create-table db :measurement-y2006m02
          (sql/like :measurements :including [:all])
          (sql/check '(>= :logdate (cast "2006-02-01" :date)))
          (sql/check '(< :logdate (cast "2006-03-01" :date)))
          (sql/inherits :measurement))
        [(str "CREATE TABLE \"measurement-y2006m02\" ("
              "LIKE \"measurements\" INCLUDING ALL, "
              "CHECK (\"logdate\" >= CAST(? AS DATE)), "
              "CHECK (\"logdate\" < CAST(? AS DATE))) "
              "INHERITS (\"measurement\")")
         "2006-02-01" "2006-03-01"]))

(deftest test-create-table-tmp-if-not-exists-inherits
  (sql= (sql/create-table db :import
          (sql/temporary true)
          (sql/if-not-exists true)
          (sql/inherits :quotes))
        [(str "CREATE TEMPORARY TABLE IF NOT EXISTS "
              "\"import\" () INHERITS (\"quotes\")")]))

(deftest test-create-table-tmp-if-not-exists-false
  (sql= (sql/create-table db :import
          (sql/temporary true)
          (sql/if-not-exists false)
          (sql/inherits :quotes))
        ["CREATE TEMPORARY TABLE \"import\" () INHERITS (\"quotes\")"]))

(deftest test-create-table-like-including-defaults
  (sql= (sql/create-table db :tmp-films
          (sql/like :films :including [:defaults]))
        ["CREATE TABLE \"tmp-films\" (LIKE \"films\" INCLUDING DEFAULTS)"]))

(deftest test-create-table-like-excluding-indexes
  (sql= (sql/create-table db :tmp-films
          (sql/like :films :excluding [:indexes]))
        ["CREATE TABLE \"tmp-films\" (LIKE \"films\" EXCLUDING INDEXES)"]))

(deftest test-create-table-films
  (sql= (sql/create-table db :films
          (sql/column :code :char :size 5 :primary-key? true)
          (sql/column :title :varchar :size 40 :not-null? true)
          (sql/column :did :integer :not-null? true)
          (sql/column :date-prod :date)
          (sql/column :kind :varchar :size 10)
          (sql/column :len :interval)
          (sql/column :created-at :timestamp-with-time-zone :not-null? true :default '(now))
          (sql/column :updated-at :timestamp-with-time-zone :not-null? true :default '(now)))
        [(str "CREATE TABLE \"films\" ("
              "\"code\" CHAR(5) PRIMARY KEY, "
              "\"title\" VARCHAR(40) NOT NULL, "
              "\"did\" INTEGER NOT NULL, "
              "\"date-prod\" DATE, "
              "\"kind\" VARCHAR(10), "
              "\"len\" INTERVAL, "
              "\"created-at\" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "
              "\"updated-at\" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now())")]))

(deftest test-create-table-compound-primary-key
  (sql= (sql/create-table db :ratings
          (sql/column :id :serial)
          (sql/column :user-id :integer :not-null? true)
          (sql/column :spot-id :integer :not-null? true)
          (sql/column :rating :integer :not-null? true)
          (sql/column :created-at :timestamp-with-time-zone :not-null? true :default '(now))
          (sql/column :updated-at :timestamp-with-time-zone :not-null? true :default '(now))
          (sql/primary-key :user-id :spot-id :created-at))
        [(str "CREATE TABLE \"ratings\" ("
              "\"id\" SERIAL, "
              "\"user-id\" INTEGER NOT NULL, "
              "\"spot-id\" INTEGER NOT NULL, "
              "\"rating\" INTEGER NOT NULL, "
              "\"created-at\" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "
              "\"updated-at\" TIMESTAMP WITH "
              "TIME ZONE NOT NULL DEFAULT now(), "
              "PRIMARY KEY(\"user-id\", \"spot-id\", \"created-at\"))")]))

(deftest test-create-table-references-schema-table-column
  (sql= (sql/create-table db :countries
          (sql/column :id :serial)
          (sql/column :continent-id :integer :references :public.continents.id))
        [(str "CREATE TABLE \"countries\" ("
              "\"id\" SERIAL, \"continent-id\" INTEGER REFERENCES \"public\".\"continents\" (\"id\"))")]))

(deftest test-create-table-references-table-column
  (sql= (sql/create-table db :countries
          (sql/column :id :serial)
          (sql/column :continent-id :integer :references :continents.id))
        [(str "CREATE TABLE \"countries\" ("
              "\"id\" SERIAL, \"continent-id\" INTEGER REFERENCES \"continents\" (\"id\"))")]))

(deftest test-create-table-references-table
  (sql= (sql/create-table db :countries
          (sql/column :id :serial)
          (sql/column :continent-id :integer :references :continents))
        [(str "CREATE TABLE \"countries\" ("
              "\"id\" SERIAL, \"continent-id\" INTEGER REFERENCES \"continents\")")]))

(deftest test-create-table-array-column
  (sql= (sql/create-table db :ratings
          (sql/column :x :text :array? true))
        ["CREATE TABLE \"ratings\" (\"x\" TEXT[])"]))

(deftest test-create-table-geometry
  (sql= (sql/create-table db :my-table
          (sql/column :my-geom :geometry))
        ["CREATE TABLE \"my-table\" (\"my-geom\" GEOMETRY)"]))

(deftest test-create-table-geography
  (sql= (sql/create-table db :my-table
          (sql/column :my-geom :geography))
        ["CREATE TABLE \"my-table\" (\"my-geom\" GEOGRAPHY)"]))

(deftest test-create-table-geometry-collection
  (sql= (sql/create-table db :my-table
          (sql/column :my-geom :geometry :geometry :geometry-collection))
        ["CREATE TABLE \"my-table\" (\"my-geom\" GEOMETRY(GEOMETRYCOLLECTION))"]))

(deftest test-create-table-line-string
  (sql= (sql/create-table db :my-table
          (sql/column :my-geom :geometry :geometry :line-string))
        ["CREATE TABLE \"my-table\" (\"my-geom\" GEOMETRY(LINESTRING))"]))

(deftest test-create-table-multi-line-string
  (sql= (sql/create-table db :my-table
          (sql/column :my-geom :geometry :geometry :multi-line-string))
        ["CREATE TABLE \"my-table\" (\"my-geom\" GEOMETRY(MULTILINESTRING))"]))

(deftest test-create-table-multi-polygon
  (sql= (sql/create-table db :my-table
          (sql/column :my-geom :geometry :geometry :multi-polygon))
        ["CREATE TABLE \"my-table\" (\"my-geom\" GEOMETRY(MULTIPOLYGON))"]))

(deftest test-create-table-multi-point
  (sql= (sql/create-table db :my-table
          (sql/column :my-geom :geometry :geometry :multi-point))
        ["CREATE TABLE \"my-table\" (\"my-geom\" GEOMETRY(MULTIPOINT))"]))

(deftest test-create-table-point
  (sql= (sql/create-table db :my-table
          (sql/column :my-geom :geometry :geometry :point))
        ["CREATE TABLE \"my-table\" (\"my-geom\" GEOMETRY(POINT))"]))

(deftest test-create-table-point-srid
  (sql= (sql/create-table db :my-table
          (sql/column :my-geom :geometry :geometry :point :srid 4326))
        ["CREATE TABLE \"my-table\" (\"my-geom\" GEOMETRY(POINT, 4326))"]))
