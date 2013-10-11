# SQLingvo [![Build Status](https://travis-ci.org/r0man/sqlingvo.png)](https://travis-ci.org/r0man/sqlingvo)

A SQL DSL in Clojure.

## Installation

Via Clojars: https://clojars.org/sqlingvo

## Usage

    (refer-clojure :exclude '[distinct group-by])
    (use 'sqlingvo.core)

## Compiling to SQL

### [Copy](http://www.postgresql.org/docs/9.2/static/sql-copy.html)

Copy from standard input.

    (sql (copy :country []
           (from :stdin)))
    ;=> ["COPY \"country\" FROM STDIN"]

Copy data from a file into the country table.

    (sql (copy :country []
           (from "/usr1/proj/bray/sql/country_data")))
    ;=> ["COPY \"country\" FROM ?" "/usr1/proj/bray/sql/country_data"]

Copy data from a file into the country table with columns in the given order.

    (sql (copy :country [:id :name]
           (from "/usr1/proj/bray/sql/country_data")))
    ;=> ["COPY \"country\" (\"id\", \"name\") FROM ?" "/usr1/proj/bray/sql/country_data"]

### [Create Table](http://www.postgresql.org/docs/9.2/static/sql-createtable.html)

    (sql (create-table :films
	   (column :code :char :length 5 :primary-key? true)
	   (column :title :varchar :length 40 :not-null? true)
	   (column :did :integer :not-null? true)
	   (column :date-prod :date)
	   (column :kind :varchar :length 10)
	   (column :len :interval)
	   (column :created-at :timestamp-with-time-zone :not-null? true :default '(now))
	   (column :updated-at :timestamp-with-time-zone :not-null? true :default '(now))))

    ;=> ["CREATE TABLE \"films\" ("
    ;=>  "\"code\" CHAR(5) PRIMARY KEY, "
    ;=>  "\"title\" VARCHAR(40) NOT NULL, "
    ;=>  "\"did\" INTEGER NOT NULL, "
    ;=>  "\"date_prod\" DATE, "
    ;=>  "\"kind\" VARCHAR(10), "
    ;=>  "\"len\" INTERVAL, "
    ;=>  "\"created_at\" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "
    ;=>  "\"updated_at\" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now())"]

### [Delete](http://www.postgresql.org/docs/9.2/static/sql-delete.html)

Clear the table films.

    (sql (delete :films))
    ;=> ["DELETE FROM \"films\""]

Delete all films but musicals.

    (sql (delete :films
           (where '(<> :kind "Musical"))))
    ;=> ["DELETE FROM \"films\" WHERE (\"kind\" <> ?)" "Musical"]

Delete completed tasks, returning full details of the deleted rows.

    (sql (delete :tasks
           (where '(= :status "DONE"))
           (returning *)))
    ;=> ["DELETE FROM \"tasks\" WHERE (\"status\" = ?) RETURNING *" "DONE"]

### [Insert](http://www.postgresql.org/docs/9.2/static/sql-insert.html)

Insert a single row into table films.

    (sql (insert :films []
           (values {:code "T_601" :title "Yojimbo" :did 106 :date-prod "1961-06-16" :kind "Drama"})))
    ;=> ["INSERT INTO \"films\" (\"did\", \"date_prod\", \"kind\", \"title\", \"code\") VALUES (?, ?, ?, ?, ?)"
    ;=>  106 "1961-06-16" "Drama" "Yojimbo" "T_601"]

Insert multiple rows into the table films using the multirow VALUES syntax.

    (sql (insert :films []
           (values [{:code "B6717" :title "Tampopo" :did 110 :date-prod "1985-02-10" :kind "Comedy"},
                    {:code "HG120" :title "The Dinner Game" :did 140 :date-prod "1985-02-10":kind "Comedy"}])))
    ;=> ["INSERT INTO \"films\" (\"did\", \"date_prod\", \"kind\", \"title\", \"code\") VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)"
    ;=>  110 "1985-02-10" "Comedy" "Tampopo" "B6717" 140 "1985-02-10" "Comedy" "The Dinner Game" "HG120"]

Insert a row consisting entirely of default values.

    (sql (insert :films []
           (values :default)))
    ;=> ["INSERT INTO \"films\" DEFAULT VALUES"]

Insert some rows into table films from a table tmp_films with the same column layout as films.

    (sql (insert :films []
           (select [*]
             (from :tmp-films)
             (where '(< :date_prod "2004-05-07")))))
    ;=> ["INSERT INTO \"films\" (SELECT * FROM \"tmp_films\" WHERE (\"date_prod\" < ?))" "2004-05-07"]

### [Select](http://www.postgresql.org/docs/9.2/static/sql-select.html)

Select all films.

    (sql (select [*] (from :films)))
    ;=> ["SELECT * FROM \"films\""]

Select all Comedy films.

    (sql (select [*]
           (from :films)
           (where '(= :kind "Comedy"))))
    ;=> ["SELECT * FROM \"films\" WHERE (\"kind\" = ?)" "Comedy"]

Retrieve the most recent weather report for each location.

    (sql (select (distinct [:location :time :report] :on [:location])
           (from :weather-reports)
           (order-by :location (desc :time))))
    ;=> ["SELECT DISTINCT ON (\"location\") \"location\", \"time\", \"report\" FROM \"weather_reports\" ORDER BY \"location\", \"time\" DESC"]

### [Update](http://www.postgresql.org/docs/9.2/static/sql-update.html)

Change the word Drama to Dramatic in the column kind of the table films.

    (sql (update :films {:kind "Dramatic"}
           (where '(= :kind "Drama"))))
    ;=> ["UPDATE \"films\" SET \"kind\" = ? WHERE (\"kind\" = ?)" "Dramatic" "Drama"]

### [Sorting Rows](http://www.postgresql.org/docs/9.2/static/queries-order.html)

The sort expression(s) can be any expression that would be valid in the query's select list.

    (sql (select [:a :b]
           (from :table-1)
           (order-by '(+ :a :b) :c)))
    ;=> ["SELECT \"a\", \"b\" FROM \"table_1\" ORDER BY (\"a\" + \"b\"), \"c\""]

A sort expression can also be the column label

    (sql (select [(as '(+ :a :b) :sum) :c]
           (from :table-1)
           (order-by :sum)))
    ;=> ["SELECT \"a\" + \"b\" AS \"sum\", \"c\" FROM \"table_1\" ORDER BY \"sum\""]

or the number of an output column.

    (sql (select [:a '(max :b)]
           (from :table-1)
           (group-by :a)
           (order-by 1)))
    ;=> ["SELECT \"a\", max(\"b\") FROM \"table_1\" GROUP BY \"a\" ORDER BY 1"]

For more complex examples, look at the [tests](https://github.com/r0man/sqlingvo/blob/master/test/sqlingvo/core_test.clj).

## Tips & Tricks

### Emacs

For better indentation in clojure-mode add this to your Emacs config.

    (add-hook
     'clojure-mode-hook
     (lambda ()
       (define-clojure-indent
         (copy 2)
         (create-table 1)
         (delete 1)
         (drop-table 1)
         (insert 2)
         (select 1)
         (truncate 1)
         (update 2))))

## License

Copyright © 2012-13 Roman Scherer

Distributed under the Eclipse Public License, the same as Clojure.
