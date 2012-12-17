# SQLingvo [![Build Status](https://travis-ci.org/r0man/sqlingvo.png)](https://travis-ci.org/r0man/sqlingvo)

A SQL DSL in Clojure.

## Installation

Via Clojars: https://clojars.org/sqlingvo

## Usage

    (refer-clojure :exclude '[distinct group-by])
    (use 'sqlingvo.core)

## Examples

### [Copy](http://www.postgresql.org/docs/9.2/static/sql-copy.html)

Copy data from a file into the country table.

    (sql (copy :country
           (from "/usr1/proj/bray/sql/country_data")))
    ;=> ["COPY country FROM ?" "/usr1/proj/bray/sql/country_data"]

### [Delete](http://www.postgresql.org/docs/9.2/static/sql-delete.html)

Clear the table films.

    (sql (delete :films))
    ;=> ["DELETE FROM films"]

Delete all films but musicals.

    (sql (delete :films
           (where '(<> :kind "Musical"))))
    ;=> ["DELETE FROM films WHERE (kind <> ?)" "Musical"]

Delete completed tasks, returning full details of the deleted rows.

    (sql (delete :tasks
           (where '(= status "DONE"))
           (returning *)))
    ;=> ["DELETE FROM tasks WHERE (status = ?) RETURNING *" "DONE"]

### [Insert](http://www.postgresql.org/docs/9.2/static/sql-insert.html)

Insert a single row into table films.

    (sql (insert :films [{:code "T_601" :title "Yojimbo" :did 106 :date-prod "1961-06-16" :kind "Drama"}]))
    ;=> ["INSERT INTO films (did, date-prod, kind, title, code) VALUES (?, ?, ?, ?, ?)"
    ;=>  106 "1961-06-16" "Drama" "Yojimbo" "T_601"]

Insert multiple rows into the table films using the multirow VALUES syntax.

    (sql (insert :films [{:code "B6717" :title "Tampopo" :did 110 :date-prod "1985-02-10" :kind "Comedy"},
                         {:code "HG120" :title "The Dinner Game" :did 140 :date-prod "1985-02-10":kind "Comedy"}]))
    ;=> ["INSERT INTO films (did, date-prod, kind, title, code) VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)"
    ;=>  110 "1985-02-10" "Comedy" "Tampopo" "B6717" 140 "1985-02-10" "Comedy" "The Dinner Game" "HG120"]

Insert a row consisting entirely of default values.

    (sql (insert :films)
         (default-values))
    ;=> ["INSERT INTO films DEFAULT VALUES"]


Insert some rows into table films from a table tmp-films with the same column layout as films.

    (sql (insert :films
                 (-> (select *)
                     (from :tmp-films)
                     (where '(< :date-prod "2004-05-07")))))
    ;=> ["INSERT INTO films (SELECT * FROM tmp-films WHERE (date-prod < ?))" "2004-05-07"]


### [Select](http://www.postgresql.org/docs/9.2/static/sql-select.html)

Select all films.

    (sql (select [*] (from :films)))
    ;=> ["SELECT * FROM films"]

Select all Comedy films.

    (sql (select [*]
           (from :films)
           (where '(= :kind "Comedy"))))
    ;=> ["SELECT * FROM films WHERE (kind = ?)" "Comedy"]

Retrieve the most recent weather report for each location.

    (sql (select (distinct [:location :time :report] :on [:location])
           (from :weather-reports)
           (order-by [:location :time] :direction :desc)))
    ;=> ["SELECT DISTINCT ON (location) location, time, report FROM weather-reports ORDER BY location, time DESC"]

### [Update](http://www.postgresql.org/docs/9.2/static/sql-update.html)

Change the word Drama to Dramatic in the column kind of the table films.

    (sql (update :films {:kind "Dramatic"}
           (where '(= :kind "Drama"))))
    ;=> ["UPDATE films SET kind = ? WHERE (kind = ?)" "Dramatic" "Drama"]

### [Sorting Rows](http://www.postgresql.org/docs/9.2/static/queries-order.html)

The sort expression(s) can be any expression that would be valid in the query's select list.

    (sql (select [:a :b]
           (from :table-1)
           (order-by '(+ :a :b) :c)))
    ;=> ["SELECT a, b FROM table-1 ORDER BY (a + b), c"]

A sort expression can also be the column label

    (sql (select [(as '(+ :a :b) :sum) :c]
           (from :table-1)
           (order-by :sum)))
    ;=> ["SELECT a + b AS sum, c FROM table-1 ORDER BY sum"]

or the number of an output column.

    (sql (select [:a '(max :b)]
           (from :table-1)
           (group-by :a)
           (order-by 1)))
    ;=> ["SELECT a, max(b) FROM table-1 GROUP BY a ORDER BY 1"]

## License

Copyright © 2012 Roman Scherer

Distributed under the Eclipse Public License, the same as Clojure.
