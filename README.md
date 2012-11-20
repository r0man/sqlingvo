# SQLingvo [![Build Status](https://travis-ci.org/r0man/sqlingvo.png)](https://travis-ci.org/r0man/sqlingvo)

A SQL DSL in Clojure.

## Installation

Via Clojars: https://clojars.org/sqlingvo

## Usage

    (refer-clojure :exclude '[group-by])
    (use 'sqlingvo.core)

## Examples

### Copy

Copy data from a file into the country table.

    (sql (copy :country)
         (from "/usr1/proj/bray/sql/country_data"))
    ;=> ["COPY country FROM ?" "/usr1/proj/bray/sql/country_data"]

### Delete

Clear the table films.

    (sql (delete :films))
    ;=> ["DELETE FROM films"]

Delete all films but musicals.

    (sql (delete :films)
         (where '(<> :kind "Musical")))
    ;=> ["DELETE FROM films WHERE (kind <> ?)" "Musical"]

Delete completed tasks, returning full details of the deleted rows.

    (sql (delete :tasks)
         (where '(= status "DONE"))
         (returning *))
    ;=> ["DELETE FROM tasks WHERE (status = ?) RETURNING *" "DONE"]

### Insert

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


### Select

Select all films.

    (sql (select *)
         (from :films))
    ;=> ["SELECT * FROM films"]

Select all Comedy films.

    (sql (select *)
         (from :films)
         (where '(= :kind "Comedy")))
    ;=> ["SELECT * FROM films WHERE (kind = ?)" "Comedy"]

### Update

Change the word Drama to Dramatic in the column kind of the table films.

    (sql (update :films {:kind "Dramatic"})
         (where '(= :kind "Drama")))
    ;=> ["UPDATE films SET kind = ? WHERE (kind = ?)" "Dramatic" "Drama"]

## License

Copyright © 2012 Roman Scherer

Distributed under the Eclipse Public License, the same as Clojure.
