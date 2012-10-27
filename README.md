# SQLingvo [![Build Status](https://travis-ci.org/r0man/sqlingvo.png)](https://travis-ci.org/r0man/sqlingvo)

A SQL DSL in Clojure.

## Installation

Via Clojars: https://clojars.org/sqlingvo

## Usage

    (use 'sqlingvo.core)

## Examples

### Select

Select all films.

    (-> (select *)
        (from :films)
        (sql))
    ;=> ["SELECT * FROM films"]

Select all Comedy films.

    (-> (select *)
        (from :films)
        (where '(= :kind "Comedy"))
        (sql))
    ;=> ["SELECT * FROM films WHERE (kind = ?)" "Comedy"]

### Insert

Insert a single row into table films.

    (-> (insert :films {:code "T_601" :title "Yojimbo" :did 106 :date-prod "1961-06-16" :kind "Drama"})
        (sql))
    ;=> ["INSERT INTO films (did, date-prod, kind, title, code) VALUES (?, ?, ?, ?, ?)"
    =>   106 "1961-06-16" "Drama" "Yojimbo" "T_601"]

Insert multiple rows into the table films using the multirow VALUES syntax.

    (-> (insert :films [{:code "B6717" :title "Tampopo" :did 110 :date-prod "1985-02-10" :kind "Comedy"},
                        {:code "HG120" :title "The Dinner Game" :did 140 :date-prod "1985-02-10":kind "Comedy"}])
        (sql))
    ;=> ["INSERT INTO films (did, date-prod, kind, title, code) VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)"
    ;=>  110 "1985-02-10" "Comedy" "Tampopo" "B6717" 140 "1985-02-10" "Comedy" "The Dinner Game" "HG120"]

Insert a row consisting entirely of default values

    (-> (insert :films)
        (default-values)
        (sql)
    ;=> ["INSERT INTO films DEFAULT VALUES"]

### Update

Change the word Drama to Dramatic in the column kind of the table films.

    (-> (update :films {:kind "Dramatic"})
        (where '(= :kind "Drama"))
        (sql))
    ;=> ["UPDATE films SET kind = ? WHERE (kind = ?)" "Dramatic" "Drama"]

## License

Copyright © 2012 Roman Scherer

Distributed under the Eclipse Public License, the same as Clojure.
