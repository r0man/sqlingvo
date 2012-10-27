# SQLingvo [![Build Status](https://travis-ci.org/r0man/sqlingvo.png)](https://travis-ci.org/r0man/sqlingvo)

A SQL compiler and DSL in Clojure.

## Installation

Via Clojars: https://clojars.org/sqlingvo

## Usage

FIXME

## Examples

### Update

Change the word Drama to Dramatic in the column kind of the table films.

    (-> (update :films {:kind "Dramatic"})
        (where '(= :kind "Drama"))
        (sql))
    ;=> ["UPDATE films SET kind = ? WHERE (kind = ?)" "Dramatic" "Drama"]

## License

Copyright © 2012 Roman Scherer

Distributed under the Eclipse Public License, the same as Clojure.
