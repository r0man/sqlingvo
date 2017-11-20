(ns sqlingvo.test.runner
  (:require [clojure.spec.test.alpha :as stest]
            [doo.runner :refer-macros [doo-tests]]
            [sqlingvo.compiler-test]
            [sqlingvo.copy-test]
            [sqlingvo.core-test]
            [sqlingvo.create-table-test]
            [sqlingvo.db-test]
            [sqlingvo.delete-test]
            [sqlingvo.drop-table-test]
            [sqlingvo.explain-test]
            [sqlingvo.expr-test]
            [sqlingvo.insert-test]
            [sqlingvo.materialized-view-test]
            [sqlingvo.select-test]
            [sqlingvo.truncate-test]
            [sqlingvo.update-test]
            [sqlingvo.url-test]
            [sqlingvo.util-test]
            [sqlingvo.values-test]
            [sqlingvo.with-test]))

(stest/instrument)

(doo-tests
 'sqlingvo.compiler-test
 'sqlingvo.copy-test
 'sqlingvo.core-test
 'sqlingvo.create-table-test
 'sqlingvo.db-test
 'sqlingvo.delete-test
 'sqlingvo.drop-table-test
 'sqlingvo.explain-test
 'sqlingvo.expr-test
 'sqlingvo.insert-test
 'sqlingvo.materialized-view-test
 'sqlingvo.select-test
 'sqlingvo.truncate-test
 'sqlingvo.update-test
 'sqlingvo.util-test
 'sqlingvo.url-test
 'sqlingvo.values-test
 'sqlingvo.with-test)
