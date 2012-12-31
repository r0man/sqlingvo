(defproject sqlingvo "0.2.0-SNAPSHOT"
  :description "A SQL DSL in Clojure."
  :url "http://github.com/r0man/sqlingvo"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/algo.monads "0.1.0"]
                 [org.clojure/java.jdbc "0.2.3"]
                 [inflections "0.7.5-SNAPSHOT"]]
  :profiles {:dev {:dependencies [[mysql/mysql-connector-java "5.1.21"]
                                  [postgresql "9.1-901.jdbc4"]
                                  [org.xerial/sqlite-jdbc "3.7.2"]]}})
