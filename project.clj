(defproject sqlingvo "0.1.0-SNAPSHOT"
  :description "A SQL compiler and DSL in Clojure."
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/java.jdbc "0.2.3"]]
  :profiles {:dev {:dependencies [[org.xerial/sqlite-jdbc "3.7.2"]]}})
