(defproject sqlingvo "0.6.6-SNAPSHOT"
  :description "A SQL DSL in Clojure."
  :url "http://github.com/r0man/sqlingvo"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :deploy-repositories [["releases" :clojars]]
  :dependencies [[org.clojure/clojure "1.6.0"]]
  :profiles {:provided {:plugins [[jonase/eastwood "0.1.4"]
                                  [lein-difftest "2.0.0"]]}}
  :aliases {"lint" ["do" ["eastwood"]]
            "ci" ["do" ["difftest"] ["lint"]]}
  :eastwood {:exclude-linters [:suspicious-expression]})
