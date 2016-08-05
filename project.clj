(defproject sqlingvo "0.8.17"
  :description "A Clojure DSL to create SQL statements"
  :url "http://github.com/r0man/sqlingvo"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :min-lein-version "2.5.2"
  :deploy-repositories [["releases" :clojars]]
  :dependencies [[org.clojure/clojure "1.8.0"]]
  :plugins [[jonase/eastwood "0.2.3"]
            [lein-cljsbuild "1.1.3"]
            [lein-difftest "2.0.0"]
            [lein-doo "0.1.7"]]
  :profiles
  {:dev
   {:dependencies [[org.clojure/test.check "0.9.0"]]}
   :provided
   {:dependencies [[org.clojure/clojurescript "1.9.93"]]}
   :repl
   {:dependencies [[com.cemerick/piggieback "0.2.1"]
                   [reloaded.repl "0.2.2"]]
    :plugins [[figwheel-sidecar "0.5.4-7"]]
    :init-ns user
    :nrepl-middleware [cemerick.piggieback/wrap-cljs-repl]}}
  :aliases
  {"ci" ["do"
         ["clean"]
         ["difftest"]
         ["doo" "node" "node" "once"]
         ["doo" "phantom" "none" "once"]
         ["doo" "phantom" "advanced" "once"]
         ["lint"]]
   "lint" ["do"  ["eastwood"]]}
  :cljsbuild
  {:builds
   [{:id "none"
     :compiler
     {:main 'sqlingvo.test.runner
      :optimizations :none
      :output-dir "target/none"
      :output-to "target/none.js"
      :parallel-build true
      :pretty-print true
      :verbose false}
     :source-paths ["src" "test"]}
    {:id "node"
     :compiler
     {:main 'sqlingvo.test.runner
      :optimizations :none
      :output-dir "target/node"
      :output-to "target/node.js"
      :parallel-build true
      :pretty-print true
      :target :nodejs
      :verbose false}
     :source-paths ["src" "test"]}
    {:id "advanced"
     :compiler
     {:main 'sqlingvo.test.runner
      :optimizations :advanced
      :output-dir "target/advanced"
      :output-to "target/advanced.js"
      :parallel-build true
      :pretty-print true
      :verbose false}
     :source-paths ["src" "test"]}]})
