(defproject techascent/tech.ml.dataset.sql "5.00-beta-13-SNAPSHOT"
  :description "SQL bindings for the 5.X branch of tech.ml.dataset"
  :url "https://github.com/techascent/tech.ml.dataset.sql"
  :license {:name "EPL-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.2-alpha1"]]
  :profiles
  {:dev
   {:dependencies [[org.postgresql/postgresql  "42.2.12"]
                   [seancorfield/next.jdbc     "1.0.424"]
                   [techascent/tech.ml.dataset "5.00-beta-12"]]}
   :codox
   {:dependencies [[codox-theme-rdash "0.1.2"]]
    :plugins [[lein-codox "0.10.7"]]
    :codox {:project {:name "tech.ml.dataset.sql"}
            :metadata {:doc/format :markdown}
            :themes [:rdash]
            :source-paths ["src"]
            :output-path "docs"
            :doc-paths ["topics"]
            :source-uri "https://github.com/techascent/tech.ml.dataset.sql/blob/master/{filepath}#L{line}"
            :namespaces [tech.v3.dataset.sql]}}}
  :aliases {"codox" ["with-profile" "codox,dev" "codox"]})
