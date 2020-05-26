(defproject techascent/tech.ml.dataset.sql "0.3"
  :description "SQL bindings for tech.ml.dataset"
  :url "https://github.com/techascent/tech.ml.dataset.sql"
  :license {:name "EPL-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :plugins [[lein-tools-deps "0.4.5"]]
  :middleware [lein-tools-deps.plugin/resolve-dependencies-with-deps-edn]
  :lein-tools-deps/config {:config-files [:install :user :project]}
  :profiles {:dev {:lein-tools-deps/config {:resolve-aliases [:test]}
                   ;;Word to the wise; if you aren't developing on the dataset code
                   ;;itself then precompile it.
                   :aot [tech.ml.dataset]}})
