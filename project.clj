(defproject relation-utils "0.1.0"
  :description "Relational utilities for Clojure."
  :license {:name "MIT"}
  :url "https://github.com/comnik/clj-relations"
  :plugins [[lein-tools-deps "0.4.5"]]
  :middleware [lein-tools-deps.plugin/resolve-dependencies-with-deps-edn]
  :lein-tools-deps/config {:config-files ["deps.edn" :install :user :project]})
