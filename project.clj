(defproject mbs-db "1.0.0-SNAPSHOT"
  :description "FIXME: write description"
  :dev-dependencies [[com.h2database/h2 "1.3.162"]]
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/java.jdbc "0.1.0"]
                 [org.clojure/data.json "0.1.2"]
                 [clj-cache "0.0.4"]
                 ;; TODO use either or, or make configurable by consumer application
                 [mysql/mysql-connector-java "5.1.17"]])
