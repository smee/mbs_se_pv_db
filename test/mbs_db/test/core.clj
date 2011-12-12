(ns mbs-db.test.core
  (:require [mbs-db.core :as db])
  (:use [clojure.test]))

(deftest replace-me ;; FIXME: write
  (is false "No tests have been written."))

(comment
  
  (binding [mbs-db.core/*db* {:classname   "org.h2.Driver"
              :subprotocol "h2"
              :user        "sa"
              :password     ""
              :subname      "~/solarlog"}]
    (db/count-all-values))
  )

;;create table ts2 (time timestamp not null, value int, belongs int);
;;create table tsnames(name varchar(255), belongs int);
;;create table metadatajson(name varchar(255), json varchar(30000));