(ns mbs-db.test.db-core
  (:use clojure.test
        mbs-db.core))

(deftest skip-missing
  (are [names x y] (= x (mapv :name (#'mbs-db.core/skip-missing names (map #(hash-map :name %) y))))
       [:a :b :c] [:a :b :c :a :b :c] [:b :c :a :b :c :a :b :a :b :c :a]))

