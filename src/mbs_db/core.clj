(ns mbs-db.core
  (:use 
    [mbs-db.util :only (encrypt-name encrypt decrypt-name)]
    [org.clojars.smee.map :only (map-values)])
  (:require 
    [clojure.java.jdbc :as sql]
    [clojure.core.memoize :as cache])
  (:import java.sql.Timestamp))

(def h2-config {:classname   "org.h2.Driver"
                :subprotocol "h2"
                :user        "sa"
                :password     ""
                :subname      "~/solarlog"})
(def mysql-config {:classname   "com.mysql.jdbc.Driver"
                   :subprotocol "mysql"
                   :user        "read"
                   :password     "eumpw"
                   :subname      "//localhost:5029/solarlog"})
(def ^:dynamic *db* mysql-config)

(defprotocol ^{:added "1.2"} Time-Coercions
  "Coerce between various 'resource-namish' things."
  (^{:tag java.sql.Timestamp} as-sql-timestamp [x] "Coerce argument to java.sql.Timestamp.")
  (^{:tag java.lang.Long} as-unix-timestamp [x] "Coerce argument to time in milliseconds"))

(extend-protocol Time-Coercions
  nil
  (as-sql-timestamp [_] nil)
  (as-unix-timestamp [_] nil)
  
  Long
  (as-sql-timestamp [i] (Timestamp. i))
  (as-unix-timestamp [i] i)
  
  Timestamp
  (as-sql-timestamp [s] s)
  (as-unix-timestamp [s] (.getTime s))
  
  java.util.Date
  (as-sql-timestamp [d] (Timestamp. (.getTime d)))
  (as-unix-timestamp [d] (.getTime d))
  
  java.util.Calendar
  (as-sql-timestamp [c] (Timestamp. (.getTimeInMillis c)))
  (as-unix-timestamp [c] (.getTimeInMillis c))
  )

(defn adhoc [query & params]
  (sql/with-connection *db*
       (sql/with-query-results res (apply vector query params) (doall (for [r res] r)))))

(defmacro defquery 
  "Create an sql query that accepts a variable number of paramters and a body that handles the 
sequence of results by manipulating the var 'res'. Handles name obfuscation transparently."
  [name doc-string query & body]
  `(defn ~name ~doc-string[& params#]
     (sql/with-connection 
         *db* 
         (sql/with-query-results 
           ~'res (reduce conj [~query] params#) 
           ;; let user handle the results
           ~@body))))

(defmacro defquery-cached [num-to-cache name doc-string query & body]
  `(do
     (defquery ~name ~doc-string ~query ~@body)
     (alter-var-root #'~name cache/memo-lru ~num-to-cache)))

(defn- fix-time
  ([r] (fix-time r :time))
  ([r & keys]
  (reduce #(if-let [ts (get % %2)] 
             (assoc % %2 (as-unix-timestamp ts))
             (assoc % %2 0)) 
          r keys)))

(defn create-tables []
  (sql/with-connection *db*
      (sql/create-table :ts2 [:belongs "int"] [:value "int"] [:timestamp "timestamp"])
      (sql/create-table :tsnames [:name "varchar(255)"] [:belongs "int"])
      (sql/create-table :metadatadetails 
                        [:AnlagenKWP "int"]
                        [:AnzahlWR "int"]
                        [:BannerZeile1 "varchar(255)"]
                        [:BannerZeile2 "varchar(255)"]
                        [:BannerZeile3 "varchar(255)"]
                        [:HPAusricht "varchar(255)"]
                        [:HPBetreiber "varchar(255)"]
                        [:HPEmail "varchar(255)"]
                        [:HPInbetrieb "varchar(255)"]
                        [:HPLeistung "varchar(255)"]
                        [:HPModul "varchar(255)"]
                        [:HPPostleitzahl "char(5)"]
                        [:HPStandort "varchar(255)"]
                        [:HPTitel "varchar(255)"]
                        [:HPWR "varchar(255)"]
                        [:Serialnr "varchar(255)"]
                        [:SollYearKWP "int"]
                        [:Verguetung "int"]
                        [:id "varchar(255)"])))

(defquery count-all-values "Count all values" 
  "select count(*) as num from ts2"
  (:num (first res)))

(defquery-cached 20000 all-names-limit "Select all pv names (first part of the time series' names)."
  "select distinct SUBSTRING_INDEX(name,'.',1) as name from tsnames limit ?,?"
  (doall (map :name res)))

(defquery-cached 1 count-all-series "Count all available time series."
  "select count(*) as num from tsnames;"
  (:num (first res)))

(defquery count-all-series-of "Count all time series where the name is like the given parameter"
  "select count(name) as num from tsnames where name like ?"
  (:num (first res)))

(defquery all-series-names-of "Select all time series names that are like the given parameter"
  "select * from tsnames where name like ?  order by name"
  (doall (map :name res)))

(defquery count-all-values-of "Count all time series data points where the name is like the given parameter"
  "select count(*) as num from ts2 where belongs=(select belongs from tsnames where name=?)"
  (:num (first res)))

(defquery all-values-of "Select all time series data points of a given name."
  "select time, value from ts2 where belongs=(select belongs from tsnames where name=?)  order by time"
  (doall (map fix-time res)))

(defquery-cached 30 all-values-in-time-range "Select all time series data points of a given name that are between two times."
  "select time, value from ts2 where belongs=(select belongs from tsnames where name=?) and time >? and time <?  order by time"
  (doall (map fix-time res)))

(defquery-cached 10000 min-max-time-of "Select time of the oldest data point of a time series."
  "select min(time) as min, max(time) as max from ts2 where belongs=(select belongs from tsnames where name=?)"
  (fix-time (first res) :min :max))

(defquery summed-values-in-time-range "Select times and added values of all time series that match a given parameter and are between two times."
  "select time, sum(value) as value 
     from ts2 where belongs in (select belongs from tsnames where name like ?)  
      and time>? and time<? 
 group by time 
 order by time"
  (doall (doall (map (comp #(assoc % :value (.doubleValue (:value %))) fix-time) res))))

(defn get-efficiency [id wr-id start end]
  (let [[pdc-sum pac] (pvalues
                     (summed-values-in-time-range (format "%s.wr.%s.pdc.string.%%" id wr-id) (as-sql-timestamp start) (as-sql-timestamp end))
                     (all-values-in-time-range (format "%s.wr.%s.pac" id wr-id) (as-sql-timestamp start) (as-sql-timestamp end)))
        efficiency (map (fn [a d] (if (< 0 d)  (* 100 (/ a d)) 0)) 
                        (map :value pac) (map :value pdc-sum))]
    (map #(hash-map :time % :value %2) (map :time pac) efficiency)))
(alter-var-root #'get-efficiency cache/memo-lru 1000)

;; TODO more generic? allow all kinds of time intervals, days, weeks, months, years....
(defquery sum-per-day "Select sum of gains per day of a series in a time interval"
  "select max(value) as value, date(time) as time from ts2 where belongs=(select belongs from tsnames where name = ?)
   and time>? and time<?
   group by date(time) order by time"
  (doall (map fix-time res)))
(defquery sum-per-week "Select sum of gains per week of a series in a time interval"
  "select sum(value) as value, time 
   from   (select max(value) as value, date(time) as time 
           from   ts2 
           where  belongs= (select belongs from tsnames where name = ?)
                  and time>? and time<?
           group by date(time) 
           order by time) as daily
   group by week(time)"
  (doall (map fix-time res)))
(defquery sum-per-month "Select sum of gains per month of a series in a time interval"
  "select sum(value) as value, time from (select max(value) as value, date(time) as time from ts2 where belongs=(select belongs from tsnames where name = ?)
   and time>? and time<?
   group by date(time) order by time) as daily
group by month(time)"
  (doall (map fix-time res)))
(defquery sum-per-year "Select sum of gains per year of a series in a time interval"
  "select sum(value) as value, time from (select max(value) as value, date(time) as time from ts2 where belongs=(select belongs from tsnames where name = ?)
   and time>? and time<?
   group by date(time) order by time) as daily
group by year(time)"
  (doall (map fix-time res)))


(defn get-metadata "get map of metadata for multiple pv installations in one query." 
  [& names]
  (let [query (if names 
                (apply str "select * from metadatadetails where id=?" (repeat (dec (count names)) " or id=?"))
                "select * from metadatadetails")] 
    ;; run the query
    (sql/with-connection 
      *db* 
      (sql/with-query-results 
        res 
        (reduce conj [query] names)
        (let [;; metadata has wrong encoding, reinterpret every string as UTF8
              fixed-utf8 (for [r res] (map-values #(if (string? %) (String. (.getBytes %) "UTF8") %) r))
              ;; censor private data
              private-names [:bannerzeile1 :bannerzeile2 :bannerzeile3 :hpbetreiber :hpemail :hpstandort :hptitel]
              encrypted (for [r fixed-utf8] 
                          (reduce #(update-in % [%2] encrypt) r private-names))]
          (zipmap (map :id encrypted) encrypted))))))
(alter-var-root #'get-metadata cache/memo-lru 1000)

(comment
  
  (binding [*db* *db*]
    (let [df (java.text.SimpleDateFormat. "yyyy-MM-dd")
          start (.. df (parse "2011-01-01") getTime)
          end (.. df (parse "2011-12-31") getTime)] 
      #_(num-series db)
      #_(all-names 0 10)
      #_(time (def x (all-values-of "1468.wr.0.pdc.string.0")))
      #_(time (def x (all-values-in-time-range "1468.wr.0.pdc.string.0" (java.sql.Timestamp. start) (java.sql.Timestamp. end))))
      (count-all-series-of "1555"))
    )
  
  )

