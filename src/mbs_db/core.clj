(ns mbs-db.core
  (:use 
    [org.clojars.smee 
     [map :only (map-values)]
     [time :only (as-unix-timestamp as-sql-timestamp)]])
  (:require 
    [clojure.java.jdbc :as sql]
    [clojure.core.memoize :as cache])
  (:import com.mchange.v2.c3p0.ComboPooledDataSource))

;;;;;;;;;;;;;;;;;;;; connections ;;;;;;;;;;;;;;;;;;;;;
(def mysql-config-default {:classname   "com.mysql.jdbc.Driver"
                           :subprotocol "mysql"
                           :user        "root"
                           :password     ""
                           :subname      "//localhost:5029/siemens"})

(def mysql-config-siemens (assoc mysql-config-default 
                                 :subname "//localhost:5029/siemens"
                                 :password (get (System/getenv) "eumdb01_password") ))

(def mysql-config-psm (assoc mysql-config-default :subname "//localhost:5029/psm"))

(defn- connection-pool
  [spec]
  (let [cpds (doto (ComboPooledDataSource.)
               (.setDriverClass (:classname spec)) 
               (.setJdbcUrl (str "jdbc:" (:subprotocol spec) ":" (:subname spec)))
               (.setUser (:user spec))
               (.setPassword (:password spec))
               ;; expire excess connections after 30 minutes of inactivity:
               (.setMaxIdleTimeExcessConnections (* 1 60))
               ;; expire connections after 3 hours of inactivity:
               (.setMaxIdleTime (* 3 60 60)))] 
    {:datasource cpds}))

(def ^{:doc "map of current database connection settings"} current-db-settings (atom {}))
(def ^{:private true :doc "connection pool to be used with `with-connection`"} conn (delay (connection-pool mysql-config-psm)))

(defn use-db-settings [settings]
  (let [{:keys [user password subname subprotocol]} (merge mysql-config-psm settings)]
    (println "[db] using new database settings: " settings)
    (reset! current-db-settings settings)
    (doto (:datasource @conn)
      (.setJdbcUrl (str "jdbc:" subprotocol ":" subname))
      (.setUser user)
      (.setPassword password))))

(defn connection-status []
  (let [c (:datasource @conn)] 
    {:num-connections      (.getNumConnectionsDefaultUser c)
     :num-busy-connections (.getNumBusyConnectionsDefaultUser c)
     :num-idle-connections (.getNumIdleConnectionsDefaultUser c)}))

;;;;;;;;;;;;;;;;;;;; tables definitions ;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn create-tables-siemens []
  (sql/with-connection @conn
      (sql/create-table :series_data
                        [:plant "varchar(255)" "comment 'lookup'"] ; use infobright's lookup feature for better compression
                        [:name "varchar(255)" "comment 'lookup'"] ; use infobright's lookup feature for better compression
                        [:value "double"] 
                        [:quality "int"] 
                        [:flags "int"]
                        [:timestamp "timestamp" "default 0"] 
                        [:year "smallint"]
                        [:month "tinyint"]
                        [:day_of_year "smallint"]
                        [:day_of_month "tinyint"])
      (sql/create-table :plant
                        [:name"varchar(127)"]
                        [:street "varchar(127)"]
                        [:street_number "varchar(127)"]
                        [:zipcode "varchar(127)"]
                        [:city "varchar(127)"]
                        [:country"varchar(127)"])
      (sql/create-table :customer
                        [:name"varchar(127)"]
                        [:street "varchar(127)"]
                        [:street_number "varchar(127)"]
                        [:zipcode "varchar(127)"]
                        [:city "varchar(127)"]
                        [:country"varchar(127)"]) 
      (sql/create-table :series 
                        [:plant "varchar(127)"] ;name of the power plant
                        [:name "varchar(127)"] ;name of the series 
                        [:identification "varchar(127)"]; ???
                        [:unit "varchar(127)"] ;physical SI unit
                        [:scale "varchar(127)"] ;???
                        [:component "varchar(127)"] ;name of the origin component
                        [:type "varchar(127)"] ;type of the series
                        [:resolution "int"]) ;average time between two measures in milliseconds
      (sql/create-table :series_summary
                        [:plant "varchar(127)" "comment 'lookup'"] ;name of the power plant
                        [:name "varchar(127)" "comment 'lookup'"] ;name of the series 
                        [:date "date"]
                        [:num "integer"]) 
      ))
#_(defn create-dates-helper-table []
  (sql/with-connection @conn
      (sql/create-table :dates [:date "date"])))
;;;;;;;;;;;;;;;;;; infobright import functions ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn import-into-infobright* [table & data-csv-files]
  (sql/with-connection 
    @conn
    (apply sql/do-commands 
      "set @bh_dataformat = 'txt_variable'"
      (for [file data-csv-files] 
        (format "load data infile '%s' into table %s"  (.replaceAll (str file) "\\\\" "/") table)))))

(defn import-into-infobright [& data-csv-files]
  (apply import-into-infobright* "series_data" data-csv-files))


;;;;;;;;;;;;;;;;; query helpers ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn adhoc [query & params]
  (sql/with-connection @conn
       (sql/with-query-results res (apply vector query params) (doall (for [r res] r)))))

(defmacro defquery 
  "Create an sql query that accepts a variable number of paramters and a body that handles the 
sequence of results by manipulating the var 'res'. Handles name obfuscation transparently."
  [name doc-string query & body]
  `(defn ~name ~doc-string [& params#]
     (sql/with-connection 
         @conn 
         (sql/with-query-results 
           ~'res (reduce conj [~query] params#) 
           ;; let user handle the results
           ~@body))))

(defmacro defquery-cached [name num-to-cache doc-string query & body]
  `(do
     (defquery ~name ~doc-string ~query ~@body)
     (alter-var-root #'~name cache/memo-lru ~num-to-cache)))

(defn- fix-time
  ([r] (fix-time r :timestamp))
  ([r & keys]
  (reduce #(if-let [ts (get % %2)] 
             (assoc % %2 (as-unix-timestamp ts))
             (assoc % %2 0)) 
          r keys)))

;;;;;;;;; series meta data ;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defquery count-all-series-of-plant "Count all time series where the name of the plant is the given parameter"
  "select count(*) as num from series where plant= ?;"
  (apply + (map :num res)))

(defquery all-series-names-of-plant "Select all time series names with given plant name. Returns a map of identifier (for example IEC61850 name)
to display name."
  "select name, identification from series where plant=?;"
  (reduce merge (map (comp (partial apply hash-map) (juxt :identification :name)) res)))

;;;;;;;;; time series values ;;;;;;;;;;;;;;;;;;;;;;;;;;;

;(defquery all-values-of "Select all time series data points of a given name."
;  "select time, value from series_data where belongs=(select belongs from tsnames where name=?)  order by time"
;  (doall (map fix-time res)))

(defquery-cached all-values-in-time-range 1 "Select all time series data points of a given plant and series id that are between two times."
  "select timestamp, value from series_data where plant=? and name=? and timestamp >? and timestamp <?  order by timestamp"
  (doall (map fix-time res)))

(defquery-cached all-all-values-in-time-range 1 "Select all time series data points of all series of a given plant that are between two times."
  "select name,timestamp, value from series_data where plant=? and timestamp >? and timestamp <?  order by timestamp"
  (doall (map fix-time res)))

(defquery-cached min-max-time-of 10000 "Select time of the oldest/newest data point of a time series."
  "select min(timestamp) as min, max(timestamp) as max from series_data where plant=? and name=?"
  (fix-time (first res) :min :max))


(def ^:private daily 
           "select sum(value) as value, maxima.t as time from
              (select date(timestamp) as t, max(value) as value, name from series_data 
               where name like 'INVU%/DAY_MMTR0%'
                     and plant=?
                     and timestamp>? and timestamp<? 
               group by t, name) as maxima
            group by maxima.t order by t")
;todo
(defquery-cached sum-per-day 10 "Select sum of gains per day of a series in a time interval"
  daily
  (doall (map fix-time res)))
(defquery-cached sum-per-week 10 "Select sum of gains per week of a series in a time interval"
  (str "select sum(value) as value, time from (" daily ") as daily group by week(time)")
  (doall (map fix-time res)))
(defquery-cached sum-per-month 10 "Select sum of gains per month of a series in a time interval"
  (str "select sum(value) as value, time from (" daily ") as daily group by month(time)")
  (doall (map fix-time res)))
(defquery-cached sum-per-year 10 "Select sum of gains per year of a series in a time interval"
  (str "select sum(value) as value, time from (" daily ") as daily group by year(time)")
  (doall (map fix-time res)))

(defquery-cached available-data 1 "select all dates for which there is any data."
  "select date,sum(num) as num from series_summary where plant=? group by date order by date"
  (doall (map (fn [{d :date :as m}] (assoc m :date (as-unix-timestamp d))) res)))

(defn get-metadata "get map of metadata for multiple pv installations in one query." 
  [& names]
  (let [query (if names 
                (apply str "select * from plant where name=?" (repeat (dec (count names)) " or name=?"))
                "select * from plant")] 
    ; TODO need real metadata, number of inverters etc.
    (sql/with-connection @conn 
       (sql/with-query-results res (reduce conj [query] names)
            (zipmap (map :name res) (map #(hash-map :address % :anzahlwr 2 :anlagenkwp 1000000) res))))))
(alter-var-root #'get-metadata cache/memo-lru 100)

;;;;;;;; internal statistics ;;;;;;;;;;;;;;;;;;;;
(defn data-base-statistics []
  (adhoc "SELECT table_schema, sum( data_length + index_length ) / 1024 / 1024 'Data Base Size in MB',TABLE_COMMENT FROM information_schema.TABLES WHERE ENGINE = 'BRIGHTHOUSE' GROUP BY table_schema"))
(defn table-statistics []
  (adhoc "SHOW TABLE STATUS WHERE ENGINE='BRIGHTHOUSE'"))


(defn rolled-up-values-in-time-range 
  "Find min, max, and average of values aggregated into `num` time slots."
  [plant name start end num]
  (let [s (as-unix-timestamp start) 
        e (as-unix-timestamp end)
        interval-in-s (int (/ (- e s) num 1000)) ;Mysql handles unix time stamps as seconds, not milliseconds since 1970
        query "select avg(value) as value, min(value) as min, max(value) as max, count(value) as count, timestamp
               from series_data 
               where plant=? and name=? and timestamp between ? and ? group by unix_timestamp(timestamp) div ?"]
    (sql/with-connection @conn
       (sql/with-query-results res [query plant name (as-sql-timestamp start) (as-sql-timestamp end) interval-in-s]
            (doall (map fix-time res))))))