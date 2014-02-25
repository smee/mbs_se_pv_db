(ns mbs-db.core
  (:use
    [clojure.string :only [join]] 
    [org.clojars.smee 
     [time :only (as-unix-timestamp as-sql-timestamp)]])
  (:require 
    [clojure.java.jdbc :as sql])
  (:import com.mchange.v2.c3p0.ComboPooledDataSource))

;;;;;;;;;;;;;;;;;;;; connections ;;;;;;;;;;;;;;;;;;;;;
(def mysql-config-default {:classname   "com.mysql.jdbc.Driver"
                           :subprotocol "mysql"
                           :user        (get (System/getenv) "DBUSER" "root")
                           :password     (get (System/getenv) "DBPW" "")
                           :subname      "//localhost:5029/psm2"
                           :connection-name "default"
                           :parameters {}})

(def mysql-config-psm (assoc mysql-config-default 
                             :connection-name "psm-db" 
                             :parameters {"useCursorFetch" true "defaultFetchSize" 100000}))

(defn create-db-connection-pool
  [{:keys [subname classname subprotocol user password connection-name parameters]}]
  (let [params (map (fn [[k v]] (str k "=" v)) parameters)
        params (if (empty? parameters) "" (apply str "?" (join "&" params)))
        url (str "jdbc:" subprotocol ":" subname params)
        cpds (doto (ComboPooledDataSource. (or connection-name (str (java.util.UUID/randomUUID))))
               #_(.setProperties (doto (java.util.Properties.)
                                 (.setProperty "characterEncoding" "latin1")
                                 (.setProperty "useUnicode", "true")
                                 (.setProperty "characterSetResults", "ISO8859_1")))
               (.setDriverClass classname) 
               (.setJdbcUrl url)
               (.setUser user)
               (.setPassword password)
               ;; expire excess connections after 30 minutes of inactivity:
               (.setMaxIdleTimeExcessConnections (* 1 60))
               (.setMaxConnectionAge (* 1 60)) 
               (.setIdleConnectionTestPeriod (* 1 30)) 
               ;; expire connections after 3 hours of inactivity:
               (.setMaxIdleTime (* 3 60 60)))] 
    {:datasource cpds}))

(defonce ^{:doc "map of current database connection settings"} current-db-settings (atom nil))
(defonce ^{:private true :dynamic true :doc "connection pool to be used with `with-connection`"} conn (atom {:datasource nil}))

(defn- get-connection []
  @conn)

(defn use-db-settings [settings]
  (let [settings (merge mysql-config-psm settings)]
    (println "[db] using new database settings: " settings)
    (reset! current-db-settings settings)
    (reset! conn (create-db-connection-pool settings))))

(defmacro with-db [connection-name & body]
  `(binding [conn (atom {:datasource (com.mchange.v2.c3p0.C3P0Registry/pooledDataSourceByName ~connection-name)})]
     ~@body))

(defmacro q [& body]
  `(sql/query (get-connection) ~@body))

(defn connection-names []
  (map (memfn getDataSourceName) (com.mchange.v2.c3p0.C3P0Registry/getPooledDataSources)))

(defn connection-status []
  (let [c (:datasource (get-connection))] 
    {:num-connections      (.getNumConnectionsDefaultUser c)
     :num-busy-connections (.getNumBusyConnectionsDefaultUser c)
     :num-idle-connections (.getNumIdleConnectionsDefaultUser c)}))

;;;;;;;;;;;;;;;;;;;; tables definitions ;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- create-time-series-tables []
  (sql/create-table-ddl :series_data
                        [:plant "varchar(255)" "comment 'lookup'"] ; use infobright's lookup feature for better compression
                        [:name "varchar(255)" "comment 'lookup'"] ; use infobright's lookup feature for better compression
                        [:value "double"] 
                        [:timestamp "timestamp" "default 0"] 
                        [:unixtimestamp "bigint"] 
                        [:year "smallint"]
                        [:month "tinyint"]
                        [:day_of_year "smallint"]
                        [:day_of_month "tinyint"]
                        [:hour_of_day "tinyint"])
      (sql/create-table-ddl :plant
                        [:name"varchar(127)"]
                        [:street "varchar(127)"]
                        [:street_number "varchar(127)"]
                        [:zipcode "varchar(127)"]
                        [:city "varchar(127)"]
                        [:country"varchar(127)"]
                        [:gain_series_name_template "varchar(127)"]
                        :table-spec "engine = 'MyIsam'")
      (sql/create-table-ddl :customer
                        [:name"varchar(127)"]
                        [:street "varchar(127)"]
                        [:street_number "varchar(127)"]
                        [:zipcode "varchar(127)"]
                        [:city "varchar(127)"]
                        [:country"varchar(127)"]
                        :table-spec "engine = 'MyIsam'") 
      (sql/create-table-ddl :series 
                        [:plant "varchar(127)"] ;name of the power plant
                        [:name "varchar(127)"] ;name of the series 
                        [:identification "varchar(127)"]; ???
                        [:unit "varchar(127)"] ;physical SI unit
                        [:scale "varchar(127)"] ;???
                        [:component "varchar(127)"] ;name of the origin component
                        [:type "varchar(127)"] ;type of the series
                        [:resolution "int"] ;average time between two measures in milliseconds
                        :table-spec "engine = 'MyIsam'") 
      (sql/create-table-ddl :series_summary
                        [:plant "varchar(127)" "comment 'lookup'"] ;name of the power plant
                        [:name "varchar(127)" "comment 'lookup'"] ;name of the series 
                        [:date "date"]
                        [:num "integer"]))

(defn create-tables []
  (sql/db-do-commands (get-connection)
      (create-time-series-tables)
      ; mutable tables
      (sql/create-table-ddl :maintainance
                        [:start "datetime"] 
                        [:end "datetime"] 
                        [:plant "varchar(127)"]
                        [:reason "varchar(1000)"]
                        :table-spec "engine = 'MyIsam'")
      (sql/create-table-ddl :structure
                        [:plant "varchar(127)"]
                        [:clj "text"]
                        :table-spec "engine = 'MyIsam'")
      ; scenarios of time series that should be compared daily
      ; via relative entropy
      (sql/create-table-ddl :analysisscenario 
                        [:plant "varchar(127)"]
                        [:id :int "PRIMARY KEY NOT NULL AUTO_INCREMENT"]
                        [:name "varchar(500)"]
                        [:settings :text]
                        :table-spec "engine = 'MyIsam'")
      (sql/create-table-ddl :analysis
                        [:plant "varchar(255)"]
                        [:scenario :int]
                        [:date :date]                        
                        [:result :text]
                        :table-spec "engine = 'MyIsam'")))

;;;;;;;;;;;;;;;;;; infobright import functions ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn import-into-infobright* [table & data-csv-files]
  (apply sql/db-do-commands (get-connection) 
         "set @bh_dataformat = 'txt_variable'"
         (for [file data-csv-files] 
           (format "load data infile '%s' into table %s fields terminated by ';'"  (.replaceAll (str file) "\\\\" "/") table))))

(defn import-into-infobright [& data-csv-files]
  (apply import-into-infobright* "series_data" data-csv-files))


;;;;;;;;;;;;;;;;; query helpers ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn adhoc "Adhoc query, for development" [query & params]
  (q (apply vector query params)))

(defn do! "Run adhoc commands, like drop, create table, etc." [& cmds]
  (apply sql/db-do-commands (get-connection) cmds)) 

(defn- fix-time
  ([r] (fix-time r :timestamp))
  ([r & keys]
  (reduce #(if-let [ts (get % %2)] 
             (assoc % %2 (as-unix-timestamp ts))
             (assoc % %2 0)) 
          r keys)))

(defn- fix-string-encoding 
  "The database uses latin1 encoding, but there are values that were originally in utf8.
Within the database there are now two byte characters for umlauts etc. This functions
fixes those strings after being fetched via jdbc."
  [^String s]
  (if (string? s)
    (String. (.getBytes s "latin1"))
    s))
;;;;;;;;; series meta data ;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn count-all-series-of-plant 
  "Count all time series where the name of the plant is the given parameter"
  [plant]
  (q ["select count(*) as num from series where plant= ?" plant]
     :result-set-fn #(apply + (map :num %))))

(defn all-series-names-of-plant 
  "Select all time series names with given plant name. Returns a map of identifier (for example IEC61850 name)
to display name."
  [plant]
  (q ["select * from series where plant=?;" plant]
             :row-fn (fn [m] {(:identification m) m})
             :result-set-fn (partial reduce merge)))

;;;;;;;;; time series values ;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn- skip-missing 
  "There may be cases where a query assumes, that there are values for several time series for the same
instance in time. If that should be not the case, we get a sequence of time series entries in `vs` where
 `(not= (set (map :name vs)) (set names)))`. This function filters the sequence `vs` and drops all subsequences
of `names`.
For example see the tests."
  [names vs] 
  (let [n (count names)] 
    (lazy-seq
      (when (seq vs) 
        (if (= names (map :name (take n vs)))
          (concat (take n vs) (skip-missing names (drop n vs)))
          (skip-missing names (next vs)))))))

(defn all-values-in-time-range 
  "Fetches timestamps and values for a number of time series. If `num` is given, rolls up values into `num` different bins (returns averages).
`f` is a function that gets fed the lazy sequence of responses from the database. Use it
for long sequences that should not be realized fully into memory."
  ([plant names start end num] (all-values-in-time-range plant names start end num doall))
  ([plant names start end num f] 
  (let [sort-order (into {} (map vector names (range (count names))))
        n (count names)
        num (or num java.lang.Long/MAX_VALUE)
        interval-in-s (max 1 (long (/ (- (as-unix-timestamp end) (as-unix-timestamp start)) num)))
        names-q (str "(" (join " or " (repeat n "name=?")) ")")
        query (-> (str "select timestamp, value, name from series_data 
                     where plant=? and 
                           timestamp  between ? and ? and " 
                   names-q 
                   (if num " group by (unixtimestamp div ?),name" "")
                   " order by timestamp, name"))
        query (apply vector query plant (as-sql-timestamp start) (as-sql-timestamp end) names)
        query (if num (conj query interval-in-s) query)] 
    (q query 
       :row-fn fix-time
       :result-set-fn #(->> %
                         (partition-by :timestamp)
                         (map (partial sort-by (comp sort-order :name)))
                         (apply concat)
                         (skip-missing names)
                         (partition n)
                         f)))))

(defn rolled-up-values-in-time-range 
  "Find min, max, and average of values aggregated into `num` time slots."
  ([plant name start end] (rolled-up-values-in-time-range plant name start end java.lang.Long/MAX_VALUE))
  ([plant name start end num]
    (let [s (as-unix-timestamp start) 
          e (as-unix-timestamp end)
          num (max 1 num) 
          interval-in-s (max 1 (long (/ (- e s) num))) ;Mysql handles unix time stamps as seconds, not milliseconds since 1970
          query "select avg(value) as value, min(value) as min, max(value) as max, count(value) as count, timestamp
               from series_data 
               where plant=? and 
                     name=? and 
                     timestamp between ? and ? 
               group by (unixtimestamp div ?)"] 
      (q [query plant name (as-sql-timestamp start) (as-sql-timestamp end) interval-in-s]
         :row-fn fix-time))))


(defn min-max-time-of 
  "Select time of the oldest/newest data point of a time series."
  [plant series-name] 
  (q ["select min(timestamp) as min, max(timestamp) as max from series_data where plant=? and name=?" plant series-name]
     :row-fn #(fix-time % :min :max)
     :result-set-fn first))

;;;;;;;;;;; sums of park gains ;;;;;;;;;
(def ^:private daily-template
  "select sum(daily.mav-daily.miv) as value, daily.t as time from
      (select name, timestamp as t,max(value) as mav, min(value) as miv from series_data
        where plant=? and timestamp>? and timestamp<?
              and name like ?
      group by name, year,day_of_year) as daily
      group by daily.t order by t") 

(defn internal-find-gain-template ""
  [plant]
  (q ["select gain_series_name_template as t from plant where name=?" plant]
     :result-set-fn #(-> % first :t)))

(defn sum-per "Select sum of gains per day of a series in a time interval"
  [type plant start-time end-time] 
  (let [template (internal-find-gain-template plant)
        query (if (not= "day" type) 
                (str "select sum(value) as value, time from (" daily-template ") as daily group by year(time), " type "(time)")
                daily-template)]
    (q [query plant start-time end-time template]
       :row-fn #(fix-time % :time))))

(defn sum-per-day [plant start-time end-time]
  (sum-per "day" plant start-time end-time))
(defn sum-per-week [plant start-time end-time]
  (sum-per "week" plant start-time end-time))
(defn sum-per-month [plant start-time end-time]
  (sum-per "month" plant start-time end-time))
(defn sum-per-year [plant start-time end-time]
  (sum-per "year" plant start-time end-time))

(defn available-data 
  "select all dates for which there is any data."
  [plant] 
  (q ["select date,sum(num) as num from series_summary where plant=? group by date order by date" plant]
     :row-fn #(fix-time % :date)))

(defn get-metadata "get map of metadata for multiple pv installations in one query." 
  [& names]
  (let [query (if names 
                (apply str "select * from plant where name=?" (repeat (dec (count names)) " or name=?"))
                "select * from plant")] 
    ; TODO need real metadata, number of inverters etc.
    (q (reduce conj [query] names)
       :row-fn #(vector (:name %) {:address % :anzahlwr 2 :anlagenkwp 2150000}) 
       :result-set-fn (partial into {}))))

;;;;;;;; internal statistics ;;;;;;;;;;;;;;;;;;;;
(defn data-base-statistics []
  (adhoc "SELECT table_schema, sum( data_length + index_length ) / 1024 / 1024 'Data Base Size in MB',TABLE_COMMENT FROM information_schema.TABLES WHERE ENGINE = 'BRIGHTHOUSE' GROUP BY table_schema"))
(defn table-statistics []
  (adhoc "SHOW TABLE STATUS WHERE ENGINE='BRIGHTHOUSE'"))


;;;;;;;;;;;;;; 
(defn db-max-current-per-insolation 
  "Currently used only for changepoint charts."
  [plant current-name insolation-name start end]
    (let [sub-q "select name, timestamp, hour_of_day as hour, avg(value) as value, stddev(value) as s, count(value) as count from series_data 
                 where plant=? and name=? and timestamp between ? and ?
                   and hour_of_day>=9 and hour_of_day<=16 
                 group by year, day_of_year, hour 
                 order by year, day_of_year, hour"
          query (str "select v.name as name, v.timestamp as timestamp, v.value/i.value as value, v.hour as hour, v.s as std_val, i.s as std_ins from (" sub-q ") as v join (" sub-q") as i on i.timestamp=v.timestamp where i.count>58")
          start (as-sql-timestamp start)
          end (as-sql-timestamp end)]
      (q [query plant current-name start end plant insolation-name start end]
         :row-fn fix-time)))


(defn maintainance-intervals 
  "Find all known time intervals where any maintainance works was done on a plant"
  [plant] 
  (q ["select * from maintainance where plant=?" plant]
     :row-fn #(fix-time % :start :end)))

(defn structure-of 
  "Get the component structure of a plant"
  [plant] 
  (q ["select clj from structure where plant=?" plant]
     :result-set-fn #(if (:clj (first %)) 
                       (read-string (:clj (first %)))
                       {})))

;;;;;;;;;;;;;;; relative entropy comparison queries
;(sql/create-table-ddl :analysisscenario 
;                        [:plant "varchar(127)"]
;                        [:id :int "PRIMARY KEY NOT NULL AUTO_INCREMENT"]
;                        [:name "varchar(500)"]
;                        [:settings :text]
;                        :table-spec "engine = 'MyIsam'")
;      (sql/create-table-ddl :analysis
;                        [:plant "varchar(255)"]
;                        [:scenario :int]
;                        [:date :date]                        
;                        [:result :text]
;                        :table-spec "engine = 'MyIsam'")
(defn get-scenarios ""
  [plant] 
  (q ["select * from analysisscenario where plant=?" plant]
     :row-fn #(update-in % [:settings] read-string)))

(defn get-scenario-id "" [plant settings] 
  (binding [*print-length* nil
            *print-level* nil]
    (let [settings (pr-str settings)]      
  (q ["select id from analysisscenario where plant=? and settings=?" plant settings]
    :result-set-fn (comp :id first)))))

(defn get-scenario ""
  [id]
  (q ["select * from analysisscenario where id=?" id]
     :row-fn #(-> % fix-time (update-in [:settings] read-string))
     :result-set-fn first))


(defn insert-scenario "" [plant name settings]
  (binding [*print-length* nil
            *print-level* nil]
    (sql/insert! (get-connection) 
                 :analysisscenario 
                 {:plant plant :settings (pr-str settings) :name name})))

(defn insert-scenario-result [plant date analysis-id result]
  (binding [*print-length* nil
            *print-level* nil]
    (let [date (as-sql-timestamp date) 
          e (pr-str result)]
      (sql/insert! (get-connection)
                   :analysis 
                   {:plant plant :date date :result e :scenario analysis-id}))))

(defn get-analysis-results "Get results for an analysis scenario." [plant s e analysis-id]
  (let [s (as-sql-timestamp s) 
        e (as-sql-timestamp e)]
    (q ["select result 
              from analysis
             where plant=? and 
                   date >= ? and date <= ? and 
                   scenario=?
          order by date"
        plant s e analysis-id]
      :row-fn (comp read-string :result))))