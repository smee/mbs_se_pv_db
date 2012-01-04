(ns mbs-db.core
  (:use 
    [mbs-db.util :only (encrypt-name encrypt decrypt-name)])
  (:require 
    [clojure.java.jdbc :as sql]
    [clojure.data.json :as json]
    [clojure.core.memoize :as cache]))

(def ^:dynamic *db* 
  {:classname   "org.h2.Driver"
   :subprotocol "h2"
   :user        "sa"
   :password     ""
   :subname      "~/solarlog"})

(defn- adhoc [query & params]
  (sql/with-connection *db*
       (sql/with-query-results res (apply vector query params) (doall (for [r res] r)))))

(defn- handle-params 
  "decrypt the first parameter, assuming it's a name"
  [params]
  (when-let [[p & ps] params] 
    (if (string? p) 
      (cons (decrypt-name p) ps)
      params)))

(defn- handle-results
  "encrypt all names found via accessing key :name in all result maps"
  [res]
  (map #(if-let [n (:name %)]
          (assoc % :name (encrypt-name n))
          %)
       res))

(defmacro defquery 
  "Create an sql query that accepts a variable number of paramters and a body that handles the 
sequence of results by manipulating the var 'res'. Handles name obfuscation transparently."
  [name doc-string query & body]
  `(defn ~name ~doc-string[& params#]
     (let [params# (handle-params params#)] 
       ;; run the query
       (sql/with-connection 
         *db* 
         (sql/with-query-results 
           ~'res (reduce conj [~query] params#) 
           (let [~'res (handle-results ~'res)]
             ;; let user handle the results
             ~@body))))))

(defmacro defquery-cached [name doc-string query & body]
  `(do
     (defquery ~name ~doc-string ~query ~@body)
     (alter-var-root #'~name cache/memo-lru 30)))

(defn- fix-time
  ([r] (fix-time r :time))
  ([r & keys]
  (reduce #(if-let [ts (get % %2)] 
             (assoc % %2 (.getTime ts))
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

(defquery-cached all-names-limit "Select all pv names (first part of the time series' names)."
  "select distinct SUBSTRING_INDEX(name,'.',1) as name from tsnames limit ?,?"
  (doall (map :name res)))

(defquery count-all-series "Count all available time series."
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

(defquery-cached all-values-in-time-range "Select all time series data points of a given name that are between two times."
  "select time, value from ts2 where belongs=(select belongs from tsnames where name=?) and time >? and time <?  order by time"
  (doall (map fix-time res)))

(defquery-cached min-max-time-of "Select time of the oldest data point of a time series."
  "select min(time) as min, max(time) as max from ts2 where belongs=(select belongs from tsnames where name=?)"
  (fix-time (first res) :min :max))

(defquery summed-values-in-time-range "Select times and added values of all time series that match a given parameter and are between two times."
  "select time, sum(value) as value 
     from ts2 where belongs in (select belongs from tsnames where name like ?)  
      and time>? and time<? 
 group by time 
 order by time"
  (doall (doall (map (comp #(assoc % :value (.doubleValue (:value %))) fix-time) res))))

(defquery-cached get-metadata "get map of metadata for one pv installation"
  "select json from metadatajson where name=?"
  (when (first res) 
    (let[m (-> res first :json json/read-json)
         private-names [:BannerZeile1 :BannerZeile2 :BannerZeile3 :HPBetreiber :HPEmail :HPStandort :HPTitel]]
      (reduce #(update-in % [%2] encrypt) m private-names))))

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

