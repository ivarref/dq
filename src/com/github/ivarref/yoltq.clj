(ns com.github.ivarref.yoltq
  (:require
    [clojure.tools.logging :as log]
    [com.github.ivarref.yoltq.error-poller :as errpoller]
    [com.github.ivarref.yoltq.impl :as i]
    [com.github.ivarref.yoltq.migrate :as migrate]
    [com.github.ivarref.yoltq.poller :as poller]
    [com.github.ivarref.yoltq.report-queue :as rq]
    [com.github.ivarref.yoltq.slow-executor-detector :as slow-executor]
    [com.github.ivarref.yoltq.utils :as u]
    [datomic.api :as d])
  (:import (datomic Connection)
           (java.lang.management ManagementFactory)
           (java.time Duration Instant ZoneOffset ZonedDateTime)
           (java.util.concurrent ExecutorService Executors TimeUnit)))


(defonce ^:dynamic *config* (atom nil))
(defonce threadpool (atom nil))
(defonce ^:dynamic *running?* (atom false))
(defonce ^:dynamic *test-mode* false)

(def default-opts
  (-> {; Default number of times a queue job will be retried before giving up
       ; Can be overridden on a per-consumer basis with
       ; (yq/add-consumer! :q (fn [payload] ...) {:max-retries 200})
       ; If you want no limit on the number of retries, specify
       ; the value `0`. That will set the effective retry limit to
       ; 9223372036854775807 times.
       :max-retries                   10000

       ; Minimum amount of time to wait before a failed queue job is retried
       :error-backoff-time            (Duration/ofSeconds 5)

       ; Max time a queue job can execute before an error is logged
       :max-execute-time              (Duration/ofMinutes 5)

       ; Amount of time an in progress queue job can run before it is considered failed
       ; and will be marked as such.
       :hung-backoff-time             (Duration/ofMinutes 30)

       ; Most queue jobs in init state will be consumed by the tx-report-queue listener.
       ; However, in the case where an init job was added right before the application
       ; was shut down and did not have time to be processed by the tx-report-queue listener,
       ; it will be consumer by the init poller. This init poller backs off by
       ; :init-backoff-time to avoid unnecessary compare-and-swap lock failures that could
       ; otherwise occur if competing with the tx-report-queue listener.
       :init-backoff-time             (Duration/ofSeconds 60)

       ; If you are dealing with a flaky downstream service, you may not want
       ; yoltq to mark itself as unhealthy on the first failure encounter with
       ; the downstream service. Change this setting to let yoltq mark itself
       ; as healthy even though a queue item has been failing for some time.
       :healthy-allowed-error-time    (Duration/ofMinutes 15)

       ; How frequent polling for init, error and hung jobs should be done.
       :poll-delay                    (Duration/ofSeconds 10)

       ; Specifies the number of threads available for executing queue and polling jobs.
       ; The final thread pool will be this size + 2.
       ;
       ; One thread is permanently allocated for listening to the
       ; tx-report-queue.
       ;
       ; Another thread is permanently allocated for checking :max-execute-time.
       ; This means that if all executing queue jobs are stuck and the thread pool is unavailable
       ; as such, at least an error will be logged about this. The log entry will
       ; contain the stacktrace of the stuck threads.
       :pool-size                     4

       :capture-bindings              (if-let [s (resolve (symbol "taoensso.timbre/*context*"))]
                                        [s]
                                        [])

       ; How often should the system be polled for failed queue jobs
       :system-error-poll-delay       (Duration/ofMinutes 1)

       ; How often should the system invoke
       :system-error-callback-backoff (Duration/ofHours 1)

       ; Should old, possibly stalled jobs be automatically be migrated
       ; as part of `start!`?
       :auto-migrate?                 true}

      u/duration->millis))


(defn init! [{:keys [conn] :as cfg}]
  (assert (instance? Connection conn) (str "Expected :conn to be of type datomic Connection. Was: " (or (some-> conn class str) "nil")))
  (locking threadpool
    @(d/transact conn i/schema)
    (let [new-cfg (swap! *config*
                         (fn [old-conf]
                           (-> (merge-with (fn [_ b] b)
                                           {:running-queues            (atom #{})
                                            :start-execute-time        (atom {})
                                            :system-error              (atom {})
                                            :healthy?                  (atom nil)
                                            :slow?                     (atom nil)
                                            :slow-thread-watcher-done? (promise)}
                                           default-opts
                                           (if *test-mode* old-conf (select-keys old-conf [:handlers]))
                                           cfg)
                               u/duration->millis)))]
      new-cfg)))


(defn get-queue-id
  [queue-id-or-var]
  (cond (and (var? queue-id-or-var)
             (keyword? (:yoltq/queue-id (meta queue-id-or-var))))
        (:yoltq/queue-id (meta queue-id-or-var))

        (keyword? queue-id-or-var)
        queue-id-or-var

        :else
        (throw (ex-info (str "Could not get queue-id for " queue-id-or-var) {:queue-id queue-id-or-var}))))

(defn add-consumer!
  ([queue-id f]
   (add-consumer! queue-id f {}))
  ([queue-id f opts]
   (swap! *config* (fn [old-config] (assoc-in old-config [:handlers (get-queue-id queue-id)] (merge opts {:f f}))))))


(defn put
  ([queue-id payload] (put queue-id payload {}))
  ([queue-id payload opts]
   (let [{:keys [bootstrap-poller! conn] :as cfg} @*config*]
     (when (and *test-mode* bootstrap-poller!)
       (bootstrap-poller! conn))
     (i/put cfg (get-queue-id queue-id) payload opts))))


(defn- do-start! []
  (let [{:keys [poll-delay pool-size system-error-poll-delay auto-migrate? slow-thread-watcher-done?] :as cfg} @*config*]
    (when auto-migrate?
      (future (migrate/migrate! cfg)))
    (let [pool (reset! threadpool (Executors/newScheduledThreadPool (+ 1 pool-size)))
          queue-listener-ready (promise)]
      (reset! *running?* true)
      (.scheduleAtFixedRate pool (fn [] (poller/poll-all-queues! *running?* *config* pool)) 0 poll-delay TimeUnit/MILLISECONDS)
      (.scheduleAtFixedRate pool (fn [] (errpoller/poll-errors *running?* *config*)) 0 system-error-poll-delay TimeUnit/MILLISECONDS)
      (.execute pool (fn [] (rq/report-queue-listener *running?* queue-listener-ready pool *config*)))
      (future (try
                (slow-executor/show-slow-threads pool *config*)
                (finally
                  (deliver slow-thread-watcher-done? :done))))
      @queue-listener-ready)))


(defn start! []
  (locking threadpool
    (cond (true? *test-mode*)
          (log/info "test mode enabled, doing nothing for start!")

          (true? @*running?*)
          nil

          (false? @*running?*)
          (do-start!))))


(defn stop! []
  (locking threadpool
    (cond (true? *test-mode*)
          (log/info "test mode enabled, doing nothing for stop!")

          (false? @*running?*)
          nil

          (true? @*running?*)
          (do
            (reset! *running?* false)
            (when-let [^ExecutorService tp @threadpool]
              (log/debug "shutting down threadpool")
              (.shutdown tp)
              (while (not (.awaitTermination tp 1 TimeUnit/SECONDS))
                (log/trace "waiting for threadpool to stop"))
              (log/debug "stopped!")
              (reset! threadpool nil))
            (when-let [wait-slow-threads (some->> *config* deref :slow-thread-watcher-done?)]
              (log/debug "waiting for slow-thread-watcher to stop ...")
              @wait-slow-threads
              (log/debug "waiting for slow-thread-watcher to stop ... OK"))))))


(defn healthy? []
  (cond
    (< (.toMinutes (Duration/ofMillis (.getUptime (ManagementFactory/getRuntimeMXBean)))) 10)
    true

    (false? (some->> @*config*
                     :healthy?
                     (deref)))
    false

    (true? (some->> @*config*
                    :slow?
                    (deref)))
    false

    :else
    true))

(defn unhealthy?
  "Returns `true` if there are queues in error or a thread is slow, and the application has been up for over 10 minutes, otherwise `false`."
  []
  (false? (healthy?)))

(defn queue-stats []
  (let [{:keys [conn]} @*config*
        db (d/db conn)]
    (->> (d/q '[:find ?e ?qname ?status
                :in $
                :where
                [?e :com.github.ivarref.yoltq/queue-name ?qname]
                [?e :com.github.ivarref.yoltq/status ?status]]
              db)
         (mapv (partial zipmap [:e :qname :status]))
         (mapv #(select-keys % [:qname :status]))
         (mapv (fn [qitem] {qitem 1}))
         (reduce (partial merge-with +) {})
         (mapv (fn [[{:keys [qname status]} v]]
                 (array-map
                   :qname qname
                   :status status
                   :count v)))
         (sort-by (juxt :qname :status))
         (vec))))

(defn batch-progress [queue-name batch-name]
  (let [{:keys [conn]} @*config*
        db (d/db conn)]
    (->> (d/q '[:find ?e ?qname ?bname ?status
                :keys :e :qname :bname :status
                :in $ ?qname ?bname
                :where
                [?e :com.github.ivarref.yoltq/queue-name ?qname]
                [?e :com.github.ivarref.yoltq/batch-name ?bname]
                [?e :com.github.ivarref.yoltq/status ?status]]
              db queue-name batch-name)
         (mapv #(select-keys % [:qname :bname :status]))
         (mapv (fn [qitem] {qitem 1}))
         (reduce (partial merge-with +) {})
         (mapv (fn [[{:keys [qname bname status]} v]]
                 (array-map
                  :qname qname
                  :batch-name bname
                  :status status
                  :count v)))
         (sort-by (juxt :qname :batch-name :status))
         (vec))))

(defn get-errors [qname]
  (let [{:keys [conn]} @*config*
        db (d/db conn)]
    (->> (d/q '[:find [?id ...]
                :in $ ?qname ?status
                :where
                [?e :com.github.ivarref.yoltq/queue-name ?qname]
                [?e :com.github.ivarref.yoltq/status ?status]
                [?e :com.github.ivarref.yoltq/id ?id]]
              db
              qname
              :error)
         (mapv (partial u/get-queue-item db)))))

(defn retry-one-error! [qname]
  (let [{:keys [handlers] :as cfg} @*config*
        _ (assert (contains? handlers qname) "Queue not found")
        cfg (assoc-in cfg [:handlers qname :max-retries] Long/MAX_VALUE)]
    (poller/poll-once! cfg qname :error)))

(defn retry-stats
  "Gather retry statistics.

  Optional keyword arguments:
  * :age-days —  last number of days to look at data from. Defaults to 30.
  * :queue-name — only gather statistics for this queue name. Defaults to nil, meaning all queues.

  Example return value:
  {:queue-a {:ok 100, :retries 2, :retry-percentage 2.0}
   :queue-b {:ok 100, :retries 75, :retry-percentage 75.0}}

  From the example value above, we can see that :queue-b fails at a much higher rate than :queue-a.
  Assuming that the queue consumers are correctly implemented, this means that the service representing :queue-b
  is much more unstable than the one representing :queue-a. This again implies
  that you will probably want to fix the downstream service of :queue-b, if that is possible.
  "
  [{:keys [age-days queue-name now db]
    :or   {age-days 30
           now      (ZonedDateTime/now ZoneOffset/UTC)}}]
  (let [{:keys [conn]} @*config*
        db (or db (d/db conn))]
    (->> (d/query {:query {:find  '[?qname ?status ?tries ?init-time]
                           :in    (into '[$] (when queue-name '[?qname]))
                           :where '[[?e :com.github.ivarref.yoltq/queue-name ?qname]
                                    [?e :com.github.ivarref.yoltq/status ?status]
                                    [?e :com.github.ivarref.yoltq/tries ?tries]
                                    [?e :com.github.ivarref.yoltq/init-time ?init-time]]}
                   :args  (remove nil? [db queue-name])})
         (mapv (partial zipmap [:qname :status :tries :init-time]))
         (mapv #(update % :init-time (fn [init-time] (.atZone (Instant/ofEpochMilli init-time) ZoneOffset/UTC))))
         (mapv #(assoc % :age-days (.toDays (Duration/between (:init-time %) now))))
         (filter #(<= (:age-days %) age-days))
         (group-by :qname)
         (mapv (fn [[q values]]
                 {q (let [{:keys [ok retries] :as m} (->> values
                                                          (mapv (fn [{:keys [tries status]}]
                                                                  (condp = status
                                                                    u/status-init {}
                                                                    u/status-processing {:processing 1 :retries (dec tries)}
                                                                    u/status-done {:ok 1 :retries (dec tries)}
                                                                    u/status-error {:error 1 :retries (dec tries)})))
                                                          (reduce (partial merge-with +) {}))]
                      (into (sorted-map) (merge m
                                                (when (pos-int? ok)
                                                  {:retry-percentage (double (* 100 (/ retries ok)))}))))}))
         (into (sorted-map)))))

(defn- percentile [n values]
  (let [idx (int (Math/floor (* (count values) (/ n 100))))]
    (nth values idx)))

(defn processing-time-stats
  "Gather processing time statistics. Default unit is seconds.

  Optional keyword arguments:
  * :age-days —  last number of days to look at data from. Defaults to 30.
                 Use nil to have no limit.

  * :queue-name — only gather statistics for this queue name. Defaults to nil, meaning all queues.

  * :duration->long - Specify what unit should be used for values.
                      Must take a java.time.Duration as input and return a long.

                      Defaults to (fn [duration] (.toSeconds duration).
                      I.e. the default unit is seconds.

  Example return value:
  {:queue-a {:avg 1
             :max 10
             :min 0
             :p50 ...
             :p90 ...
             :p95 ...
             :p99 ...}}"
  [{:keys [age-days queue-name now db duration->long]
    :or   {age-days 30
           now      (ZonedDateTime/now ZoneOffset/UTC)
           duration->long (fn [duration] (.toSeconds duration))}}]
  (let [{:keys [conn]} @*config*
        db (or db (d/db conn))
        ->zdt #(.atZone (Instant/ofEpochMilli %) ZoneOffset/UTC)]
    (->> (d/query {:query {:find  '[?qname ?status ?init-time ?done-time]
                           :in    (into '[$ ?status] (when queue-name '[?qname]))
                           :where '[[?e :com.github.ivarref.yoltq/queue-name ?qname]
                                    [?e :com.github.ivarref.yoltq/status ?status]
                                    [?e :com.github.ivarref.yoltq/init-time ?init-time]
                                    [?e :com.github.ivarref.yoltq/done-time ?done-time]]}
                   :args  (vec (remove nil? [db u/status-done queue-name]))})
         (mapv (partial zipmap [:qname :status :init-time :done-time]))
         (mapv #(update % :init-time ->zdt))
         (mapv #(update % :done-time ->zdt))
         (mapv #(assoc % :age-days (.toDays (Duration/between (:init-time %) now))))
         (mapv #(assoc % :spent-time (duration->long (Duration/between (:init-time %) (:done-time %)))))
         (filter #(or (nil? age-days) (<= (:age-days %) age-days)))
         (group-by :qname)
         (mapv (fn [[q values]]
                 (let [values (vec (sort (mapv :spent-time values)))]
                   {q (sorted-map
                        :max (apply max values)
                        :avg (int (Math/floor (/ (reduce + 0 values) (count values))))
                        :p50 (percentile 50 values)
                        :p90 (percentile 90 values)
                        :p95 (percentile 95 values)
                        :p99 (percentile 99 values)
                        :min (apply min values))})))
         (into (sorted-map)))))

(comment
  (do
    (require 'com.github.ivarref.yoltq.log-init)
    (com.github.ivarref.yoltq.log-init/init-logging!
      [[#{"datomic.*" "com.datomic.*" "org.apache.*"} :warn]
       [#{"ivarref.yoltq.report-queue"} :info]
       [#{"ivarref.yoltq.poller"} :info]
       [#{"com.github.ivarref.yoltq"} :debug]
       ;[#{"ivarref.yoltq*"} :info]
       [#{"*"} :info]])
    (stop!)
    (future (let [received (atom [])
                  uri (str "datomic:mem://demo")]
              (d/delete-database uri)
              (d/create-database uri)
              (let [conn (d/connect uri)
                    started-consuming? (promise)
                    n 1]
                (init! {:conn                         conn
                        :error-backoff-time           (Duration/ofSeconds 1)
                        :poll-delay                   (Duration/ofSeconds 1)
                        :max-execute-time             (Duration/ofSeconds 3)
                        :slow-thread-show-stacktrace? false})
                (add-consumer! :q (fn [_]
                                    (deliver started-consuming? true)
                                    (log/info "sleeping...")
                                    (Thread/sleep (.toMillis (Duration/ofSeconds 60)))
                                    (log/info "done sleeping")))
                (start!)
                @(d/transact conn [(put :q {:work 123})])
                @started-consuming?
                (stop!)
                nil)))))
