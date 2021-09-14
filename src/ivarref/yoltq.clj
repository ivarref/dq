(ns ivarref.yoltq
  (:require [datomic-schema.core]
            [datomic.api :as d]
            [clojure.tools.logging :as log]
            [ivarref.yoltq.impl :as i]
            [ivarref.yoltq.report-queue :as rq]
            [ivarref.yoltq.poller :as poller]
            [ivarref.yoltq.error-poller :as errpoller]
            [ivarref.yoltq.slow-executor-detector :as slow-executor]
            [ivarref.yoltq.utils :as u])
  (:import (datomic Connection)
           (java.util.concurrent Executors TimeUnit ExecutorService)
           (java.time Duration)))


(defonce ^:dynamic *config* (atom nil))
(defonce threadpool (atom nil))
(defonce ^:dynamic *running?* (atom false))
(defonce ^:dynamic *test-mode* false)


(def default-opts
  (-> {; Default number of times a queue job will be retried before giving up
       ; Can be overridden on a per consumer basis with
       ; (yq/add-consumer! :q (fn [payload] ...) {:max-retries 200})
       :max-retries                   100

       ; Minimum amount of time to wait before a failed queue job is retried
       :error-backoff-time            (Duration/ofSeconds 5)

       ; Max time a queue job can execute before an error is logged
       :max-execute-time              (Duration/ofMinutes 5)

       ; Amount of time an in progress queue job can run before it is considered failed
       ; and will be marked as such.
       :hung-backoff-time             (Duration/ofMinutes 30)

       ; Most queue jobs in init state will be consumed by the tx-report-queue listener.
       ; However in the case where a init job was added right before the application
       ; was shut down and did not have time to be processed by the tx-report-queue listener,
       ; it will be consumer by the init poller. This init poller backs off by
       ; :init-backoff-time to avoid unnecessary compare-and-swap lock failures that could
       ; otherwise occur if competing with the tx-report-queue listener.
       :init-backoff-time             (Duration/ofSeconds 60)

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

       ; How often should the system be polled for failed queue jobs
       :system-error-poll-delay       (Duration/ofMinutes 1)

       ; How often should the system invoke
       :system-error-callback-backoff (Duration/ofHours 1)}

      u/duration->nanos))


(defn init! [{:keys [conn] :as cfg}]
  (assert (instance? Connection conn) (str "Expected :conn to be of type datomic Connection. Was: " (or (some-> conn class str) "nil")))
  (locking threadpool
    @(d/transact conn i/schema)
    (let [new-cfg (swap! *config*
                         (fn [old-conf]
                           (-> (merge-with (fn [a b] (or b a))
                                           {:running-queues     (atom #{})
                                            :start-execute-time (atom {})}
                                           default-opts
                                           old-conf
                                           cfg)
                               (assoc :system-error (atom {}))
                               u/duration->nanos)))]
      new-cfg)))


(defn add-consumer!
  ([queue-id f]
   (add-consumer! queue-id f {}))
  ([queue-id f opts]
   (swap! *config* (fn [old-config] (assoc-in old-config [:handlers queue-id] (merge opts {:f f}))))))


(defn put [id payload]
  (let [{:keys [bootstrap-poller! conn] :as cfg} @*config*]
    (when (and *test-mode* bootstrap-poller!)
      (bootstrap-poller! conn))
    (i/put cfg id payload)))


(defn- do-start! []
  (let [{:keys [poll-delay pool-size system-error-poll-delay]} @*config*]
    (reset! threadpool (Executors/newScheduledThreadPool (+ 2 pool-size)))
    (let [pool @threadpool
          queue-listener-ready (promise)]
      (reset! *running?* true)
      (.scheduleAtFixedRate pool (fn [] (poller/poll-all-queues! *running?* *config* pool)) 0 poll-delay TimeUnit/NANOSECONDS)
      (.scheduleAtFixedRate pool (fn [] (errpoller/poll-errors *running?* *config*)) 0 system-error-poll-delay TimeUnit/NANOSECONDS)
      (.execute pool (fn [] (rq/report-queue-listener *running?* queue-listener-ready pool *config*)))
      (.execute pool (fn [] (slow-executor/show-slow-threads *running?* *config*)))
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
              (log/debug "shutting down old threadpool")
              (.shutdown tp)
              (while (not (.awaitTermination tp 1 TimeUnit/SECONDS))
                (log/debug "waiting for threadpool to stop"))
              (log/debug "stopped!")
              (reset! threadpool nil))))))


(comment
  (do
    (require 'ivarref.yoltq.log-init)
    (ivarref.yoltq.log-init/init-logging!
      [[#{"datomic.*" "com.datomic.*" "org.apache.*"} :warn]
       [#{"ivarref.yoltq.report-queue"} :info]
       [#{"ivarref.yoltq.poller"} :info]
       [#{"ivarref.yoltq*"} :info]
       [#{"*"} :info]])
    (stop!)
    (let [received (atom [])
          uri (str "datomic:mem://demo")]
      (d/delete-database uri)
      (d/create-database uri)
      (let [ok-items (atom [])
            conn (d/connect uri)
            n 100]
        (init! {:conn                          conn
                :error-backoff-time            (Duration/ofSeconds 1)
                :poll-delay                    (Duration/ofSeconds 1)})
        (add-consumer! :q (fn [payload]
                            (when (> (Math/random) 0.5)
                              (throw (ex-info "oops" {})))
                            (if (= n (count (swap! received conj (:work payload))))
                              (log/info "... and we are done!")
                              (log/info "got payload" payload "total ok:" (count @received)))))
        (start!)
        (dotimes [x n]
          @(d/transact conn [(put :q {:work x})]))
        nil))))