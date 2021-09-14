(ns ivarref.yoltq.impl
  (:require [datomic.api :as d]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [ivarref.yoltq.utils :as u]
            [ivarref.yoltq.ext-sys :as ext]))


(def schema
  [#:db{:ident :ivarref.yoltq/id, :cardinality :db.cardinality/one, :valueType :db.type/uuid, :unique :db.unique/identity}
   #:db{:ident :ivarref.yoltq/queue-name, :cardinality :db.cardinality/one, :valueType :db.type/keyword, :index true}
   #:db{:ident :ivarref.yoltq/status, :cardinality :db.cardinality/one, :valueType :db.type/keyword, :index true}
   #:db{:ident :ivarref.yoltq/payload, :cardinality :db.cardinality/one, :valueType :db.type/string}
   #:db{:ident :ivarref.yoltq/bindings, :cardinality :db.cardinality/one, :valueType :db.type/string}
   #:db{:ident :ivarref.yoltq/tries, :cardinality :db.cardinality/one, :valueType :db.type/long, :noHistory true}
   #:db{:ident :ivarref.yoltq/lock, :cardinality :db.cardinality/one, :valueType :db.type/uuid, :noHistory true}
   #:db{:ident :ivarref.yoltq/init-time, :cardinality :db.cardinality/one, :valueType :db.type/long}
   #:db{:ident :ivarref.yoltq/processing-time, :cardinality :db.cardinality/one, :valueType :db.type/long}
   #:db{:ident :ivarref.yoltq/done-time, :cardinality :db.cardinality/one, :valueType :db.type/long}
   #:db{:ident :ivarref.yoltq/error-time, :cardinality :db.cardinality/one, :valueType :db.type/long}])


(defn put [config queue-name payload]
  (if-let [_ (get-in config [:handlers queue-name])]
    (let [id (u/squuid)]
      (log/debug "queue item" (str id) "for queue" queue-name "is pending status" u/status-init)
      {:ivarref.yoltq/id         id
       :ivarref.yoltq/queue-name queue-name
       :ivarref.yoltq/status     u/status-init
       :ivarref.yoltq/payload    (pr-str payload)
       :ivarref.yoltq/bindings   (pr-str {})
       :ivarref.yoltq/lock       (u/random-uuid)
       :ivarref.yoltq/tries      0
       :ivarref.yoltq/init-time  (u/now-ns)})
    (do
      (log/error "Did not find registered handler for queue" queue-name)
      (throw (ex-info (str "Did not find registered handler for queue: " queue-name) {:queue queue-name})))))


(defn take! [{:keys [conn cas-failures hung-log-level]
              :or   {hung-log-level :error}}
             {:keys [tx id queue-name was-hung? to-error?] :as queue-item-info}]
  (when queue-item-info
    (try
      (cond to-error?
            (log/logp hung-log-level "queue-item" (str id) "was hung and retried too many times. Giving up!")

            was-hung?
            (log/logp hung-log-level "queue-item" (str id) "was hung, retrying ...")

            :else
            nil)
      (let [{:keys [db-after]} @(d/transact conn tx)
            {:ivarref.yoltq/keys [status] :as q-item} (u/get-queue-item db-after id)]
        (log/debug "queue item" (str id) "for queue" queue-name "now has status" status)
        q-item)
      (catch Throwable t
        (let [{:db/keys [error] :as m} (u/db-error-map t)]
          (cond
            (= :db.error/cas-failed error)
            (do
              (log/info ":db.error/cas-failed for queue item" (str id) "and attribute" (:a m))
              (when cas-failures
                (swap! cas-failures inc))
              nil)

            :else
            (do
              (log/error t "Unexpected failure for queue item" (str id) ":" (ex-message t))
              nil)))))))


(defn mark-status! [{:keys [conn]}
                    {:ivarref.yoltq/keys [id lock tries]}
                    new-status]
  (try
    (let [tx [[:db/cas [:ivarref.yoltq/id id] :ivarref.yoltq/lock lock (u/random-uuid)]
              [:db/cas [:ivarref.yoltq/id id] :ivarref.yoltq/tries tries (inc tries)]
              [:db/cas [:ivarref.yoltq/id id] :ivarref.yoltq/status u/status-processing new-status]
              (if (= new-status u/status-done)
                {:db/id [:ivarref.yoltq/id id] :ivarref.yoltq/done-time (u/now-ns)}
                {:db/id [:ivarref.yoltq/id id] :ivarref.yoltq/error-time (u/now-ns)})]
          {:keys [db-after]} @(d/transact conn tx)]
      (u/get-queue-item db-after id))
    (catch Throwable t
      (log/error t "unexpected error in mark-status!: " (ex-message t))
      nil)))


(defn fmt [id queue-name new-status tries spent-ns]
  (str/join " " ["queue-item" (str id)
                 "for queue" queue-name
                 "now has status" new-status
                 "after" tries (if (= 1 tries)
                                 "try"
                                 "tries")
                 "in" (format "%.1f" (double (/ spent-ns 1e6))) "ms"]))


(defn execute! [{:keys [handlers mark-status-fn! start-execute-time]
                 :or   {mark-status-fn! mark-status!}
                 :as   cfg}
                {:ivarref.yoltq/keys [status id queue-name payload] :as queue-item}]
  (when queue-item
    (if (= :error status)
      (assoc queue-item :failed? true)
      (if-let [queue (get handlers queue-name)]
        (let [{:keys [f allow-cas-failure?]} queue]
          (log/debug "queue item" (str id) "for queue" queue-name "is now processing")
          (let [{:keys [retval exception]}
                (try
                  (swap! start-execute-time assoc (Thread/currentThread) [(ext/now-ns) id queue-name])
                  (let [v (f payload)]
                    {:retval v})
                  (catch Throwable t
                    {:exception t})
                  (finally
                    (swap! start-execute-time dissoc (Thread/currentThread))))
                {:db/keys [error] :as m} (u/db-error-map exception)]
            (cond
              (and (some? exception)
                   allow-cas-failure?
                   (= :db.error/cas-failed error)
                   (or (true? allow-cas-failure?)
                       (allow-cas-failure? (:a m))))
              (when-let [q-item (mark-status-fn! cfg queue-item u/status-done)]
                (let [{:ivarref.yoltq/keys [init-time done-time tries]} q-item]
                  (log/info (fmt id queue-name u/status-done tries (- done-time init-time)))
                  (assoc q-item :retval retval :success? true :allow-cas-failure? true)))

              (some? exception)
              (when-let [q-item (mark-status-fn! cfg queue-item u/status-error)]
                (let [{:ivarref.yoltq/keys [init-time error-time tries]} q-item
                      level (if (>= tries 3) :error :warn)]
                  (log/logp level exception (fmt id queue-name u/status-error tries (- error-time init-time)))
                  (log/logp level exception "error message was:" (str \" (ex-message exception) \") "for queue-item" (str id))
                  (log/logp level exception "ex-data was:" (ex-data exception) "for queue-item" (str id))
                  (assoc q-item :exception exception)))

              :else
              (when-let [q-item (mark-status-fn! cfg queue-item u/status-done)]
                (let [{:ivarref.yoltq/keys [init-time done-time tries]} q-item]
                  (log/info (fmt id queue-name u/status-done tries (- done-time init-time)))
                  (assoc q-item :retval retval :success? true))))))
        (do
          (log/error "no handler for queue" queue-name)
          nil)))))
