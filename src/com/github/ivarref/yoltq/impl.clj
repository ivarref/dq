(ns com.github.ivarref.yoltq.impl
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [com.github.ivarref.double-trouble :as dt]
            [com.github.ivarref.yoltq.ext-sys :as ext]
            [com.github.ivarref.yoltq.utils :as u]
            [datomic.api :as d])
  (:import (java.time Year)))

(def schema
  [#:db{:ident :com.github.ivarref.yoltq/id, :cardinality :db.cardinality/one, :valueType :db.type/uuid, :unique :db.unique/identity}
   #:db{:ident :com.github.ivarref.yoltq/ext-id, :cardinality :db.cardinality/one, :valueType :db.type/string, :unique :db.unique/value}
   #:db{:ident :com.github.ivarref.yoltq/queue-name, :cardinality :db.cardinality/one, :valueType :db.type/keyword, :index true}
   #:db{:ident :com.github.ivarref.yoltq/batch-name, :cardinality :db.cardinality/one, :valueType :db.type/keyword, :index true}
   #:db{:ident :com.github.ivarref.yoltq/status, :cardinality :db.cardinality/one, :valueType :db.type/keyword, :index true}
   #:db{:ident :com.github.ivarref.yoltq/payload, :cardinality :db.cardinality/one, :valueType :db.type/string}
   #:db{:ident :com.github.ivarref.yoltq/payload-bytes, :cardinality :db.cardinality/one, :valueType :db.type/bytes}
   #:db{:ident :com.github.ivarref.yoltq/opts, :cardinality :db.cardinality/one, :valueType :db.type/string}
   #:db{:ident :com.github.ivarref.yoltq/bindings, :cardinality :db.cardinality/one, :valueType :db.type/string}
   #:db{:ident :com.github.ivarref.yoltq/tries, :cardinality :db.cardinality/one, :valueType :db.type/long, :noHistory true}
   #:db{:ident :com.github.ivarref.yoltq/lock, :cardinality :db.cardinality/one, :valueType :db.type/uuid, :noHistory true}
   #:db{:ident :com.github.ivarref.yoltq/init-time, :cardinality :db.cardinality/one, :valueType :db.type/long}
   #:db{:ident :com.github.ivarref.yoltq/processing-time, :cardinality :db.cardinality/one, :valueType :db.type/long}
   #:db{:ident :com.github.ivarref.yoltq/done-time, :cardinality :db.cardinality/one, :valueType :db.type/long}
   #:db{:ident :com.github.ivarref.yoltq/error-time, :cardinality :db.cardinality/one, :valueType :db.type/long}
   #:db{:ident :com.github.ivarref.yoltq/version, :cardinality :db.cardinality/one, :valueType :db.type/string, :index true}])

(defn pr-str-inner [x]
  (binding [*print-dup* false
            *print-meta* false
            *print-readably* true
            *print-length* nil
            *print-level* nil
            *print-namespace-maps* false]
    (pr-str x)))

(defn pr-str-safe [what x]
  (try
    (if (= x (edn/read-string (pr-str-inner x)))
      (pr-str-inner x)
      (throw (ex-info (str "Could not read-string " what) {:input x})))
    (catch Exception e
      (log/error "could not read-string" what ":" (ex-message e))
      (throw e))))

(defn default-partition-fn [_queue-keyword]
  (keyword "yoltq" (str "queue_" (.getValue (Year/now)))))

(defn put [{:keys [capture-bindings conn encode partition-fn]
            :or   {partition-fn default-partition-fn
                   encode       (partial pr-str-safe :payload)}
            :as   config}
           queue-name
           payload
           opts]
  (if-let [q-config (get-in config [:handlers queue-name])]
    (let [id (u/squuid)
          encode (get q-config :encode encode)
          partition-fn (get q-config :partition-fn partition-fn)
          partition (partition-fn queue-name)
          _ (assert (keyword? partition) "Partition must be a keyword")
          depends-on (get q-config :depends-on (fn [_] nil))
          valid-payload? (get q-config :valid-payload? (fn [_] true))
          opts (merge
                 (when-let [deps (depends-on payload)]
                   {:depends-on deps})
                 (or opts {}))
          str-bindings (->> (reduce (fn [o k]
                                      (assoc o (symbol k) (deref k)))
                                    {}
                                    (or capture-bindings []))
                            (pr-str-safe :capture-bindings))
          _ (when-not (valid-payload? payload)
              (log/error "Payload was not valid. Payload was:" payload)
              (throw (ex-info (str "Payload was not valid: " payload) {:payload payload})))
          encoded (encode payload)
          _ (when (not (or (bytes? encoded) (string? encoded)))
              (log/error "Payload must be encoded to either a string or a byte array")
              (throw (ex-info (str "Payload must be encoded to a string or a byte array. Payload: " payload) {:payload payload})))]
      (log/debug "queue item" (str id) "for queue" queue-name "is pending status" u/status-init)
      (do
        (dt/ensure-partition! conn partition)
        (merge
          (if (bytes? encoded)
            {:com.github.ivarref.yoltq/payload-bytes encoded}
            {:com.github.ivarref.yoltq/payload encoded})
          {:db/id                               (d/tempid partition)
           :com.github.ivarref.yoltq/id         id
           :com.github.ivarref.yoltq/queue-name queue-name
           :com.github.ivarref.yoltq/status     u/status-init
           :com.github.ivarref.yoltq/bindings   str-bindings
           :com.github.ivarref.yoltq/opts       (pr-str-safe :opts opts)
           :com.github.ivarref.yoltq/lock       (u/random-uuid)
           :com.github.ivarref.yoltq/tries      0
           :com.github.ivarref.yoltq/init-time  (u/now-ms)
           :com.github.ivarref.yoltq/version    "2"}
          (when-let [[q ext-id] (:depends-on opts)]
            (when-not (d/q '[:find ?e .
                             :in $ ?ext-id
                             :where
                             [?e :com.github.ivarref.yoltq/ext-id ?ext-id]]
                           (d/db conn)
                           (pr-str-safe :depends-on [q ext-id]))
              (throw (ex-info (str ":depends-on not found in database. Queue: " q ", id: " ext-id) opts))))
          (when-let [ext-id (:id opts)]
            {:com.github.ivarref.yoltq/ext-id (pr-str-safe :id [queue-name ext-id])})
          (when-let [batch-name (:batch-name opts)]
            {:com.github.ivarref.yoltq/batch-name batch-name}))))
    (do
      (log/error "Did not find registered handler for queue" queue-name)
      (throw (ex-info (str "Did not find registered handler for queue: " queue-name) {:queue queue-name})))))


(defn depends-on-waiting? [{:keys [conn]}
                           q-item]
  (let [db (d/db conn)]
    (when-let [{:com.github.ivarref.yoltq/keys [opts]} (u/get-queue-item db (:id q-item))]
      (when-let [[q id :as depends-on] (:depends-on opts)]
        (when-not (d/q '[:find ?e .
                         :in $ ?ext-id
                         :where
                         [?e :com.github.ivarref.yoltq/ext-id ?ext-id]
                         [?e :com.github.ivarref.yoltq/status :done]]
                       db
                       (pr-str [q id]))
          (log/info "queue item" (str (:id q-item)) "is waiting on" depends-on)
          {:depends-on depends-on})))))


(defn take! [{:keys [conn cas-failures hung-log-level tx-spent-time!]
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
      (let [start-time (System/nanoTime)
            {:keys [db-after]} @(d/transact conn tx)
            _ (when tx-spent-time! (tx-spent-time! (- (System/nanoTime) start-time)))
            {:com.github.ivarref.yoltq/keys [status] :as q-item} (u/get-queue-item db-after id)]
        (log/debug "queue item" (str id) "for queue" queue-name "now has status" status)
        q-item)
      (catch Throwable t
        (let [{:db/keys [error] :as m} (u/db-error-map t)]
          (cond
            (= :db.error/cas-failed error)
            (do
              (log/info "take! :db.error/cas-failed for queue item" (str id) "and attribute" (:a m))
              (when cas-failures
                (swap! cas-failures inc))
              nil)

            :else
            (do
              (log/error t "Unexpected failure for queue item" (str id) ":" (ex-message t))
              nil)))))))


(defn mark-status! [{:keys [conn tx-spent-time!]}
                    {:com.github.ivarref.yoltq/keys [id lock tries]}
                    new-status]
  (try
    (let [tx [[:db/cas [:com.github.ivarref.yoltq/id id] :com.github.ivarref.yoltq/lock lock (u/random-uuid)]
              [:db/cas [:com.github.ivarref.yoltq/id id] :com.github.ivarref.yoltq/tries tries (inc tries)]
              [:db/cas [:com.github.ivarref.yoltq/id id] :com.github.ivarref.yoltq/status u/status-processing new-status]
              (if (= new-status u/status-done)
                {:db/id [:com.github.ivarref.yoltq/id id] :com.github.ivarref.yoltq/done-time (u/now-ms)}
                {:db/id [:com.github.ivarref.yoltq/id id] :com.github.ivarref.yoltq/error-time (u/now-ms)})]
          start-time (System/nanoTime)
          {:keys [db-after]} @(d/transact conn tx)]
      (when tx-spent-time! (tx-spent-time! (- (System/nanoTime) start-time)))
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


(defn execute! [{:keys [decode handlers mark-status-fn! start-execute-time collect-spent-time!]
                 :or   {mark-status-fn! mark-status!
                        decode          edn/read-string}
                 :as   cfg}
                {:com.github.ivarref.yoltq/keys [status id queue-name payload payload-bytes] :as queue-item}]
  (when queue-item
    (if (= :error status)
      (assoc queue-item :failed? true)
      (if-let [queue (get handlers queue-name)]
        (let [{:keys [f allow-cas-failure?]} queue
              decode (get queue :decode decode)]
          (log/debug "queue item" (str id) "for queue" queue-name "is now processing")
          (let [{:keys [retval exception]}
                (try
                  (swap! start-execute-time assoc (Thread/currentThread) [(ext/now-ms) id queue-name])
                  (let [payload (decode (or payload payload-bytes))
                        v (f payload)]
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
                (let [{:com.github.ivarref.yoltq/keys [init-time done-time tries]} q-item]
                  (log/info (fmt id queue-name u/status-done tries (- done-time init-time)))
                  (when collect-spent-time! (collect-spent-time! (- (u/now-ms) init-time)))
                  (assoc q-item :retval retval :success? true :allow-cas-failure? true)))

              (some? exception)
              (when-let [q-item (mark-status-fn! cfg queue-item u/status-error)]
                (let [{:com.github.ivarref.yoltq/keys [init-time error-time tries]} q-item
                      level (if (>= tries 3) :error :warn)]
                  (log/logp level exception (fmt id queue-name u/status-error tries (- error-time init-time)))
                  (log/logp level exception "error message was:" (str \" (ex-message exception) \") "for queue-item" (str id))
                  (log/logp level exception "ex-data was:" (ex-data exception) "for queue-item" (str id))
                  (when collect-spent-time! (collect-spent-time! (- (u/now-ms) init-time)))
                  (assoc q-item :exception exception)))

              :else
              (when-let [q-item (mark-status-fn! cfg queue-item u/status-done)]
                (let [{:com.github.ivarref.yoltq/keys [init-time done-time tries]} q-item]
                  (log/info (fmt id queue-name u/status-done tries (- done-time init-time)))
                  (when collect-spent-time! (collect-spent-time! (- (u/now-ms) init-time)))
                  (assoc q-item :retval retval :success? true))))))
        (do
          (log/error "no handler for queue" queue-name)
          nil)))))
