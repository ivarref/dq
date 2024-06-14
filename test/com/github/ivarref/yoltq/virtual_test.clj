(ns com.github.ivarref.yoltq.virtual-test
  (:require
    [clojure.string :as str]
    [clojure.test :refer [deftest is use-fixtures] :refer-macros [thrown?]]
    [clojure.tools.logging :as log]
    [com.github.ivarref.yoltq :as yq]
    [com.github.ivarref.yoltq.error-poller :as error-poller]
    [com.github.ivarref.yoltq.ext-sys :as ext]
    [com.github.ivarref.yoltq.impl :as i]
    [com.github.ivarref.yoltq.migrate :as migrate]
    [com.github.ivarref.yoltq.test-queue :as tq]
    [com.github.ivarref.yoltq.test-utils :as u]
    [com.github.ivarref.yoltq.utils :as uu]
    [datomic-schema.core]
    [datomic.api :as d]
    [taoensso.nippy :as nippy]
    [taoensso.timbre :as timbre])
  (:import (java.time Duration LocalDateTime)))


(use-fixtures :each tq/call-with-virtual-queue!)


(deftest happy-case-1
  (let [conn (u/empty-conn)]
    (yq/init! {:conn conn})
    (yq/add-consumer! :q identity)
    @(d/transact conn [(yq/put :q {:work 123})])
    (is (= {:work 123} (tq/consume! :q)))))

(deftest happy-case-no-migration-for-new-entities
  (let [conn (u/empty-conn)]
    (yq/init! {:conn conn})
    (yq/add-consumer! :q identity)
    @(d/transact conn [(yq/put :q {:work 123})])
    (is (= {:work 123} (tq/consume! :q)))
    (is (= [] (migrate/migrate! @yq/*config*)))))

(deftest happy-case-tx-report-q
  (let [conn (u/empty-conn)]
    (yq/init! {:conn conn})
    (yq/add-consumer! :q identity)
    @(d/transact conn [(yq/put :q {:work 123})])
    (is (= {:work 123} (:retval (tq/run-one-report-queue!))))
    (is (= 1 (u/done-count)))))


(deftest happy-case-poller
  (let [conn (u/empty-conn)]
    (yq/init! {:conn     conn
               :handlers {:q {:f (fn [payload] payload)}}})
    (u/put-transact! :q {:work 123})
    (u/advance! (:init-backoff-time yq/default-opts))
    (is (= {:work 123} (some->> (u/get-init :q)
                                (u/take!)
                                (u/execute!)
                                :retval)))))


(deftest happy-case-queue-fn-allow-cas-failure
  (let [conn (u/empty-conn)
        invoke-count (atom 0)]
    (yq/init! {:conn conn})
    (yq/add-consumer! :q
                      (fn [{:keys [id]}]
                        (swap! invoke-count inc)
                        @(d/transact conn [[:db/cas [:e/id id] :e/val nil "janei"]]))
                      {:allow-cas-failure? #{:e/val}})
    @(d/transact conn #d/schema [[:e/id :one :string :id]
                                 [:e/val :one :string]])
    @(d/transact conn [{:e/id "demo"}
                       (yq/put :q {:id "demo"})])
    (u/advance! (:init-backoff-time yq/default-opts))
    (swap! yq/*config* assoc :mark-status-fn! (fn [_ _ new-status]
                                                (log/info "mark-status! doing nothing for new status" new-status)))
    (is (nil? (some->> (u/get-init :q)
                       (u/take!)
                       (u/execute!))))
    (swap! yq/*config* dissoc :mark-status-fn!)

    ; (mark-status! :done) failed but f succeeded.
    (is (nil? (u/get-hung :q)))
    (u/advance! (:hung-backoff-time @yq/*config*))
    (is (some? (u/get-hung :q)))
    (is (true? (some->> (u/get-hung :q)
                        (u/take!)
                        (u/execute!)
                        :allow-cas-failure?)))
    (is (= 2 @invoke-count))))


(deftest backoff-test
  (let [conn (u/empty-conn)]
    (yq/init! {:conn              conn
               :init-backoff-time (:init-backoff-time yq/default-opts)
               :handlers          {:q {:f (fn [_] (throw (ex-info "janei" {})))}}})
    (u/put-transact! :q {:work 123})
    (is (nil? (u/get-init :q)))

    (u/advance! (dec (:init-backoff-time yq/default-opts)))
    (is (nil? (u/get-init :q)))
    (u/advance! 1)
    (is (some? (u/get-init :q)))

    (is (some? (some->> (u/get-init :q)
                        (u/take!)
                        (u/execute!)
                        :exception)))

    (u/advance! (dec (:error-backoff-time @yq/*config*)))
    (is (nil? (u/get-error :q)))
    (u/advance! 1)
    (is (some? (u/get-error :q)))))


(deftest get-hung-test
  (let [conn (u/empty-conn)]
    (yq/init! {:conn              conn
               :init-backoff-time (:init-backoff-time yq/default-opts)
               :handlers          {:q {:f (fn [_] (throw (ex-info "janei" {})))}}})
    (u/put-transact! :q {:work 123})
    (u/advance! (:init-backoff-time yq/default-opts))
    (is (some? (u/get-init :q)))

    (is (= :processing (some->> (u/get-init :q)
                                (u/take!)
                                :com.github.ivarref.yoltq/status)))

    (is (nil? (u/get-hung :q)))
    (u/advance! (dec (:hung-backoff-time yq/default-opts)))
    (is (nil? (u/get-hung :q)))
    (u/advance! 1)
    (is (some? (u/get-hung :q)))))


(deftest basic-locking
  (let [conn (u/empty-conn)]
    (yq/init! {:conn              conn
               :init-backoff-time (:init-backoff-time yq/default-opts)
               :cas-failures      (atom 0)
               :handlers          {:q {:f (fn [_] (throw (ex-info "janei" {})))}}})
    (u/put-transact! :q {:work 123})
    (u/advance! (:init-backoff-time yq/default-opts))
    (is (some? (u/get-init :q)))

    (let [job (u/get-init :q)]
      (is (= :processing (some->> job (u/take!) :com.github.ivarref.yoltq/status)))
      (u/take! job)
      (is (= 1 @(:cas-failures @yq/*config*))))))


(deftest retry-test
  (let [conn (u/empty-conn)]
    (yq/init! {:conn              conn
               :init-backoff-time (:init-backoff-time yq/default-opts)
               :handlers          {:q {:f
                                       (let [c (atom 0)]
                                         (fn [_]
                                           (if (<= (swap! c inc) 2)
                                             (throw (ex-info "janei" {}))
                                             ::ok)))}}})
    (u/put-transact! :q {:work 123})

    (u/advance! (:init-backoff-time yq/default-opts))
    (is (some? (some->> (u/get-init :q) (u/take!) (u/execute!) :exception)))

    (u/advance! (:error-backoff-time @yq/*config*))
    (is (some? (some->> (u/get-error :q) (u/take!) (u/execute!) :exception)))

    (u/advance! (:error-backoff-time @yq/*config*))
    (is (nil? (some->> (u/get-error :q) (u/take!) (u/execute!) :exception)))))


(deftest max-retries-test
  (let [conn (u/empty-conn)
        call-count (atom 0)]
    (yq/init! {:conn               conn
               :error-backoff-time 0})
    (yq/add-consumer! :q (fn [_]
                           (swap! call-count inc)
                           (throw (ex-info "janei" {})))
                      {:max-retries 1})
    (tq/put! :q {:work 123})
    (is (some? (:exception (tq/run-one-report-queue!))))

    (dotimes [_ 10]
      (tq/run-queue-once! :q :error))
    (is (= 2 @call-count))))


(deftest max-retries-test-two
  (let [conn (u/empty-conn)
        call-count (atom 0)]
    (yq/init! {:conn               conn
               :error-backoff-time 0})
    (yq/add-consumer! :q (fn [_]
                           (swap! call-count inc)
                           (throw (ex-info "janei" {})))
                      {:max-retries 3})
    (tq/put! :q {:work 123})
    (is (some? (:exception (tq/run-one-report-queue!))))

    (timbre/with-level :fatal
                       (dotimes [_ 20]
                         (tq/run-queue-once! :q :error)))
    (is (= 4 @call-count))))


(deftest hung-to-error
  (let [conn (u/empty-conn)
        call-count (atom 0)
        missed-mark-status (atom nil)]
    (yq/init! {:conn conn})
    (yq/add-consumer! :q
                      (fn [_]
                        (if (= 1 (swap! call-count inc))
                          (throw (ex-info "error" {}))
                          (log/info "return OK"))))
    (tq/put! :q {:id "demo"})
    (tq/run-one-report-queue!)                              ; now in status :error


    (swap! yq/*config* assoc :mark-status-fn! (fn [_ _ new-status]
                                                (reset! missed-mark-status new-status)
                                                (log/info "mark-status! doing nothing for new status" new-status)))
    (u/advance! (:error-backoff-time @yq/*config*))
    (tq/run-queue-once! :q :error)
    (swap! yq/*config* dissoc :mark-status-fn!)
    (is (= :done @missed-mark-status))

    (is (nil? (uu/get-hung @yq/*config* :q)))
    (u/advance! (:hung-backoff-time @yq/*config*))

    (is (some? (uu/get-hung @yq/*config* :q)))

    (is (= 2 @call-count))

    (is (true? (some->> (uu/get-hung (assoc-in @yq/*config* [:handlers :q :max-retries] 1) :q)
                        (i/take! @yq/*config*)
                        (i/execute! @yq/*config*)
                        :failed?)))

    (u/advance! (:error-backoff-time @yq/*config*))
    (is (some? (uu/get-error @yq/*config* :q)))
    (is (nil? (uu/get-error (assoc-in @yq/*config* [:handlers :q :max-retries] 1) :q)))))


(deftest consume-expect-test
  (let [conn (u/empty-conn)
        seen (atom #{})]
    (yq/init! {:conn conn})
    (yq/add-consumer! :q (fn [payload]
                           (when (= #{1 2} (swap! seen conj payload))
                             (throw (ex-info "oops" {})))
                           payload))

    @(d/transact conn [(yq/put :q 1)])
    @(d/transact conn [(yq/put :q 2)])

    (is (= 1 (tq/consume-expect! :q :done)))
    (tq/consume-expect! :q :error)))


(def ^:dynamic *some-binding* nil)


(deftest binding-test
  (let [conn (u/empty-conn)]
    (yq/init! {:conn             conn
               :capture-bindings [#'*some-binding* #'timbre/*context*]})
    (yq/add-consumer! :q (fn [_] *some-binding*))
    (binding [timbre/*context* {:x-request-id "wooho"}]
      (binding [*some-binding* 1]
        @(d/transact conn [(yq/put :q nil)]))
      (binding [*some-binding* 2]
        @(d/transact conn [(yq/put :q nil)]))
      @(d/transact conn [(yq/put :q nil)]))

    (is (= 1 (tq/consume-expect! :q :done)))
    (is (= 2 (tq/consume-expect! :q :done)))
    (is (nil? (tq/consume-expect! :q :done)))))


(deftest default-binding-test
  (let [conn (u/empty-conn)]
    (yq/init! {:conn conn})
    (yq/add-consumer! :q (fn [_] (:x-request-id timbre/*context*)))
    (binding [timbre/*context* {:x-request-id "123"}]
      @(d/transact conn [(yq/put :q nil)]))
    (is (= "123" (tq/consume-expect! :q :done)))))


(deftest force-retry-test
  (let [conn (u/empty-conn)]
    (yq/init! {:conn conn})
    (yq/add-consumer! :q (let [cnt (atom 0)]
                           (fn [_] (swap! cnt inc))))
    @(d/transact conn [(yq/put :q nil)])
    (is (= 1 (tq/consume! :q)))
    (is (= 2 (tq/force-retry! :q)))
    (is (= 3 (tq/force-retry! :q)))))


(deftest ext-id-no-duplicates
  (let [conn (u/empty-conn)]
    (yq/init! {:conn conn})
    (yq/add-consumer! :q identity)
    @(d/transact conn [(yq/put :q nil {:id "123"})])
    (is (thrown? Exception @(d/transact conn [(yq/put :q nil {:id "123"})])))))


(deftest depends-on
  (let [conn (u/empty-conn)]
    (yq/init! {:conn conn})
    (yq/add-consumer! :a identity)
    (yq/add-consumer! :b identity)
    @(d/transact conn [(yq/put :a {:id "a1"} {:id "a1"})])
    @(d/transact conn [(yq/put :b {:id "b1"} {:depends-on [:a "a1"]})])

    ; can't consume :b yet:
    (is (= {:depends-on [:a "a1"]} (tq/consume! :b)))

    (is (= {:id "a1"} (tq/consume! :a)))
    (is (= {:id "b1"} (tq/consume! :b)))))


(deftest depends-on-queue-level
  (let [conn (u/empty-conn)]
    (yq/init! {:conn conn})
    (yq/add-consumer! :a identity)
    (yq/add-consumer! :b identity {:depends-on (fn [{:keys [id]}] [:a id])})
    @(d/transact conn [(yq/put :a {:id "1"} {:id "1"})])
    @(d/transact conn [(yq/put :b {:id "1"})])

    ; can't consume :b yet:
    (is (= {:depends-on [:a "1"]} (tq/consume! :b)))

    (is (= {:id "1"} (tq/consume! :a)))
    (is (= {:id "1"} (tq/consume! :b)))))


(deftest verify-can-read-string
  (let [conn (u/empty-conn)]
    (yq/init! {:conn conn})
    (yq/add-consumer! :a identity)
    (timbre/with-level :fatal
      (is (thrown? Exception @(d/transact conn [(yq/put :a {:broken #'=})]))))))


(deftest payload-verifier
  (let [conn (u/empty-conn)]
    (yq/init! {:conn conn})
    (yq/add-consumer! :q identity
                      {:valid-payload? (fn [{:keys [id]}]
                                         (some? id))})
    @(d/transact conn [(yq/put :q {:id "a"})])
    (timbre/with-level :fatal
                       (is (thrown? Exception @(d/transact conn [(yq/put :q {})]))))))


(defn my-consumer
  {:yoltq/queue-id :some-q}
  [state payload]
  (swap! state conj payload))

(deftest queue-id-can-be-var
  (let [conn (u/empty-conn)
        received (atom #{})]
    (yq/init! {:conn conn})
    (yq/add-consumer! #'my-consumer (partial my-consumer received))
    @(d/transact conn [(yq/put #'my-consumer {:id "a"})])
    (tq/consume! :some-q)
    (is (= #{{:id "a"}} @received))
    #_(timbre/with-level :fatal
                         (is (thrown? Exception @(d/transact conn [(yq/put :q {})]))))))

(deftest healthy-allowed-error-time-test
  (let [conn (u/empty-conn)]
    (yq/init! {:conn conn})
    (yq/add-consumer! :q (fn [_] (throw (ex-info "" {}))))
    @(d/transact conn [(yq/put :q {:work 123})])
    (tq/consume-expect! :q :error)
    (is (= 0 (error-poller/do-poll-errors @yq/*config* (ext/now-ms))))
    (is (= 0 (error-poller/do-poll-errors @yq/*config* (+ (dec (.toMillis (Duration/ofMinutes 15))) (ext/now-ms)))))
    (is (= 1 (error-poller/do-poll-errors @yq/*config* (+ (.toMillis (Duration/ofMinutes 15)) (ext/now-ms)))))))

(deftest global-encode-decode
  (let [conn (u/empty-conn)
        ldt (LocalDateTime/now)
        got-work (atom nil)]
    (yq/init! {:conn conn
               :encode nippy/freeze
               :decode nippy/thaw})
    (yq/add-consumer! :q (fn [work] (reset! got-work work)))
    @(d/transact conn [(yq/put :q {:work ldt})])
    (tq/consume! :q)
    (is (= @got-work {:work ldt}))))

(deftest queue-encode-decode
  (let [conn (u/empty-conn)
        ldt (LocalDateTime/now)
        got-work (atom nil)]
    (yq/init! {:conn conn})
    (yq/add-consumer! :q (fn [work] (reset! got-work work))
                      {:encode nippy/freeze
                       :decode nippy/thaw})
    @(d/transact conn [(yq/put :q {:work ldt})])
    (tq/consume! :q)
    (is (= @got-work {:work ldt}))))

(deftest global-partition
  (let [conn (u/empty-conn)
        got-work (atom nil)]
    (yq/init! {:conn conn
               :partition-fn (fn [_queue-name] :my-part)})
    (yq/add-consumer! :q (fn [work] (reset! got-work work)))
    @(d/transact conn [(yq/put :q {:work 123})])
    (tq/consume! :q)
    (is (some? (d/q '[:find ?e .
                      :in $ ?part
                      :where
                      [?e :db/ident ?part]]
                    (d/db conn)
                    :my-part)))
    (is (= @got-work {:work 123}))))

(deftest partition-per-queue
  (let [conn (u/empty-conn)
        got-work (atom nil)]
    (yq/init! {:conn conn})
    (yq/add-consumer! :q (fn [work] (reset! got-work work))
                      {:partition-fn (fn [_queue-name] :my-part)})
    @(d/transact conn [(yq/put :q {:work 123})])
    (tq/consume! :q)
    (is (some? (d/q '[:find ?e .
                      :in $ ?part
                      :where
                      [?e :db/ident ?part]]
                    (d/db conn)
                    :my-part)))
    (is (= @got-work {:work 123}))))

(deftest string-encode-decode
  (let [conn (u/empty-conn)
        got-work (atom nil)]
    (yq/init! {:conn   conn
               :encode (fn [x] (str/join (reverse x)))
               :decode (fn [x] (str/join (reverse x)))})
    (yq/add-consumer! :q (fn [work] (reset! got-work work)))
    @(d/transact conn [(yq/put :q "asdf")])
    (tq/consume! :q)
    (is (= @got-work "asdf"))))

(deftest batch-of-jobs-test
  (let [conn (u/empty-conn)]
    (yq/init! {:conn conn})
    (yq/add-consumer! :q1 identity)
    (yq/add-consumer! :q2 identity)
    @(d/transact conn [(yq/put :q1 {:work 123} {:batch-name :b1})
                       (yq/put :q1 {:work 456} {:batch-name :b2})
                       (yq/put :q2 {:work 789} {:batch-name :b1})])
    (is (= [{:qname :q1
             :batch-name :b1
             :status :init
             :count 1}]
           (yq/batch-progress :q1 :b1)))

    (is (= {:work 123} (tq/consume! :q1)))

    (is (= [{:qname :q1
             :batch-name :b1
             :status :done
             :count 1}]
           (yq/batch-progress :q1 :b1)))))
