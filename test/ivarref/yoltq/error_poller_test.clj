(ns ivarref.yoltq.error-poller-test
  (:require [clojure.test :refer :all]
            [ivarref.yoltq.error-poller :as ep]
            [clojure.tools.logging :as log]
            [ivarref.yoltq.log-init :as logconfig]
            [clojure.edn :as edn]))


(deftest error-poller
  (logconfig/init-logging!
    [[#{"datomic.*" "com.datomic.*" "org.apache.*"} :warn]
     [#{"*"} (edn/read-string
               (System/getProperty "TAOENSSO_TIMBRE_MIN_LEVEL_EDN" ":info"))]])
  (let [cfg {:system-error-callback-backoff 100}
        time (atom 0)
        tick! (fn [& [amount]]
                (swap! time + (or amount 1)))
        verify (fn [state now-ns error-count expected-callback]
                 (let [{:keys [errors state run-callback] :as res} (ep/handle-error-count state cfg now-ns error-count)]
                   (log/info errors "=>" state "::" run-callback)
                   (is (= expected-callback run-callback))
                   res))]
    (-> {}
        (verify (tick!) 0 nil)
        (verify (tick!) 1 nil)
        (verify (tick!) 1 nil)
        (verify (tick!) 1 :error)
        (verify (tick! 100) 0 nil)
        (verify (tick!) 0 :error)
        (verify (tick!) 0 :recovery)
        (verify (tick!) 1 nil)
        (verify (tick!) 1 nil)
        (verify (tick!) 1 :error)
        (verify (tick! 100) 1 nil)
        (verify (tick!) 1 :error))))
