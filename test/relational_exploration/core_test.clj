(ns relational-exploration.core-test
  (:refer-clojure :exclude [derive])
  (:require [clojure.test :refer :all]
            [clojure.spec.alpha :as s]
            [relational-exploration.core :refer :all]))

(deftest test-where
  (let [data #{{:throughput 100 :run 0 :config :a}
               {:throughput 100 :run 1 :config :a}
               {:throughput 100 :run 2 :config :a}
               {:throughput 100 :run 0 :config :b}
               {:throughput 100 :run 1 :config :b}
               {:throughput 100 :run 2 :config :b}}]
    (is
     (= (into #{} (where {:config #{:b}}) data)
        #{{:throughput 100 :run 0 :config :b}
          {:throughput 100 :run 1 :config :b}
          {:throughput 100 :run 2 :config :b}}))

    (is
     (= (into #{} (where {:run #(< % 1)}) data)
        #{{:throughput 100 :run 0 :config :a}
          {:throughput 100 :run 0 :config :b}}))

    (is
     (= (into #{} (where {:config #{:a}
                          :run    #(< % 1)}) data)
        #{{:throughput 100 :run 0 :config :a}}))
    
    (is
     (= (into #{} (where {:config #{:a :b}}) data)
        data))

    (is
     (= (into #{} (where {:run (s/and even? pos?)}) data)
        #{{:throughput 100 :run 2 :config :a}
          {:throughput 100 :run 2 :config :b}}))

    (is
     (= (into #{}
              (comp
               (where {:config #{:b}})
               (where {:run zero?}))
              data)
        #{{:throughput 100 :run 0 :config :b}}))))

(deftest test-derive
  (let [data #{{:run 0 :config :a}
               {:run 1 :config :a}
               {:run 2 :config :a}
               {:run 0 :config :b}
               {:run 1 :config :b}
               {:run 2 :config :b}}]
    (is
     (= (into #{} (derive :foo (fn [_] "xyz")) data)
        #{{:run 0 :config :a :foo "xyz"}
          {:run 1 :config :a :foo "xyz"}
          {:run 2 :config :a :foo "xyz"}
          {:run 0 :config :b :foo "xyz"}
          {:run 1 :config :b :foo "xyz"}
          {:run 2 :config :b :foo "xyz"}}))

    (is
     (= (into #{}
              (comp
               (where {:run zero?})
               (derive :hash hash))
              #{{:run 0 :config :a}
                {:run 1 :config :a}})
        #{{:run 0 :config :a :hash 2037472533}}))))

(deftest test-derive-k
  (let [data #{{:tp 100 :run 0 :config :a}
               {:tp 100 :run 0 :config :b}
               {:tp 100 :run 1 :config :b}}]
    (is
     (= (into #{} (derive-k :name str [:config :run]) data)
        #{{:tp 100 :run 0 :config :a :name ":a0"}
          {:tp 100 :run 0 :config :b :name ":b0"}
          {:tp 100 :run 1 :config :b :name ":b1"}}))

    (is
     (= (into #{} (derive-k :tp2 #(* % %) [:tp]) data)
        #{{:tp 100 :run 0 :config :a :tp2 10000}
          {:tp 100 :run 0 :config :b :tp2 10000}
          {:tp 100 :run 1 :config :b :tp2 10000}}))

    (is
     (= (into #{}
              (comp
               (where {:config #{:a}})
               (derive :hash hash)
               (derive-k :tp2 #(* % %) [:tp]))
              data)
        #{{:tp 100 :run 0 :config :a :hash -463536415 :tp2 10000}}))))

(deftest test-relate
  (is
   (= (into #{} (relate :k [:a :b "c"]))
      #{{:k :a} {:k :b} {:k "c"}}))

  (is
   (= (into #{} (relate :idx (range 3) :y (range 3)))
      #{{:idx 0 :y 0}
        {:idx 1 :y 1}
        {:idx 2 :y 2}})))
