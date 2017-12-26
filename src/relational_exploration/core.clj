;; Relational Operations
;; =====================

;; As Clojure programmers, transforming collections of maps is
;; our bread and butter. clojure.set encourages us to treat such
;; collections as relations. Plenty of benefits await:
;;
;;   1. Bring strong keywords front and center, helping clarity
;;   2. Maintain provenance throughout the transformation
;;   3. Decomplect computation from relational structure
;;   4. Avoid join-like operations with implicit ordering dependencies


(ns relational-exploration.core
  (:use [clojure.set])
  (:require [clojure.pprint :as pprint]))


(def data
  #{{:throughput 100 :run 0 :config :a}
    {:throughput 100 :run 1 :config :a}
    {:throughput 100 :run 2 :config :a}
    {:throughput 100 :run 0 :config :b}
    {:throughput 100 :run 1 :config :b}
    {:throughput 100 :run 2 :config :b}})

(def data-1
  (rename data {:throughput :tp}))

(select #(= (:config %) :a) data-1)

(select #(< (:run %) 1) data-1)


;; we introduce the concept of a predicate-map (predmap)
;; a simple example: {:x 10 :y #(< % 100)}
;; this predicate map matches all tuples where (= x 10) and (< y 100)

(defn matches-predmap? [predmap tuple]
  (reduce-kv
   (fn [_ k v]
     (let [v'       (get tuple k)
           matches? (if (fn? v) (apply v [v']) (= v v'))]
       (if matches? true (reduced false))))
   true
   predmap))


(defn where
  "Returns a set of the elements matching the given predicate map."
  [predmap xset]
  (select #(matches-predmap? predmap %) xset))

(comment
  (where {:config :a} data-1)
  (where {:run #(< % 1)} data-1))


(project data-1 [:config :run :tp])

;; operations on tuples shouldn't remove information from them
(defn compute-stddev [tuple] (assoc tuple :stddev (rand)))

;; this way we can infer new facts non-destructively

(def data-2 (map compute-stddev data-1))

(project data-2 [:tp :stddev])


;; we must also provide a way to lift data into a relation
;; the goal of this is to put keywords front and center
;; use join additionally, whenever a single relation's paths have diverged

(defn derive
  "Maps a tuple operation over the relation, associng the result as a new attribute."
  [xrel dest-attr f & args]
  (->> xrel
       (map #(assoc % dest-attr (apply f % args)))
       (into #{})))

(defn derive2
  "Maps a function which is unaware of tuples over a relation, associng
   the result as a new attribute."
  [xrel dest-attr f & ks]
  (->> xrel
       (map #(assoc % dest-attr (apply f ((apply juxt ks) %))))
       (into #{})))

;; one might even combine juxt with flatten, such that a single key-fn
;; may return more than one paramater

(comment
  (-> data-1
      (project [:config :run])
      (derive :foo (fn [_] (rand-int 100)))
      (join data-1))

  (-> data-1
      (project [:config :run])
      (derive2 :combined-name str :config :run)
      (join data-1)))


(defn relate [& pairs]
  (assert (even? (count pairs)) "relate requires an even number of arguments")
  (->> pairs
       (partition 2)
       (map (fn [[k vs]] (map #(hash-map k %) vs)))
       (apply map merge)))

(comment
  (relate :k [:a :b "c"])
  (relate :idx (range 10) :y (repeatedly 10 rand)))


;; decouple computation from tuple structure
(let [data (relate :t (range 100) :n (repeatedly 100 #(rand-int 1000)) :y (repeatedly 100 #(rand-int 100)))]
  (let [total (apply + (map :n data))]
    (assert
     (= (into #{} (map #(assoc % :weight (/ (:n %) total)) data))
        (derive data :weight #(/ (:n %) total))
        (derive2 data :weight / :n (constantly total))))))

;; a cleaner zipmap
(let [urls ["test.wav" "bla.mp3" "skrra.wav" "weeee.ogg"]
      ids  (range)]
  ;; traditional
  (map vector ids urls)
  ;; relational
  (relate :id ids :url urls))

;; maintain provenance
(let [get-url   (fn [playable] (str "/storage/" (:name playable) ".mp3"))
      get-path  (fn [name] (str "/storage/" name ".mp3"))
      playables (relate :name ["name-a" "name-b" "name-c"])]

  ;; usual approach, relying on implicit order
  (let [urls (map get-url playables)]
    (for [[playable url] (map vector playables urls)]
      [playable url]))

  ;; usual approach, with non-tuple-aware function
  (let [urls (map (comp get-url :name) playables)]) ;...

  ;; traditional approach w/o destroying information
  (map #(assoc % :url (get-url %)) playables)
  
  ;; relational approach, w and w/o tuple-aware function
  (derive playables :url get-url)
  (derive2 playables :url get-path :name))

