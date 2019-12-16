(ns relational-exploration.core
  "As Clojure programmers, transforming collections of maps is
  our bread and butter. clojure.set encourages us to treat such
  collections as relations. Plenty of benefits await:

  1. Bring strong keywords front and center, helping clarity
  2. Maintain provenance throughout the transformation
  3. Decomplect computation from relational structure
  4. Avoid join-like operations with implicit ordering dependencies"
  (:refer-clojure :exclude [derive])
  (:require [clojure.spec.alpha :as s]))

(defn matches-specmap? [specmap m]
  (reduce-kv
   (fn [_ k spec]
     (if (s/valid? spec (get m k))
       true
       (reduced false)))
   true
   specmap))

(defn where
  "Returns a set of the elements conforming to the given spec-map. A
  spec-map is a way to quickly combine specs into a tuple filter.

  Example
  =======
  {:x #(= % 10) :y #(< % 100)} matches all tuples where (= x 10) and (< y 100)."
  ([specmap]
   (filter (partial matches-specmap? specmap)))
  ([specmap xset]
   (filter (partial matches-specmap? specmap) xset)))

(defn derive
  "Maps a tuple operation over the relation, associng the result as a
  new attribute. This allows us to non-destructively derive
  information from tuples using regular, 'destructive' functions."
  ([dest-attr f]
   (map (fn [tuple] (assoc tuple dest-attr (apply f [tuple])))))
  ([dest-attr f xrel & args]
   (->> xrel
        (map (fn [tuple] (assoc tuple dest-attr (apply f tuple args))))
        (into #{}))))

(defn derive-k
  "Corresponds to SELECT F(key, key2, ...)"
  ([dest-attr f ks]
   (map (fn [tuple]
          (let [param-fn (apply juxt ks)
                ;; @TODO combine juxt with flatten? then a single key-fn may return more than one parameter
                params   (param-fn tuple)]
            (assoc tuple dest-attr (apply f params))))))
  ([dest-attr f ks xrel]
   (into #{} (derive-k dest-attr f ks) xrel)))

(defn relate
  "Lift unkeyed data into a relation."
  [& pairs]
  (assert (even? (count pairs)) "relate requires an even number of arguments")
  (->> pairs
       (partition 2)
       (map (fn [[k vs]] (map #(hash-map k %) vs)))
       (apply map merge)))

;; e.g.
;; (relate :k [:a :b "c"])
;; (relate :idx (range 10) :y (repeatedly 10 rand))



;; EXAMPLES

;; decouple ation from tuple structure
(let [data (relate :t (range 100) :n (repeatedly 100 #(rand-int 1000)) :y (repeatedly 100 #(rand-int 100)))]
  (let [total (apply + (map :n data))]
    (assert
     (= (into #{} (map #(assoc % :weight (/ (:n %) total)) data))
        (derive :weight #(/ (:n %) total) data)
        (derive-k :weight / [:n (constantly total)] data)))))

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
  (derive :url get-url playables)
  (derive-k :url get-path [:name] playables))

