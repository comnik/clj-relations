(ns relation-utils.core
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

(defn reduce-by
  "Like reduce, but allows for a separate accumulator per
  group.

  Threading becomes a mess, whenever the structure of the data changes
  during the transform. This is especially annoying with
  groupings. But we can do better in many cases, when we are not
  interested in a specific group, but rather in processing all of
  them.

  In such a case we can reduce over the relation and keep one
  accumulator per group."
  [key-fn xf init-fn xrel]
  (persistent!
   (reduce (fn [accs next]
             (let [key (key-fn next)
                   acc (or (get accs key) (init-fn key))]
               (assoc! accs key (xf acc next))))
           (transient {})
           xrel)))

(defn select-complement
  "Returns a rel of the elements of xrel with keys in ks
  dissoced. Inverse of clojure.core/select-keys."
  [map keyseq]
  (apply dissoc map keyseq))

(defn project-out
  "Given a relation `xrel` with free variables V ∪ {k}, returns a map
  from tuples projected to V∖{k} to bound values of k. Duplicates are
  merged using the provided `fadd` function."
  ([k xrel] (project-out + k xrel))
  ([fadd k xrel]
   (persistent!
    (reduce (fn [accs next]
              (let [key (select-complement next [k])
                    val (get next k)
                    acc (or (get accs key) (fadd))]
                (assoc! accs key (fadd acc val))))
            (transient {})
            xrel))))

(comment
  
  (let [data #{{:tp 100 :run 0 :config :a}
               {:tp 100 :run 1 :config :a}
               {:tp 100 :run 2 :config :a}
               {:tp 100 :run 0 :config :b}
               {:tp 100 :run 1 :config :b}
               {:tp 100 :run 2 :config :b}
               {:tp 2000 :run 0 :config :a}}]
    (time
     (doseq [i (range 10000)]
       (reduce-by #(select-keys % [:run :config])
                  (fn [sum t] (+ sum (:tp t)))
                  (constantly 0)
                  data)))

    (time
     (doseq [i (range 10000)]
       (project-out + :tp data)))

    (time
     (doseq [i (range 10000)]
       (->> data             
             (group-by #(select-keys % [:run :config]))
             (reduce-kv
              (fn [agg key tuples]
                (assoc! agg key (apply + (map :tp tuples))))
              (transient {}))
             (persistent!)))))
  )

(comment

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
  )
