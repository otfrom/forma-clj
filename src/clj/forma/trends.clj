;; TODO: -- talk about our assumptions on what data's coming in to
;; here.  This is where we'll be implementing OLS, using either infer
;; or incanter on the resulting matrices. Again, we should make sure
;; that we're operating on vectors, not seqs. (Turn a seq into a
;; vector be calling vec on it.)

(ns forma.trends
  (:use cascalog.api)
  (:require [forma.hadoop.predicate :as p]
            [cascalog.ops :as c]
            [cascalog.vars :as v]))

;; ### Time Series Generation
;;
;; The goal here is to write an aggregator that takes in a sequence of
;; tuples of the form <tperiod, pixel-vector>, and returns tuples of
;; the form <pixel-index, min-time, max-time, timeseries>.
;;
;; We assume that we're receiving chunks for every month within the
;; range. We measure pixel-index as the position inside the
;; chunk. For example:
;;
;;     (timeseries [[1 [7 4 2 1]]
;;                  [2 [1 2 3 4]]
;;                  [3 [3 4 3 2]]
;;                  [4 [4 8 7 5]]
;;                  [5 [1 5 3 2]]])
;;     ;=> ([0 1 5 [7 1 3 4 1]]
;;          [1 1 5 [4 2 4 8 5]]
;;          [2 1 5 [2 3 3 7 3]]
;;          [3 1 5 [1 4 2 5 2]])
;;
;; Let's define a function to give us chunks to work with. (This
;; should really be in a separate test file... that's the next
;; project.)

(defn test-chunks
  "Returns a sample input to the timeseries creation buffer, or a
  sequence of 2-tuples, structured as <period, int-array>. Each
  int-array is sized to `chunk-size`; the returned sequence contains
  tuples equal to the supplied value for `periods`."
  [periods chunk-size]
  (for [period (range periods)]
    (vector period
            (int-array chunk-size
                       (range chunk-size)))))

;; Here's my first try at timeseries:
;;
;;     (defn timeseries [tuples]
;;       (let [transpose #(apply map vector %)
;;             [periods chunks] (transpose tuples)]
;;         (map-indexed #(vector %1
;;                               (first periods)
;;                               (last periods)
;;                               (int-array %2))
;;                      (transpose chunks))))
;;
;; One issue with this is that we're mapping through the data three
;; times, which is inefficient. we do a first transpose to get
;; separate the data into periods and chunks -- we can see this, in
;; our destructuring vector, inside the `let` form. We map again to
;; transpose the chunks into timeseries, then again over these to
;; build our results tuples. I tried the following to see the speed
;; difference gained by removing one of these maps:
;;
;;     (defn timeseries [tuples]
;;       (let [[periods chunks] (apply map vector tuples)
;;             periodize (partial vector
;;                                (first periods)
;;                                (last periods))
;;             tupleize (comp periodize int-array vector)]
;;         (map-indexed cons (apply map tupleize chunks))))
;;
;; In this version, we gain about 15% speed. benchmarked with
;;
;;     (def chunks (test-chunks 130 24000))
;;     (time (dotimes [n 10] (count (timeseries chunks))))
;;
;; I get a bit under 24 seconds for the first version, a bit over 20
;; for the second version. For hadoop, we'll go with the more
;; efficient one! I think it ends up looking nicer, too.
;;
;; To be honest, I'm not really sure why this is faster. I had a hunch
;; that moving the call to `tupleize` inside of map (previously, we
;; were using `transpose` here, then `map-indexed`) would speed things
;; up, and it did.

(defbufferop
  ^{:doc "Takes in a number of <t-period, modis-chunk> tuples, sorted
  by time period, and transposes these into (n = chunk-size) 4-tuples,
  formatted as <pixel-idx, t-start, t-end, t-series>, where the
  `t-series` field is represented by an int-array. Entering chunks
  should be sorted in descending order."}
  timeseries [tuples]
  (let [[periods chunks] (apply map vector tuples)
        periodize (partial vector
                           (first periods)
                           (last periods))
        tupleize (comp periodize int-array vector)]
    (map-indexed cons (apply map tupleize chunks))))

;; [This gist](https://gist.github.com/845813) is a solid example of
;; how to get cascalog to sort by time period and provide tuples to
;; our final `timeseries` function. Note again that incoming chunks
;; will be either float or int arrays.

;; ## Walking the Matrix

(defbufferop tuples->string
  {:doc "Returns a string representation of the tuples input to this
  buffer. Useful for testing!"}
  [tuples]
  [(apply str (map str tuples))])

;; Generates combinations of `mod-h`, `mod-v`, `sample` and `line` for
;; use in buffers.
(def points
  (memory-source-tap
   (for [mod-h  (range 3)
         sample (range 20)
         line   (range 20)
         :let [val sample, mod-v 1]]
     [mod-h mod-v sample line val])))

(def points-plus
  (memory-source-tap
   (for [mod-h  (range 3)
         sample (range 20)
         line   (range 20)
         :let [val sample
               mod-v 1]]
     [mod-h mod-v sample line val])))


(defn vec-of
  "Returns a vector of `length` filled with `val`."
  [length val]
  (vec (repeat length val)))

;; This is what we're mimicking.

(defn sample-aggregator
  "Takes a samples and line generator, and stitches lines back
  together. "
  [point-source edge splits]
  (let [sample-agger (p/vals->sparsevec edge splits 0)]
    (<- [?mod-h ?mod-v ?line ?line-vec-col ?line-vec]
        (point-source ?mod-h ?mod-v ?sample ?line ?val)
        (sample-agger ?sample ?val :> ?line-vec-col ?line-vec))))

(defn window-aggregator
  "Stitches lines back together into little windows."
  [point-source edge splits]
  (let [line-source (sample-aggregator point-source edge splits)
        line-agger (p/vals->sparsevec edge
                                      splits
                                      (vec-of (/ edge splits) 0))]
    (<- [?mod-h ?mod-v ?window-col ?window-row ?window]
        (line-source ?mod-h ?mod-v ?line ?window-col ?line-vec)
        (line-agger ?line ?line-vec :> ?window-row ?window))))

;; Or, a bit more condensed...
;; 
(defn window-aggregator
  "Stitches lines back together into little windows."
  ([point-source] (window-aggregator point-source 20 4))
  ([point-source edge splits]
     (let [sample-agger (p/vals->sparsevec edge splits 0)
           line-agger  (p/vals->sparsevec edge
                                          splits
                                          (vec-of (/ edge splits) 0))
           line-source (<- [?mod-h ?mod-v ?line ?window-col ?line-vec]
                           (point-source ?mod-h ?mod-v ?sample ?line ?val)
                           (sample-agger ?sample ?val :> ?window-col ?line-vec))]
       (<- [?mod-h ?mod-v ?window-col ?window-row ?window]
           (line-source ?mod-h ?mod-v ?line ?window-col ?line-vec)
           (line-agger ?line ?line-vec :> ?window-row ?window)))))

;;(?- (stdout) (window-aggregator points 20 4))

(def key-tap
  (<- [?mh ?mv ?s ?l ?v]
      (points-plus ?mh ?mv ?s ?l ?v)))

(defn mk-vars
  "Generates the three stages of var needed by build-windows."
  [gen symbols [c-sym r-sym v-sym :as new-syms]]
  (let [src-vars (replace (zipmap symbols new-syms)
                          (get-out-fields gen))]
    [src-vars
     (replace (zipmap new-syms [r-sym "?win-col" "?row-vec"])
              src-vars)
     (replace (zipmap new-syms ["?win-col" "?win-row" "?window"])
              src-vars)]))

(defn build-windows
  "Accepts a cascalog generator, and a vector of keys corresponding to the "
  [gen [col row val :as symbols] edge splits empty-val]
  (let [[c-sym r-sym v-sym :as new-syms] (v/gen-nullable-vars 3)
        [src-vars int-vars out-vars] (mk-vars gen symbols new-syms)
        col-aggr (p/vals->sparsevec edge splits empty-val)
        row-aggr (p/vals->sparsevec edge
                                    splits
                                    (vec-of (/ edge splits) 0))        
        row-source (construct int-vars
                              [(into [gen] src-vars)
                               [col-aggr c-sym v-sym :> "?win-col" "?row-vec"]])]
    (construct out-vars
               [(into [row-source] int-vars)
                [row-aggr r-sym "?row-vec" :> "?win-row" "?window"]])))

;; THIS CURRENTLY WORKS
(defn mk-vars
  "Generates the three stages of var needed by build-windows."
  [gen in-syms int-sym out-syms]
  (let [src-vars (get-out-fields gen)]
    [src-vars
     (replace (zipmap in-syms [(in-syms 1) (out-syms 0) int-sym])
              src-vars)
     (replace (zipmap in-syms [(out-syms 0) (out-syms 1) (out-syms 2)])
              src-vars)]))

(defn build-windows
  "Accepts a cascalog generator, and a vector of keys corresponding to the "
  [gen in-syms edge splits empty-val]
  (let [[int-sym] (v/gen-non-nullable-vars 1)
        out-syms (v/gen-non-nullable-vars 3)
        [src-vars int-vars out-vars] (mk-vars gen in-syms int-sym out-syms)
        col-aggr (p/vals->sparsevec edge splits empty-val)
        row-aggr (p/vals->sparsevec edge splits (vec-of (/ edge splits) empty-val))
        row-source (construct int-vars
                              [(into [gen] src-vars)
                               [col-aggr (in-syms 0) (in-syms 2) :> (out-syms 0) int-sym]])
        win-source (construct out-vars
                              [(into [row-source] int-vars)
                               [row-aggr (in-syms 1) int-sym :> (out-syms 1) (out-syms 2)]])]
    win-source))
