(ns forma.ops.classify-test
  "This namespace tests the wrappers around the logistic classifier
  functions that generate the coefficient vectors based on the
  training data."
  (:use forma.ops.classify :reload)
  (:use cascalog.api
        [midje sweet cascalog])
  (:require [forma.testing :as t]
            [forma.date-time :as date]
            [forma.thrift :as thrift]))

(def test-map {:convergence-thresh 1.0E-6
               :ridge-const 1.0E-8
               :t-res "16"
               :est-start "2005-12-31"
               :max-iterations 500
               :nodata -9999.0})

(fact "Test the creation of a feature vector from a FormaValue and
  NeighborValue."
  (let [fire-val (thrift/FireValue* 1 1 1 1)
        forma-val (thrift/FormaValue* fire-val 3.0 3.0 3.0 3.0)
        neighbor-val (thrift/NeighborValue* fire-val 1 2.0 3.0 4.0 5.0 6.0 7.0)
        unpacked (unpack-feature-vec forma-val neighbor-val)]
    unpacked => [1 1 1 1 1 3.0 3.0 3.0 3.0 1 1 1 1 2.0 3.0 4.0 5.0 6.0 7.0 0.0 0.0]))

(fact
  "Test `parse-pixels`, ensuring that no feature vector containing
   `nodata` is returned"
  (let [nodata (:nodata test-map)
        label 1
        fire-val (thrift/FireValue* 1 1 1 1)
        forma-val (thrift/FormaValue* fire-val 3.0 3.0 3.0 3.0)
        bad-forma (thrift/FormaValue* fire-val 3.0 3.0 nodata 3.0)
        neighbor-val (thrift/NeighborValue* fire-val 1 2.0 3.0 4.0 5.0 6.0 7.0)
        tuples [[label forma-val neighbor-val]
                [label bad-forma neighbor-val]]]
    (parse-pixels nodata tuples) => [[label (unpack-feature-vec forma-val neighbor-val)]]))

(fact
  "Test `logistic-beta-wrap`"
  (let [nodata (:nodata test-map)
        fire-val (thrift/FireValue* 1 1 1 1)
        forma-val (thrift/FormaValue* fire-val 3.0 3.0 3.0 3.0)
        bad-forma (thrift/FormaValue* fire-val 3.0 3.0 nodata 3.0)
        neighbor-val (thrift/NeighborValue* fire-val 1 2.0 3.0 4.0 5.0 6.0 7.0)
        {:keys [ridge-const convergence-thresh max-iterations]} test-map
        src [[1 0 forma-val neighbor-val]
             [1 1 forma-val neighbor-val]
             [1 1 bad-forma neighbor-val]]]
    (<- [?id ?beta]
        (src ?ecoid ?hansen ?val ?neighbor-val)
        (logistic-beta-wrap [nodata ridge-const convergence-thresh max-iterations]
                            ?hansen ?val ?neighbor-val :> ?beta)))
  => (produces [1]))

(defn- generate-betas
  "Returns a source of the estimated coefficient vectors for each
  ecoregion (and spatial resolution, but this is ignored here)."
  [{:keys [convergence-thresh t-res ridge-const est-start  max-iterations]} src]
  (let [first-idx (date/datetime->period t-res est-start)]
    (<- [?s-res ?eco ?beta]
        (src ?s-res ?pd ?mod-h ?mod-v ?s ?l ?val ?neighbor-val ?eco ?hansen)
        (logistic-beta-wrap [ridge-const convergence-thresh max-iterations]
                                ?hansen ?val ?neighbor-val :> ?beta)
        (:distinct false))))
