(ns forma.hadoop.jobs.cdm-test
  "This namespace defines Midge tests for the forma.hadoop.jobs.cdm
namespace."
  (:use [midje sweet]
        forma.hadoop.jobs.cdm
        cascalog.api
        midje.cascalog))

;; TODO: Fix this. See: https://github.com/reddmetrics/forma-clj/issues/160
;; (tabular
;;  (fact "Test hansen-latlon->cdm function.  A valid lat-lon should produce the
;; specified row; but an invalid lat-lon will not produce a row."
;;    (hansen-latlon->cdm ?src ?zoom ?tres) => (produces ?row))
;;  ?src ?zoom ?tres ?row 
;;  [["-87.65005229999997,41.850033,1"]] 16 "32" [[16811 24364 16 131]]
;;  [["2000,2000,1"]] 16 "32" [])

(fact "Test that the output of `forma->cdm` correspond to the proper
common data model for upload into CartoDB.  Note that only the pixels
with ?sample equal to 422 and 423 are returned, since they are the
only pixels with smoothed probabilities that exceed the threshold of
`50`. Note also that the period of first-alert is later for 422 than
for 423, since the probabilities are lower -- taking more time to
register as an alert, given the smoothing of the raw probability
series."
  (let [raw-output [["500" 28 8 420 2182 [0.0002 0.002 0.02 0.2 0.2 0.2]]
                    ["500" 28 8 421 2182 [0.0004 0.004 0.04 0.4 0.4 0.4]]
                    ["500" 28 8 422 2182 [0.0006 0.006 0.06 0.6 0.6 0.6]]
                    ["500" 28 8 423 2182 [0.0008 0.008 0.08 0.8 0.8 0.8]]]
        gadm-src   [["500" 28 8 420 2182 15614]
                    ["500" 28 8 421 2182 15614]
                    ["500" 28 8 422 2182 15614]
                    ["500" 28 8 423 2182 15614]]]
    (forma->cdm raw-output gadm-src 17 "16" "32" "2005-12-31" 50)
    => (produces [[102590 65206 17 74 "IDN" 0.9062499999999973 101.77314715106174]
                  [102591 65206 17 73 "IDN" 0.9062499999999973 101.7773143389889]])))

(fact
  "Check parse-prodes-id."
  (parse-prodes-id 3) => [2000 0]
  (parse-prodes-id 22) => [2004 6]
  (parse-prodes-id 70) => [2011 0])

(fact
  "Check year-lag->prodes-code."
  (year-lag->prodes-code 2000 0) => "d2000_0"
  (year-lag->prodes-code 2000 2) => "d2000_2"
  (year-lag->prodes-code 2010 6) => "d2010_6")

(fact
  "Check prodes-id->year. Correctly handles ids even when using lag id."
  (prodes-id->year 3) => 2000
  (prodes-id->year 68) => 2010
  (prodes-id->year 62) => 2010)

(fact
  "Check get-prodes-id"
  (get-prodes-id 2000) => 3
  (get-prodes-id 2000 0) => 3
  (get-prodes-id 2000 2) => 4)

(fact
  "Check prodes-interval. Intervals are correct for given PRODES id, and
   lag classes are handled the same way as 'normal' ids."
  (prodes-interval 3) => ["1999-09-01" "2000-09-01"]
  (prodes-interval 62) => ["2009-09-01" "2010-09-01"]
  (prodes-interval 68) => ["2009-09-01" "2010-09-01"])

(fact
  "Check date->prodes-year. PRODES year is calculated correctly for dates just before
   and after beginning of year."
  (date->prodes-year "2005-12-31") => 2006
  (date->prodes-year "2006-06-30") => 2006
  (date->prodes-year "2006-08-31") => 2006
  (date->prodes-year "2006-09-01") => 2007)

(fact
  "Check date->prodes-id. PRODES id is correct for dates with and without lags."
  (date->prodes-id "2000-01-01") => 3
  (date->prodes-id "2000-01-01" 2) => 4
  (date->prodes-id "2010-01-01" 6) => 68
  (date->prodes-id "2010-01-01" 8) => nil
  (date->prodes-id "2015-01-01") => (throws AssertionError))

(fact
  "Check extract-forma. FORMA values are extracted correctly for PRODES year."
  (let [tres "16" 
        src [["500" 12 8 0 0 (vec (range 150))]]]
    (<- [?sres ?modh ?modv ?s ?l ?years ?vals]
          (src ?sres ?modh ?modv ?s ?l ?prob-series)
          (extract-forma tres ?prob-series :> ?years ?vals))) =>
          (produces [["500" 12 8 0 0 2006 15]
                     ["500" 12 8 0 0 2007 38]
                     ["500" 12 8 0 0 2008 61]
                     ["500" 12 8 0 0 2009 84]
                     ["500" 12 8 0 0 2010 107]
                     ["500" 12 8 0 0 2011 130]]))

(fact
  "Check mk-prodes-map."
  (mk-prodes-map [3 4] [10 15]) => {2 15, 0 10})

(fact
  "Check prodes-map->wide."
  (prodes-map->wide {2 15, 0 10}) => [10 0 15 0 0 0 0 0])

(fact
  (let [src [[12 8 0 0 3 20]
             [12 8 0 0 4 25]
             [12 8 0 0 5 21]]]
        (<- [?modh ?modv ?sample ?line ?p0 ?p1 ?p2 ?p3 ?p4 ?p5 ?p6 ?p7]
            (src ?modh ?modv ?sample ?line ?prodes-id ?ct)
            (prodes-id->year ?prodes-id :> ?year)
            (prodes-wide ?prodes-id ?ct :> ?p0 ?p1 ?p2 ?p3 ?p4 ?p5 ?p6 ?p7))) => (produces [[12 8 0 0 20 0 25 21 0 0 0 0]]))

(fact
  "Check in-training?"
  ;; in training, no lag
  (in-training? [2003 2003] "2000-01-01" "2005-12-31") => true

  ;; inside training, lag inside training
  (in-training? [2004 2002] "2000-01-01" "2005-12-31") => true

  ;; inside training, lag outside training
  (in-training? [2003 1998] "2000-01-01" "2005-12-31") => false

  ;; outside training, no lag
  (in-training? [2008 2008] "2000-01-01" "2005-12-31") => false

  ;; outside training, lag inside training
  (in-training? [2008 2004] "2000-01-01" "2005-12-31") => false

  ;; inside training, no lag, stretched training period to include 2006
  (in-training? [2006 2006] "2000-01-01" "2006-09-02") => true)

(fact
  "Check prodes-sum-training"
  (let [src [["2000-01-01" "2005-12-31" 9 10]
             ["2000-01-01" "2005-12-31" 69 10]
             ["2000-01-01" "2005-12-31" 21 10]]]
    (<- [?sum]
          (src ?start ?end ?id ?ct)
          (prodes-sum-training ?start ?end ?id ?ct :> ?sum))) => (produces [[20]]))

(fact
  "Check tweak-training"
  (tweak-training "2000-02-18" "2005-12-31") => ["2000-09-01" "2005-09-01"]
  (tweak-training "2000-02-18" "2005-12-31" :extend-start true) => ["1999-09-01" "2005-09-01"]
  (tweak-training "2000-02-18" "2005-12-31" :extend-end true) => ["2000-09-01" "2006-09-01"]
  (tweak-training "2000-02-18" "2005-12-31" :extend-start true :extend-end true) => ["1999-09-01" "2006-09-01"])

(fact
  "Check prodes-wide."
  (let [prodes-src [[12 8 0 0 3 20] ;; training drop
                    [12 8 0 0 4 20] ;; training drop
                    [12 8 0 0 68 5] ;; 2010 lag 6
                    [12 8 0 0 55 5] ;; 2009 lag 1
                    [12 8 0 1 3 20] ;; training drop
                    [12 8 0 1 68 9] ;; 2010 lag 6
                    [12 8 0 1 30 30] ;; 2006 lag 0
                    [12 8 0 1 31 30]]] ;; 2006 lag 1]]
    (prodes-wide-query prodes-src)) => (produces [[12 8 0 0 2009 0 5 0 0 0 0 0 0] [12 8 0 0 2010 0 0 0 0 0 0 5 0] [12 8 0 1 2006 30 30 0 0 0 0 0 0] [12 8 0 1 2010 0 0 0 0 0 0 9 0]]))

(fact
  "Check forma-extract-query"
  (let [forma-src [(into ["500" 12 8 0 0] [(vec (map #(/ % 100.0) (range 1 150)))])
                   (into ["500" 12 8 0 2] [(vec (map #(/ % 100.0) (range 1 150)))])]]
    (forma-extract-query
     "16" forma-src)) => (produces [[12 8 0 0 2006 15]
                                    [12 8 0 0 2007 38]
                                    [12 8 0 0 2008 61]
                                    [12 8 0 0 2009 84]
                                    [12 8 0 0 2010 107]
                                    [12 8 0 0 2011 130]
                                    [12 8 0 2 2006 15]
                                    [12 8 0 2 2007 38]
                                    [12 8 0 2 2008 61]
                                    [12 8 0 2 2009 84]
                                    [12 8 0 2 2010 107]
                                    [12 8 0 2 2011 130]]))

(comment
  (let [t-res "16"
        train-start "2000-01-01"
        est-start "2005-12-31"
        px ["500" 12 8 0 0]
        forma-src [(into px [(vec (map #(/ % 100.0) (range 1 150)))])]
        static-src [(into px [25 1000 3000 100 21])]
        prodes-src [[28 8 0 1 3 21] ;; test ungrounding
                    (into (vec (rest px)) [3 21])
                    (into (vec (rest px)) [4 22])
                    (into (vec (rest px)) [5 23])
                    (into (vec (rest px)) [9 24])
                    (into (vec (rest px)) [20 25])]]
    (forma-prodes t-res forma-src static-src prodes-src est-start
                  "2000-02-18" "2005-12-31") => (produces-some
                                         [["500" 12 8 0 0 100 25 1000 3000 1 1 2006 15 49 24 0 25 23 0 0 0 0]
                                          ["500" 12 8 0 0 100 25 1000 3000 1 1 2007 38 49 24 0 25 23 0 0 0 0]])
     (forma-prodes t-res forma-src static-src prodes-src est-start
                   "2000-02-18" "2005-12-31" :extend-start false
                   :extend-end true) => (produces-some
                                         [["500" 12 8 0 0 100 25 1000 3000 1 16 2006 15 49 24 0 25 23 0 0 0 0]
                                          ["500" 12 8 0 0 100 25 1000 3000 1 16 2007 38 49 24 0 25 23 0 0 0 0]]
                                         ))
  )
