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
  "Check that PRODES codes are parsed correctly"
  (parse-prodes-code 3) => [2000 0]
  (parse-prodes-code 22) => [2004 6]
  (parse-prodes-code 70) => [2011 0])

(fact
  "Check that year/lag parsing produces correct PRODES codes"
  (year-lag->prodes-code 2000 0) => "d2000_0"
  (year-lag->prodes-code 2000 2) => "d2000_2"
  (year-lag->prodes-code 2010 6) => "d2010_6")

(fact
  "Check that prodes-class->year handles classes correctly even when
   using lag class."
  (prodes-class->year 3) => 2000
  (prodes-class->year 68) => 2010
  (prodes-class->year 62) => 2010)

(fact
  "Check that intervals are correct for given PRODES class, and that
   lag classes are handled the same way as 'normal' classes."
  (prodes-interval 3) => ["1999-09-01" "2000-09-01"]
  (prodes-interval 62) => ["2009-09-01" "2010-09-01"]
  (prodes-interval 68) => ["2009-09-01" "2010-09-01"])

(fact
  "Check that PRODES year is calculated correctly for dates just before
   and after beginning of year."
  (date->prodes-year "2005-12-31") => 2006
  (date->prodes-year "2006-06-30") => 2006
  (date->prodes-year "2006-08-31") => 2006
  (date->prodes-year "2006-09-01") => 2007)

(fact
  "Check that PRODES class is correct for dates with and without lags."
  (date->prodes-class "2000-01-01") => 3
  (date->prodes-class "2000-01-01" 2) => 4
  (date->prodes-class "2010-01-01" 6) => 68
  (date->prodes-class "2010-01-01" 8) => nil
  (date->prodes-class "2015-01-01") => (throws AssertionError))
