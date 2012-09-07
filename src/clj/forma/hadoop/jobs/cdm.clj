(ns forma.hadoop.jobs.cdm
  "Functions and Cascalog queries for converting data into map tile
coordinates."
  (:use [cascalog.api]
        [forma.source.gadmiso :only (gadm->iso)]
        [forma.gfw.cdm :only (latlon->tile, latlon-valid?, meters->maptile)]
        [forma.utils :only (positions)])
  (:require [forma.postprocess.output :as o]
            [forma.reproject :as r]
            [forma.date-time :as date]
            [cascalog.ops :as c]))

(defn split-line
  "Split a line of text using the supplied regular expression"
  [line re]
  (clojure.string/split line re))

(defbufferop min-period
  "Returns the minimum value in tuples."
  [tuples]
  [(reduce min (map first tuples))])

(defn first-hit
  "Returns the index of the first value in a vector of numbers that is
  greater than or equal to a threshold. `thresh` - The threshold
  value.  `series` - A vector of numbers.

  Example usage:
    (first-hit 5 [1 2 3 4 5 6 7 8 9 10]) => 4"
  [thresh series]
  (first (positions (partial <= thresh) series)))

(defn hansen-latlon->cdm
  "Returns a Cascalog query that transforms Hansen latlon data into map tile
  coordinates. `src` - The source tap.  `zoom` - The map zoom level.
  `tres` - The temporal resolution."
  [src zoom tres]
  (let [epoch (date/datetime->period tres "2000-01-01")
        hansen (date/datetime->period tres "2010-12-31")
        period (- hansen epoch)]
    (<- [?x ?y ?z ?p]
        (src ?line)
        (split-line ?line #"," :> ?lon-str ?lat-str _)
        ((c/each #'read-string) ?lat-str ?lon-str :> ?lat ?lon)
        (latlon-valid? ?lat ?lon) ;; Skip if lat/lon invalid.
        (identity period :> ?p)
        (latlon->tile ?lat ?lon zoom :> ?x ?y ?z))))

(defn hansen-xy->cdm
  "Returns a Cascalog query that transforms Hansen xy data into map tile
  coordinates. `src` - The source tap.  `zoom` - The map zoom level.
  `tres` - The temporal resolution."
  [src zoom tres]
  (let [epoch (date/datetime->period tres "2000-01-01")
        hansen (date/datetime->period tres "2010-12-31")
        period (- hansen epoch)]
    (<- [?x ?y ?z ?p]
        (src ?line)
        (split-line ?line #"," :> ?xm-str ?ym-str _)
        ((c/each #'read-string) ?xm-str ?ym-str :> ?xm ?ym)
        (identity period :> ?p)
        (meters->maptile ?xm ?ym zoom :> ?x ?y ?z))))

(defn forma->cdm
  "Returns a Cascalog generator that transforms FORMA data into map
    tile coordinates.  `start` - Estimation start period date string.
    `src` - The source tap for FORMA data.  `gadm-src` - a sequence
    file source with mod-h, mod-v, sample, line, and gadm. `thresh` -
    The threshold number for valid detections (0-100, integer).
    `tres` - The input temporal resolution (string).  `tres-out` - The
    output temporal resolution (string).  `zoom` - The map zoom
    level (integer).

  Example usage:
    (forma->cdm (hfs-seqfile \"/home/dan/Dropbox/local/output\")
                (hfs-seqfile \"/tmp/forma/data/gadm-path\")
                17
                \"16\"
                \"32\"
                \"2005-12-31\"
                50)"
  [src gadm-src zoom tres tres-out start thresh]
  (let [epoch (date/datetime->period tres-out "2000-01-01")
        start-period (date/datetime->period tres start)]
    (<- [?x ?y ?z ?p ?iso ?lat ?lon]
        (src ?sres ?modh ?modv ?s ?l ?prob-series)
        (gadm-src _ ?modh ?modv ?s ?l ?gadm)
        (gadm->iso ?gadm :> ?iso)
        (o/clean-probs ?prob-series :> ?clean-series)
        (first-hit thresh ?clean-series :> ?first-hit-idx)
        (+ start-period ?first-hit-idx :> ?period)
        (date/convert-period-res tres tres-out ?period :> ?period-new-res)
        (- ?period-new-res epoch :> ?rp)
        (min-period ?rp :> ?p)
        (r/modis->latlon ?sres ?modh ?modv ?s ?l :> ?lat ?lon)
        (latlon-valid? ?lat ?lon)
        (latlon->tile ?lat ?lon zoom :> ?x ?y ?z))))

(def prodes-classes-codes
  {1 "d1997_0" 2 "d1999_0" 3 "d2000_0" 4 "d2000_2" 5 "d2000_3"
   6 "d2001_0" 7 "d2001_3" 8 "d2001_4" 9 "d2002_0" 10 "d2002_1"
   11 "d2002_4" 12 "d2002_5" 13 "d2003_0" 14 "d2003_1" 15 "d2003_2"
   16 "d2003_5" 17 "d2003_6" 18 "d2004_0" 19 "d2004_1" 20 "d2004_2"
   21 "d2004_3" 22 "d2004_6" 23 "d2004_7" 24 "d2005_0" 25 "d2005_1"
   26 "d2005_2" 27 "d2005_3" 28 "d2005_4" 29 "d2005_7" 30 "d2006_0"
   31 "d2006_1" 32 "d2006_2" 33 "d2006_3" 34 "d2006_4" 35 "d2006_5"
   36 "d2006_6" 37 "d2006_7" 38 "d2007_0" 39 "d2007_1" 40 "d2007_2"
   41 "d2007_3" 42 "d2007_4" 43 "d2007_5" 44 "d2007_6" 45 "d2007_7"
   46 "d2008_0" 47 "d2008_1" 48 "d2008_2" 49 "d2008_3" 50 "d2008_4"
   51 "d2008_5" 52 "d2008_6" 53 "d2008_7" 54 "d2009_0" 55 "d2009_1"
   56 "d2009_2" 57 "d2009_3" 58 "d2009_4" 59 "d2009_5" 60 "d2009_6"
   61 "d2009_7" 62 "d2010_0" 63 "d2010_1" 64 "d2010_2" 65 "d2010_3"
   66 "d2010_4" 67 "d2010_5" 68 "d2010_6" 69 "d2010_7" 70 "d2011_0"})

(defn parse-prodes-code
  [prodes-class]
  {:pre [(contains? prodes-classes-codes prodes-class)]}
  (let [code (prodes-classes-codes prodes-class)]
    (->> code
        (rest)
        (apply str)
        (#(.split % "_"))
        (map #(Integer/parseInt %)))))

(defn year-lag->prodes-code
  [year lag]
  (str "d" year "_" lag))

(defn prodes-class->year
  [cls]
  (first (parse-prodes-code cls)))

(defn get-prodes-class
  ([yr]
     (get-prodes-class yr 0))
  ([yr lag]
     ((clojure.set/map-invert prodes-classes-codes)
      (year-lag->prodes-code yr lag))))

(defn prodes-interval
  "Given a PRODES class, return the start and end dates (exclusive) of
   that PRODES year. Lag classes are handled the same way as standard
   classes. That is, the returned interval will be the same.

   Usage:
     (prodes-interval 3)
     => [\"1999-09-01\" \"2000-09-01\"]"
  [prodes-class]
  {:pre [(contains? prodes-classes-codes prodes-class)]} 
  (let [y-end (prodes-class->year prodes-class)
        y-start (dec y-end)
        [m d] [9 1]]
    [(format "%d-%02d-%02d" y-start m d)
     (format "%d-%02d-%02d" y-end m d)]))

(defn date->prodes-year
  "For a given date, return the so-called 'PRODES year', which we define
   as starting Sept. 1 each year"
  [date-str]
  (let [yr (Integer/parseInt (date/convert date-str
                                           :year-month-day
                                           :year))
        prodes-class (get-prodes-class yr)
        [start end] (prodes-interval prodes-class)]
    (if (date/within-dates? start end date-str)
      yr
      (inc yr))))

(defn date->prodes-class
  "Returns the PRODES class for a given date."
  ([date-str]
     (date->prodes-class date-str 0))
  ([date-str lag]
     (get-prodes-class (date->prodes-year date-str) lag)))

