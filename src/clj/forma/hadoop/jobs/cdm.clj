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

(def prodes-count-start
  "Year to start reporting separate PRODES counts"
  2006)

(def prodes-count-end
  "Year to stop reporting separate PRODES counts."
  2011)

(def prodes-year-month
  "Month that starts the PRODES year"
  9)

(def prodes-year-day
  "Day of the month that starts the PRODES year"
  1)

(def prodes-ids-codes
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

(defn parse-prodes-id
  "Convert a PRODES id into the corresponding year and lag.

   Usage:
     (parse-prodes-id 3)
     => (2000 0)

     (parse-prodes-id 68)
     => (2010 6)"
  [prodes-id]
  (let [code (prodes-ids-codes prodes-id)]
    (if code
      (->> code
        (rest)
        (apply str)
        (#(.split % "_"))
        (map #(Integer/parseInt %))))))

(defn year-lag->prodes-code
  "Given a year and lag, return the corresponding PRODES code.

   Usage:
     (year-lag->prodes-code 2000 0)
     => \"d2000_0\"

     (year-lag->prodes-code 2010 3)
     => \"d2010_3\""
  [year lag]
  (str "d" year "_" lag))

(defn prodes-id->year
  "Given a PRODES class, return the year.

   Usage:
     (prodes-id->year 3)
     => 2000"
  [cls]
  (first (parse-prodes-id cls)))

(defn get-prodes-id
  "For a given year and (optionally) lag, return the appropriate PRODES id.
   Default lag is 0.

   Usage:
     (get-prodes-id 2000)
     => 3

     (get-prodes-id 2000 0)
     => 3

     (get-prodes-id 2000 2)
     => 4"
  ([yr]
     (let [default-lag 0]
       (get-prodes-id yr default-lag)))
  ([yr lag]
     ((clojure.set/map-invert prodes-ids-codes)
      (year-lag->prodes-code yr lag))))

(defn prodes-interval
  "Given a PRODES id or year, return the start and end dates (exclusive) of
   that PRODES year. Lag ids are handled the same way as standard
   ids. That is, the returned interval will be the same. Must include `:year`
   keyword if providing a year instead of a PRODES id.

   Usage:
     (prodes-interval 3)
     => [\"1999-09-01\" \"2000-09-01\"]

     (prodes-interval 4)
     => [\"1999-09-01\" \"2000-09-01\"]

     (prodes-interval 2003)
     => AssertionError

     (prodes-interval 2003 :year)
     => [\"2002-09-01\" \"2003-09-01\"]"
  ([prodes-id]
     {:pre [(contains? prodes-ids-codes prodes-id)]} 
     (let [y-end (prodes-id->year prodes-id)
           y-start (dec y-end)
           [m d] [prodes-year-month prodes-year-day]]
         [(format "%d-%02d-%02d" y-start m d)
          (format "%d-%02d-%02d" y-end m d)]))
  ([yr {:keys [year]}]
     (let [[m d] [prodes-year-month prodes-year-day]]
       [(format "%d-%02d-%02d" (dec yr) m d)
        (format "%d-%02d-%02d" yr m d)])))

(defn date->prodes-year
  "For a given date, return the so-called 'PRODES year', which we define
   as starting Sept. 1 each year"
  [date-str]
  (let [yr (Integer/parseInt (date/convert date-str
                                           :year-month-day
                                           :year))
        prodes-id (get-prodes-id yr)
        [start end] (prodes-interval prodes-id)]
    (if (date/within-dates? start end date-str)
      yr
      (inc yr))))

(defn date->prodes-id
  "Returns the PRODES id for a given date and (optionally) lag."
  ([date-str]
     (let [default-lag 0]
       (date->prodes-id date-str default-lag)))
  ([date-str lag]
     (get-prodes-id (date->prodes-year date-str) lag)))

(defmapcatop extract-forma
  "Extract FORMA values from a probability timeseries for each PRODES year."
  [t-res series-start prob-series]
  (let [[m d] [prodes-year-month prodes-year-day]
        dates (date/dec-date t-res
                        (map #(format "%d-%02d-%02d" % m d)
                             (range prodes-count-start
                                    (inc prodes-count-end))))
        years (range prodes-count-start (inc prodes-count-end))
        forma-vals (if prob-series
                     (map (partial date/get-val-at-date t-res
                                   series-start prob-series) dates)
                     (repeat (count years) nil))]
    (partition 2 (interleave years forma-vals))))

(defn mk-prodes-map
  "Given PRODES ids and counts, Make a map with lags as keys and counts as values.

   Usage:
     (mk-prodes-map [3 4] [10 15])
     => {2 15, 0 10}"
  [ids counts]
  (let [lags (map last (map parse-prodes-id ids))]
    (reduce merge (map (partial assoc {}) lags counts))))

(defn prodes-map->wide
  "Given a map of PRODES ids and their counts, outputs them in wide form with zeros inserted where
   an id isn't available for a particular year.

   Usage:
     (prodes-map->wide {2 15, 0 10}
     => [10 0 15 0 0 0 0 0]"
  [m]
  (let [max-lag 7]
    (vec (for [i (range (inc max-lag))]
      (m i 0)))))

(defn in-training?
  "Checks whether given PRODES year and lag year both fall within the training period.

   Note that this does not handle edge cases where the training period
   does not line up perfectly with the PRODES year. For example, clearing
   in September 2005 falls into PRODES year 2006. The logic of such edge
   cases must be embodied in the provided start and end date parameters.

   Usage:
     Standard case:
       (in-training? [2003 2003] \"2000-01-01\" \"2005-12-31\")
       => true

     Standard case, with lag:
     (in-training? [2006 2007] \"2000-01-01\" \"2005-12-31\")
     => false

     (in-training? [2004 2002] \"2000-01-01\" \"2005-12-31\")
     => true

     Edge case:  
       (in-training? [2006 2006] \"2000-01-01\" \"2006-09-02\")
       => true

   Note that the `end-dt` parameter is later than might be expected.
   This happens because the PRODES year is defined to run from
   September 1 of one year to September 1 of the next, exclusive."
  [[yr lag-yr] start-dt end-dt]
  (let [earliest-date (first (prodes-interval lag-yr :year))
        latest-date (last (prodes-interval yr :year))]
    (and (date/within-dates? start-dt end-dt earliest-date)
         (date/within-dates? start-dt end-dt latest-date))))

(defbufferop prodes-sum-training
  "Produces the sum of all PRODES counts over a training period."
  [tuples]
  (let [start-dts (first (map first tuples))
        end-dts (first (map second tuples))
        prodes-ids (map #(nth % 2) tuples)
        prodes-cts (map last tuples)
        yrs-lags (map parse-prodes-id prodes-ids)
        yrs (map first yrs-lags)
        lags (map last yrs-lags)
        lag-yrs (map - yrs lags)
        yr-ranges (map vector yrs lag-yrs)
        m (reduce merge (map (partial assoc {}) yr-ranges prodes-cts))]
    [(reduce + (map m (filter #(in-training? % start-dts end-dts) (keys m))))]))

(defbufferop prodes-wide
  "Given tuples of PRODES ids and counts, produce a sparse wide dataset with
   zeros where there is no id for a given year.

  Usage:
    (let [src [[12 8 0 0 3 20]
               [12 8 0 0 4 25]
               [12 8 0 0 5 21]
               [12 8 0 0 68 15]]
     (??<- [?modh ?modv ?sample ?line ?p0 ?p1 ?p2 ?p3 ?p4 ?p5 ?p6 ?p7]
           (src ?modh ?modv ?sample ?line ?prodes-id ?ct)
           (prodes-id->year ?prodes-id :> ?year)
           (prodes-wide ?prodes-id ?ct :> ?p0 ?p1 ?p2 ?p3 ?p4 ?p5 ?p6 ?p7)))
    => [[12 8 0 0 2000 20 0 25 21 0 0 0 0]
        [12 8 0 0 2010 0 0 0 0 0 0 15 0]]

    Note that id 3 is d2000_0, 4 is d2000_2, 5 is d2000_3. There are no
    d2000_1 or other id in 2000, hence the remaining zeros."
  [tuples]
  (let [ids (map first tuples)
        counts (map second tuples)]
    [(prodes-map->wide (mk-prodes-map ids counts))]))

(defn tweak-date->prodes
  "Given a date and an extender keyword `:inc` or `:dec`, increment or decrement
   the date to the beginning or end of the PRODES year implied by the date.

   Usage:
     (tweaker \"2005-05-31\" :inc)
     => \"2005-09-01\"

     (tweaker \"2005-12-31\" :inc)
     => \"2006-09-01\"

     (tweaker \"2005-12-31\" :dec)
     => \"2005-09-01\""
  [dt extender]
  {:pre [(or (= extender :inc)
             (= extender :dec))]}
  (let [extender (if (= extender :inc)
                   last
                   first)]
    (-> dt
        (date->prodes-year)
        (prodes-interval :year)
        (extender))))

(defn tweak-training
  "Tweak start and/or end of training period to account for difference
   between training period and associated PRODES years.

   For example, MODIS data start on 2000-02-18, so that is technically
   the start of the training period. But the associated PRODES year starts
   (for us) on September 1, 1999. If we want to compare FORMA to PRODES
   starting in 2000, that really means starting September 1, 1999. If we
   want to compare to 2001, that really means PRODES data starting
   September 1, 2000.

   The default behavior is to use a strict definition of the training period.   

   Usage:
     (tweak-training \"2000-02-18\" \"2005-12-31\")
     => [\"2000-09-01\" \"2005-09-01\"]

     (tweak-training \"2000-02-18\" \"2005-12-31\" :extend-start true)
     => [\"1999-09-01\" \"2005-09-01\"]

     (tweak-training \"2000-02-18\" \"2005-12-31\" :extend-end true)
     => [\"2000-09-01\" \"2006-09-01\"]

     (tweak-training \"2000-02-18\" \"2005-12-31\" :extend-start true :extend-end true)
     => [\"1999-09-01\" \"2006-09-01\"]"
  [train-start train-end & {:keys [extend-start extend-end]}]
  (let [new-start (if extend-start
                    (tweak-date->prodes train-start :dec)
                    (tweak-date->prodes train-start :inc))
        new-end (if extend-end
                  (tweak-date->prodes train-end :inc)
                  (tweak-date->prodes train-end :dec))]
    [new-start new-end]))

(defn replace-nils
  [nodata-val & fields]
  (replace {nil nodata-val} fields))

(defn outside-training?
  [yr]
  (if (or (nil? yr)
          (< 2005 yr))
    true
    false))

(defmapcatop gen-years
  [& fields]
  (vec (range 2006 2011)))

(defn prodes-wide-query
  [prodes-src]
  (<- [?modh ?modv ?s ?l ?year ?p0 ?p1 ?p2 ?p3 ?p4 ?p5 ?p6 ?p7]
      (prodes-src ?modh ?modv ?s ?l ?id ?ct)
      (prodes-id->year ?id :> ?year)
      (prodes-wide ?id ?ct :> ?p0 ?p1 ?p2 ?p3 ?p4 ?p5 ?p6 ?p7)
      (outside-training? ?year)))

(defn forma-extract-query
  [t-res est-start forma-src]
  (<- [?modh ?modv ?s ?l ?year ?forma]
      (forma-src ?sres ?modh ?modv ?s ?l ?prob-series)
      (o/clean-probs ?prob-series :> ?clean-series)
      (extract-forma t-res est-start ?clean-series :> ?year ?forma)))

(defmapcatop add-years
  [modh modv sample line]
  (map vector (range prodes-count-start
                     (inc prodes-count-end))))

(defn prodes-sums-query
  [train-start train-end prodes-src]
  (<- [?modh ?modv ?s ?l ?year ?ptrain]
       (prodes-src ?modh ?modv ?s ?l ?id ?ct)
       (add-years ?modh ?modv ?s ?l :> ?year)
       (prodes-sum-training train-start train-end ?id ?ct :> ?ptrain)))

(defn forma-prodes
  [t-res forma-src static-src prodes-src est-start train-start train-end
   & {:keys [extend-start extend-end]
      :or {extend-start true
           extend-end true}}]
    "Produces a time series dataset of PRODES counts and FORMA probabilities
   at the end of each PRODES year.

   Also includes the FORMA probability at the end of the standard training
   period, and PRODES counts - including lagged counts as appropriate - in
   wide form.

   Output fields:

       ?year    - PRODES year
       ?ptrain  - sum of PRODES counts - incl. lagged - in training period.
                  This does not change by year.
       ?p0 ?p1 ?p2 ?p3 ?p4 ?p5 ?p6 ?p7 - PRODES counts & lagged counts in year.
                  Changes by year  

     static data:
       ?hansen, ?vcf, ?gadm, ?ecoid

     FORMA data:
       ?forma-train - probability at the end of the training period.
                      Does not change by year.
       ?forma-train-ext - probability at the end of a potentially extended training
                          period. See `tweak-training`.
       ?forma -   probability at the end of the PRODES year,
                  Changes by year.

   Note that the provided `train-start` and `est-start` arguments
   should be chosen to take the misalignment of PRODES and regular
   years into account."
  (let [[train-start train-end] (tweak-training
                                 train-start train-end
                                 :extend-start extend-start
                                 :extend-end extend-end)
        prodes-wide-src (prodes-wide-query prodes-src)
        prodes-sum-src (prodes-sums-query train-start train-end prodes-src)
        forma-ts-src (forma-extract-query t-res est-start forma-src)]
    (<- [?modh ?modv ?s ?l ?vcf ?gadm ?ecoid ?hansen ?year ?forma ?forma-train ?forma-train-tweaked !!ptrain !!p0 !!p1 !!p2 !!p3 !!p4 !!p5 !!p6 !!p7 ]
        (forma-ts-src ?modh ?modv ?s ?l ?year ?forma)
        (prodes-wide-src ?modh ?modv ?s ?l ?year !!p0 !!p1 !!p2 !!p3 !!p4 !!p5 !!p6 !!p7)
        (prodes-sum-src ?modh ?modv ?s ?l ?year !!ptrain)
        (forma-src _ ?modh ?modv ?s ?l ?prob-series)
        (o/clean-probs ?prob-series :> ?clean-series)
        (date/get-val-at-date t-res est-start ?clean-series est-start :out-of-bounds-idx 0 :> ?forma-train)
        (date/get-val-at-date t-res est-start ?clean-series train-end :out-of-bounds-idx 0 :> ?forma-train-tweaked)
        (static-src _ ?modh ?modv ?s ?l ?vcf ?gadm ?ecoid ?hansen _)
        )))


;; have to make sure test passes
;; have to make sure training before beginning of FORMA returns nil

(defn check-years
  [forma-year prodes-year]
  (or (= forma-year prodes-year)
      (nil? forma-year)
      (nil? prodes-year)))

(comment
  (defn cumul-sum-query
  [train-end prodes-src]
  (<- [?modh ?modv ?s ?l ?year ?p]
       (prodes-src ?modh ?modv ?s ?l ?id ?ct)
       (add-years ?modh ?modv ?s ?l :> ?year)
       (prodes-cumul-sum train-start train-end ?id ?ct :> ?p))))

(defn prodes-sums-n-wide
  [train-start train-end prodes-src]
  (let [sums-src (prodes-sums-query train-start train-end prodes-src)
        wide-src (prodes-wide-query prodes-src)]
    (<- [?modh ?modv ?s ?l !year !ptrain !!p0 !!p1 !!p2 !!p3 !!p4 !!p5 !!p6 !!p7]
        (sums-src ?modh ?modv ?s ?l !year !!ptrain)
        (wide-src ?modh ?modv ?s ?l !year !!p0 !!p1 !!p2 !!p3 !!p4 !!p5 !p6 !p7))))



(defn forma-clean
  [t-res forma-src est-start]
  (let [forma-ts-src (forma-extract-query t-res est-start forma-src)]
    (<- [?modh ?modv ?s ?l ?year ?forma]
        (forma-ts-src ?modh ?modv ?s ?l ?year ?forma))))

(defbufferop cumul-sum
  [tuples]
  (let [lag-years (map first tuples)
        counts (map last tuples)]
    (for [yr (range prodes-count-start
                    (inc prodes-count-end))]
      [yr (reduce + (filter #(<= % yr) counts))])))

(defn prodes-clean
  [prodes-src]
  (let []
    (<- [?modh ?modv ?s ?l ?cumul-year ?sum]
        (prodes-src ?modh ?modv ?s ?l ?id ?count)
        (parse-prodes-id ?id :> ?year ?lag)
        (- ?year ?lag :> ?lag-year)
        (<= 2000 ?lag-year)
        (< 2005 ?year)
        (cumul-sum ?lag-year ?count :> ?cumul-year ?sum))))



