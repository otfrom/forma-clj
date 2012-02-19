(ns forma.hadoop.jobs.scatter
  "Namespace for arbitrary queries."
  (:use cascalog.api
        forma.trends.data
        [forma.hadoop.pail :only (to-pail)]
        [forma.source.tilesets :only (tile-set country-tiles)]
        [forma.hadoop.pail :only (?pail- split-chunk-tap)]
        [cascalog.checkpoint :only (workflow)])
  (:require [cascalog.ops :as c]
            [forma.utils :only (throw-illegal)]
            [forma.reproject :as r]
            [forma.schema :as schema]
            [forma.trends.stretch :as stretch]
            [forma.hadoop.predicate :as p]
            [forma.hadoop.jobs.forma :as forma]
            [forma.hadoop.jobs.timeseries :as tseries]))

(def convert-line-src
  (hfs-textline "s3n://modisfiles/ascii/admin-map.csv"))

(defn static-tap
  "Accepts a source of DataChunks containing vectors as values, and
  returns a new query with all relevant spatial information plus the
  actual value."
  [chunk-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?val]
      (chunk-src _ ?chunk)
      (p/blossom-chunk ?chunk :> ?s-res ?mod-h ?mod-v ?sample ?line ?val)))

(defn country-tap
  [gadm-src convert-src]
  (let [gadm-tap (static-tap gadm-src)]
    (<- [?s-res ?mod-h ?mod-v ?sample ?line ?country]
        (gadm-tap ?s-res ?mod-h ?mod-v ?sample ?line ?admin)
        (convert-src ?textline)
        (p/converter ?textline :> ?country ?admin))))

(defmain GetStatic
  [pail-path out-path]
  (let [[vcf hansen ecoid gadm border] (map (comp static-tap
                                                  (partial split-chunk-tap pail-path))
                                            [["vcf"] ["hansen"]
                                             ["ecoid"] ["gadm"]
                                             ["border"]])]
    (?<- (hfs-textline out-path
                       :sinkparts 3
                       :sink-template "%s/")
         [?country ?lat ?lon ?mod-h ?mod-v ?sample ?line ?hansen ?ecoid ?vcf ?gadm ?border]
         (vcf    ?s-res ?mod-h ?mod-v ?sample ?line ?vcf)
         (hansen ?s-res ?mod-h ?mod-v ?sample ?line ?hansen)
         (ecoid  ?s-res ?mod-h ?mod-v ?sample ?line ?ecoid)
         (gadm   ?s-res ?mod-h ?mod-v ?sample ?line ?gadm)
         (border ?s-res ?mod-h ?mod-v ?sample ?line ?border)
         (r/modis->latlon ?s-res ?mod-h ?mod-v ?sample ?line :> ?lat ?lon)
         (convert-line-src ?textline)
         (p/converter ?textline :> ?country ?gadm)
         (>= ?vcf 25))))

;; ## Forma

(def forma-run-parameters
  {"1000-32" {:est-start "2005-12-31"
              :est-end "2011-08-01" ;; I KEEP FUCKING THIS UP
              :s-res "1000"
              :t-res "32"
              :neighbors 1
              :window-dims [600 600]
              :vcf-limit 25
              :long-block 15
              :window 5}
   "500-16" {:est-start "2005-12-31"
             :est-end "2012-01-17"
             :s-res "500"
             :t-res "16"
             :neighbors 1
             :window-dims [600 600]
             :vcf-limit 25
             :long-block 30
             :window 10
             :ridge-const 1e-8
             :convergence-thresh 1e-6
             :max-iterations 500}})

(defn paths-for-dataset
  [dataset s-res t-res tile-seq]
  (let [res-string (format "%s-%s" s-res t-res)]
    (for [tile tile-seq]
      [dataset res-string (apply r/hv->tilestring tile)])))

(defn constrained-tap
  [ts-pail-path dataset s-res t-res country-seq]
  (->> (apply tile-set country-seq)
       (paths-for-dataset dataset s-res t-res)
       (apply split-chunk-tap ts-pail-path)))

(defn adjusted-precl-tap
  "Document... returns a tap that adjusts for the incoming
  resolution."
  [ts-pail-path s-res base-t-res t-res country-seq]
  (let [precl-tap (constrained-tap ts-pail-path "precl" s-res base-t-res country-seq)]
    (if (= t-res base-t-res)
      precl-tap
      (<- [?path ?final-chunk]
        (precl-tap ?path ?chunk)
        (map ?chunk [:location :value] :> ?location ?series)
        (schema/unpack-pixel-location ?locat)
        (stretch/ts-expander base-t-res t-res ?series :> ?new-series)
        (assoc ?chunk
          :value ?new-series
          :temporal-res t-res :> ?final-chunk)
        (:distinct false)))))

(defmapcatop expand-rain-pixel
  [sample line]
  (let [sample-0 (* 2 sample)
        line-0   (* 2 line)]
    (for [x (range 2)
          y (range 2)]
      [(+ sample-0 x) (+ line-0 y)])))

(defn new-adjusted-precl-tap
  "More brittle version that assumes conversion from 1000 to 500m."
  [ts-pail-path s-res base-t-res t-res country-seq]
  (let [precl-tap (constrained-tap ts-pail-path "precl" s-res base-t-res country-seq)]
    (<- [?path ?final-chunk]
        (precl-tap ?path ?chunk)
        (forma/get-loc ?chunk :> ?s-res ?mod-h ?mod-v ?s ?l ?series)
        (expand-rain-pixel ?s ?l :> ?sample ?line)
        (schema/pixel-location "500" ?mod-h ?mod-v ?sample ?line :> ?location)
        (stretch/ts-expander base-t-res t-res ?series :> ?new-series)
        (assoc ?chunk
          :value ?new-series
          :location ?location
          :temporal-res t-res :> ?final-chunk)
        (:distinct false))))

(comment
  "This command runs FORMA."
  (formarunner "/user/hadoop/checkpoints"
               "s3n://pailbucket/master"
               "s3n://pailbucket/series"
               "s3n://formaresults/forma2012"
               "500-16"
               [:IDN]))

(defn first-half-query
  "Poorly named! Returns a query that generates a number of position
  and dataset identifier"
  [pail-path ts-pail-path out-path run-key country-seq]
  (let [{:keys [s-res t-res est-end] :as forma-map} (forma-run-parameters run-key)]
    (assert forma-map (str run-key " is not a valid run key!"))
    (with-job-conf {"mapreduce.jobtracker.split.metainfo.maxsize" 100000000000}
      (?- (hfs-seqfile out-path :sinkmode :replace)
          (forma/forma-query
           forma-map
           (constrained-tap ts-pail-path "ndvi" s-res t-res country-seq)
           (constrained-tap ts-pail-path "reli" s-res t-res country-seq)
           (new-adjusted-precl-tap ts-pail-path "1000" "32" t-res country-seq)
           (constrained-tap pail-path "vcf" s-res "00" country-seq)
           (tseries/fire-query pail-path
                               t-res
                               "2000-11-01"
                               est-end
                               country-seq))))))

(defmain formarunner
  [tmp-root pail-path ts-pail-path out-path run-key country-seq]
  (let [{:keys [s-res t-res est-end] :as est-map} (forma-run-parameters run-key)
        mk-filter (fn [vcf-path ts-src]
                    (forma/filter-query (hfs-seqfile vcf-path)
                                        (:vcf-limit est-map)
                                        ts-src))]
    (assert est-map (str run-key " is not a valid run key!"))
    (workflow [tmp-root]
              vcf-step ([:tmp-dirs vcf-path]
                          (?- (hfs-seqfile vcf-path)
                              (constrained-tap
                               pail-path "vcf" s-res "00" country-seq)))

              fire-step ([:tmp-dirs fire-path]
                           (?- (hfs-seqfile fire-path)
                               (tseries/fire-query pail-path
                                                   t-res
                                                   "2000-11-01"
                                                   est-end
                                                   country-seq)))

              adjustfires
              ([:tmp-dirs adjusted-fire-path]
                 (?- (hfs-seqfile adjusted-fire-path)
                     (forma/fire-tap est-map (hfs-seqfile fire-path))))

              ndvi-step
              ([:tmp-dirs ndvi-path]
                 (?- (hfs-seqfile ndvi-path)
                     (mk-filter vcf-path
                                (constrained-tap
                                 ts-pail-path "ndvi" s-res t-res country-seq))))

              reli-step
              ([:tmp-dirs reli-path]
                 (?- (hfs-seqfile reli-path)
                     (mk-filter vcf-path
                                (constrained-tap
                                 ts-pail-path "reli" s-res t-res country-seq))))

              rain-step
              ([:tmp-dirs rain-path]
                 (?- (hfs-seqfile rain-path)
                     (mk-filter vcf-path
                                (new-adjusted-precl-tap
                                 ts-pail-path "1000" "32" t-res country-seq))))

              adjustseries
              ([:tmp-dirs adjusted-series-path]
                 "Adjusts the lengths of all timeseries
                               and filters out timeseries below the proper VCF value."
                 (with-job-conf {"mapred.min.split.size" 805306368}
                   (?- (hfs-seqfile adjusted-series-path)
                       (forma/dynamic-filter (hfs-seqfile ndvi-path)
                                             (hfs-seqfile reli-path)
                                             (hfs-seqfile rain-path)
                                             (hfs-seqfile vcf-path)))))

              trends ([:tmp-dirs dynamic-path]
                        "Runs the trends processing."
                        (?- (hfs-seqfile dynamic-path)
                            (forma/dynamic-tap
                             est-map (hfs-seqfile adjusted-series-path))))

              mid-forma ([:tmp-dirs forma-mid-path
                          :deps [trends adjustfires]]
                           (?- (hfs-seqfile forma-mid-path)
                               (forma/forma-tap (hfs-seqfile dynamic-path)
                                                (hfs-seqfile adjusted-fire-path))))

              final-forma ([] (?- (hfs-seqfile out-path)
                                  (forma/forma-query est-map
                                                     (hfs-seqfile forma-mid-path)))))))

(defn populate-local [main-path timeseries-path]
  (doto timeseries-path
    (to-pail [[(schema/chunk-value
                "ndvi" "16" nil
                (schema/pixel-location "500" 8 6 0 0)
                (schema/timeseries-value 693 (concat ndvi ndvi)))]])
    (to-pail [[(schema/chunk-value
                "reli" "16" nil
                (schema/pixel-location "500" 8 6 0 0)
                (schema/timeseries-value 693 (concat reli reli)))]])
    (to-pail [[(schema/chunk-value
                "precl" "32" nil
                (schema/pixel-location "1000" 8 6 0 0)
                (schema/timeseries-value 361 rain-raw))]]))
  (doto main-path
    (to-pail [[(schema/chunk-value "vcf" "00" nil
                                   (schema/chunk-location "500" 8 6 0 24000)
                                   (into [] (repeat 156 30)))]])
    (to-pail [(->> (schema/chunk-value "fire" "01" "2004-12-01"
                                       (schema/pixel-location "500" 8 6 0 0)
                                       (schema/fire-value 1 1 1 1))
                   (repeat 10))])))

(comment
  "We need to move this over to the testing namespace. This is an
example of how you'd populate a local directory with some timeseries
and run forma:"
  (let [main-path "/Users/sritchie/Desktop/mypail"
        ts-path   "/Users/sritchie/Desktop/tspail"
        out-path  "/Users/sritchie/Desktop/updawg"]
    (populate-local main-path ts-path)
    (first-half-query main-path
                      out-path
                      "500-16"
                      [[8 6]]))
  
  )
