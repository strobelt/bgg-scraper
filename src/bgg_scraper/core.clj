(ns bgg-scraper.core
  (:require
   [clojure-csv.core :as csv]
   [clojure.data.json :as json]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.xml :as xml])
  (:gen-class))

(def bgg-api "https://api.geekdo.com/xmlapi/boardgame/")
(def ks #{:yearpublished
          :minplayers
          :maxplayers
          :playingtime
          :minplaytime
          :maxplaytime
          :age
          :image
          :id
          :publisher})

(defn csv-data->maps [csv-data]
  (map zipmap
       (->> (first csv-data)
            (map keyword)
            repeat)
       (rest csv-data)))

(defn format-bg [bg-names xml]
  (let [bg (->> xml
                (filter (comp ks :tag))
                (map (fn [m] {(:tag m) (first (:content m))}))
                (into {}))]
    (assoc bg :name (bg-names (:id bg)))))

(defn parse-xml [bg-names url]
  (->> url
       xml/parse
       xml-seq
       (filter (comp #{:boardgame} :tag))
       (map (fn [m] (concat [{:tag :id :content [(-> m :attrs :objectid)]}] (:content m))))
       (map (partial format-bg bg-names))))

(defn save [bgs]
  (with-open [w (io/writer "bgs.json")]
    (json/write bgs w)))

(defn scrap []
  (let [csv-data (csv-data->maps (csv/parse-csv (slurp "boardgames_ranks.csv")))
        bg-names (->> csv-data (map (fn [m] {(:id m) (:name m)})) (into {}))]
    (->> csv-data
         (map :id)
         (partition 10)
         (map #(str bgg-api (str/join "," %)))
         (map (partial parse-xml bg-names))
         flatten
         save)))

(defn -main [] (scrap))

