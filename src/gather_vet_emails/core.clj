(ns gather-vet-emails.core
  (:require [clojure.string :as string]
            [cheshire.core :as json]
            [clojure.set :as sets]))


(def filename "zip_code_database.csv")
(def yp-url "http://api2.yp.com/listings/v1/search?searchloc=ZIPCODE&term=veterinarian&format=json&sort=distance&radius=100&listingcount=50&key=k0f4t3dfvg")

(defn read-file
  []
  (slurp filename))


(defn clean [raw]
  (string/replace raw "\"" ""))

(defn split-rows [raw]
  (string/split raw #"\n"))

(defn split-cols [rows]
  ; Splits comma-delimited rows.
  (map #(string/split % #",") rows))

(defn zip-code-col [rows]
  ; Gets the zip code column from the rows
  (map first rows))

(defn shuffled-zip-codes
  "Returns a list of zip codes"
  []
  (-> (read-file)
      (clean)
      (split-rows)
      (split-cols)
      (zip-code-col)
      (shuffle)))

(defn read-json
  ; Reads a json string and parses into a map
  [raw-json]
  (json/parse-string raw-json true))

(defn extract-result-listing
  ; Takes a json in form of clojure map and returns search result listing
  [json]
  (get-in json [:searchResult :searchListings :searchListing]))

(defn filter-empty-email-listings
  ; Filters out any listing without email address
  [listing]
  (filter #(not (string/blank? (:email %))) listing))

(defn filter-fields
  ; Only selects interesting fields in a listing which is a list of maps.
  [listing fields]
  (map #(select-keys % fields) listing))

(defn csv-output
  ; Converts list of maps to list of map values with | delimited output
  [listing]
  (map vals listing))

(defn listing-with-emails
  ; Returns a search listing that contains email.
  [raw-json]
  (-> (read-json raw-json)
      (extract-result-listing)
      (filter-empty-email-listings)
      (filter-fields [:businessName
                      :city
                      :state
                      :email
                      :phone])))

(defn pretty-print-map
  [map]
  )

(defn maps-to-lists-of-values
  "Converts a list of maps to lists of the values only"
  [m]
  (map vals m))

(defn lists-to-csvs
  "Converts list of lists to csv string delimited by |"
  [lists]
  (string/join "\n" (map #(string/join "|" %) lists)))

(defn maps-to-csvs
  "Converts a list of maps to | delimited csv "
  [m]
  (-> (maps-to-lists-of-values m)
      (lists-to-csvs)))

(defn main [iterations]
  "Gets information from yellow pages.
   Usage: (main 5)"
  (let [zipcodes (shuffled-zip-codes)]
    (dotimes [i iterations]
      (let [zipcode (nth zipcodes i)
            url (string/replace yp-url "ZIPCODE" zipcode)
            json-result (slurp url)
            listings (listing-with-emails json-result)
            csvs (maps-to-csvs listings)]
        (println "***************")
        (println "Iteration" i)
        (println "Zipcode" zipcode)
        (println csvs)
        (spit "listing.txt" (doall csvs) :append true)
        (when (seq csvs)
          (spit "listing.txt" csvs :append true))
        (println "***************")
        ))))




