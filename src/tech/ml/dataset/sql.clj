(ns tech.ml.dataset.sql
  (:require [next.jdbc :as jdbc]
            [clojure.datafy :as datafy]
            [clojure.data.json :as json]
            [clojure.set :as set]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.bitmap :as bitmap]
            [tech.ml.dataset :as ds]
            [tech.ml.dataset.impl.dataset :as ds-impl]
            [tech.ml.dataset.impl.column :as col-impl])
  (:import [java.util List]
           [java.sql
            ResultSetMetaData
            DatabaseMetaData
            ResultSet]))

(defn jdbc-postgre-connect-str
  ^String [hoststr database user pwd]
  (format "jdbc:postgresql://%s/%s?user=%s&password=%s"
          hoststr database user pwd))


(defn json-table-dump
  [connection table-name]
  (->> (jdbc/execute!
        connection
        [(format "Select * from %s" table-name)])
       (mapv (fn [entry]
               (->> entry
                    (map (fn [[k v]]
                           [k (datafy/datafy v)]))
                    (into {}))))))

(def java-cls->datatype-map
  (merge
   {"java.lang.String" :string
    "java.lang.Boolean" :boolean}
   (->> casting/host-numeric-types
        (map (fn [dtype]
               [(.getName (.getClass (casting/cast 0 dtype)))
                dtype]))
        (into {}))))

(def datatype->java-cls-map
  (set/map-invert java-cls->datatype-map))


(defn result-set-type-class->datatype
  [^String cls-name]
  (get java-cls->datatype-map cls-name :object))


(defn result-set-metadata->data
  [^ResultSetMetaData metadata]
  (for [col-idx (range 1 (inc (.getColumnCount metadata)))]
    (let [col-idx (int col-idx)]
      {:name (.getColumnName metadata col-idx )
       :datatype (result-set-type-class->datatype
                  (.getColumnClassName metadata col-idx))
       :label (.getColumnLabel metadata col-idx)
       :type-name (.getColumnTypeName metadata col-idx)
       :type-index (.getColumnType metadata col-idx)
       :class-name (.getColumnClassName metadata col-idx)
       :scale (.getScale metadata col-idx)
       :precision (.getPrecision metadata col-idx)
       :nullable? (.isNullable metadata col-idx)})))


(defn- as-result-set ^ResultSet [rs] rs)


(defmacro ^:private read-results
  [datatype results idx]
  (case datatype
    :string `(.getString ~results ~idx)))


(defn- as-list ^List [item] item)


(defmacro ^:private add-to-container!
  [datatype container value]
  `(.add (as-list ~container) ~value))


(defmacro ^:private impl-read-fn
  [datatype results container idx]
  `(fn []
     (->> (read-results ~datatype ~results ~idx)
          (add-to-container! ~datatype ~container))))


(defmacro ^:private make-read-fn
  [results datatype container idx]
  `(let [results# (as-result-set ~results)
         container# ~container
         idx# (long ~idx)]
     ;;conversion of datatype from runtime to compile time
     (case ~datatype
       :string (impl-read-fn :string results# container# idx#))))


(defn result-set->dataset
  "Given a result set, return a dataset.
  options -
  :close? - if true, then .close is called on the resultset - always - including when
  there is an exception.  Defaults to true."
  ([^ResultSet results {:keys [close?]
                        :or {close? true}
                        :as options}]
   (try
     (let [columns (->> (result-set-metadata->data (.getMetaData results))
                        (map-indexed
                         (fn [idx {:keys [datatype name]}]
                           (let [container (col-impl/make-container datatype)]
                             {:name name
                              :datatype datatype
                              :data container
                              :missing (bitmap/->bitmap)
                              :parse-fn (make-read-fn results datatype
                                                      container (inc idx))}))))
           parse-fns (mapv :parse-fn columns)]
       (loop [continue? (.next results)]
         (when continue?
           (doseq [parse-fn parse-fns]
             (parse-fn))
           (recur (.next results))))
       (ds-impl/new-dataset options columns))
     (finally
       (when close? (.close results)))))
  ([results]
   (result-set->dataset results {})))


(comment
  (def conn (-> (jdbc-postgre-connect-str
                 "localhost:5432" "dev-user" "dev-user" "unsafe-bad-password")
                (jdbc/get-connection {:auto-commit false})))

  (def conn-meta (.getMetaData conn))

  (def prepared-stmt (jdbc/prepare
                      conn
                      [(format "Select * from %s" "pg_catalog.pg_tables")]))

  (def metadata (.getMetaData prepared-stmt))


  )
