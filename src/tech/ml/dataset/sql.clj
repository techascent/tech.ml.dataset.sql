(ns tech.ml.dataset.sql
  (:require [next.jdbc :as jdbc]
            [clojure.datafy :as datafy]
            [clojure.data.json :as json]
            [clojure.set :as set]
            [tech.v2.datatype.casting :as casting])
  (:import [java.sql ResultSetMetaData]))

(defn jdbc-postgre-connect-str
  ^String [hoststr database user pwd]
  (format "jdbc:postgresql://%s/%s?user=%s&password=%s"
          hoststr database user pwd))


(defn connect
  "Returns a jdbc connection object"
  [connect-str]
  (-> (jdbc/get-datasource connect-str)
      (jdbc/get-connection)))


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


(comment
  (def conn (-> (jdbc-postgre-connect-str
                 "localhost:5432" "dev-user" "dev-user" "unsafe-bad-password")
                (connect)))

  (def prepared-stmt (jdbc/prepare
                      conn
                      [(format "Select * from %s" "pg_catalog.pg_tables")]))

  (def metadata (.getMetaData prepared-stmt))


  )
