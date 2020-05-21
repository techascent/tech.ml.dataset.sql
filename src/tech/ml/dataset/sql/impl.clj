(ns tech.ml.dataset.sql.impl
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.datetime :as dtype-dt]
            [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset.impl.column :as col-impl]
            [clojure.set :as set])
  (:import [java.sql Connection ResultSetMetaData PreparedStatement DatabaseMetaData
            ResultSet]
           [java.util List UUID]
           [org.roaringbitmap RoaringBitmap]
           [java.time Instant]))


(set! *warn-on-reflection* true)



(defn jdbc-postgre-connect-str
  ^String [hoststr database user pwd]
  (format "jdbc:postgresql://%s/%s?user=%s&password=%s"
          hoststr database user pwd))



(def java-cls->datatype-map
  (merge
   {"java.lang.String" :string
    "java.lang.Boolean" :boolean
    "java.sql.Date" :instant
    "java.util.UUID" :uuid}
   (->> casting/host-numeric-types
        (map (fn [dtype]
               [(.getName (.getClass ^Object (casting/cast 0 dtype)))
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


(defn- sql-date->instant
  ^Instant [^java.sql.Date date]
  (dtype-dt/milliseconds-since-epoch->instant
   (.getTime date)))


(defmacro ^:private read-results
  [datatype results idx]
  (case datatype
    :string `(.getString ~results ~idx)
    :boolean `(.getBoolean ~results ~idx)
    :int8 `(.getByte ~results ~idx)
    :int16 `(.getShort ~results ~idx)
    :int32 `(.getInt ~results ~idx)
    :int64 `(.getLong ~results ~idx)
    :float32 `(.getFloat ~results ~idx)
    :float64 `(.getDouble ~results ~idx)
    :uuid `(.getObject ~results ~idx UUID)
    :instant `(-> (.getDate ~results ~idx)
                  (sql-date->instant))))


(defn- as-list ^List [item] item)


(defmacro add-to-container!
  [datatype container missing missing-value row-idx value]
  `(if-let [value# ~value]
     (.add (as-list ~container) ~value)
     (do
       (.add ~missing (dec ~row-idx))
       (.add (as-list ~container) ~missing-value))))


(defmacro impl-read-fn
  [datatype results container missing missing-value idx]
  `(fn [^long row-idx#]
     (let [entry# (read-results ~datatype ~results ~idx)]
       (if-not (and (.wasNull ~results)
                    (not= entry# ~missing-value))
         (add-to-container! ~datatype ~container ~missing
                            ~missing-value row-idx# entry#)
         (add-to-container! ~datatype ~container ~missing
                            ~missing-value row-idx# nil)))))


(defn make-read-fn
  [results datatype container missing idx]
  (let [results (as-result-set results)
        idx (long idx)
        ^RoaringBitmap missing missing
        missing-value (col-impl/datatype->missing-value datatype)]
    ;;conversion of datatype from runtime to compile time
    (case datatype
      :boolean (impl-read-fn :boolean results container
                             missing missing-value idx)
      :string (impl-read-fn :string results container
                            missing missing-value idx)
      :uuid (impl-read-fn :uuid results container
                          missing missing-value idx)
      :int8 (impl-read-fn :int8 results container
                          missing missing-value idx)
      :int16 (impl-read-fn :int16 results container
                           missing missing-value idx)
      :int32 (impl-read-fn :int32 results container
                           missing missing-value idx)
      :int64 (impl-read-fn :int64 results container
                           missing missing-value idx)
      :float32 (impl-read-fn :float32 results container
                             missing missing-value idx)
      :float64 (impl-read-fn :float64 results container
                             missing missing-value idx)
      :instant (impl-read-fn :instant results container
                             missing missing-value idx))))


(defn ->str
  ^String [item]
  (if (or (keyword? item)
          (symbol? item))
    (name item)
    (str item)))


(defn sanitize-name-for-sql
  ^String [item]
  (-> (->str item)
      (.replace "-" "_")))


(defonce datatype->sql-datatype-map (atom {}))

(defn add-datatype-mapping
  [dtype sql-dtype]
  (swap! datatype->sql-datatype-map assoc dtype sql-dtype))

(->> [:boolean "bool"
      :int8 "tinyint"
      :int16 "smallint"
      :int32 "integer"
      :int64 "bigint"
      :float32 "float"
      :float64 "double"
      :string "varchar"
      :uuid "uuid"
      :local-date "date"
      :local-date-time "date"
      :zoned-date-time "date"
      :instant "date"]
     (partition 2)
     (map (partial apply add-datatype-mapping))
     (dorun))


(defn datatype->sql-datatype
  [dtype]
  (let [dtype (if (dtype-dt/packed-datatype? dtype)
                (dtype-dt/packed-type->unpacked-type dtype)
                (casting/un-alias-datatype dtype))]
    (if-let [retval (get @datatype->sql-datatype-map dtype)]
      retval
      (throw (Exception. (format "%s has no current datatype mapping"
                                 dtype))))))


(defn execute-update!
  [^Connection conn sql]
  (with-open [stmt (.createStatement conn)]
    (try
      (.executeUpdate stmt sql)
      (.commit conn)
      (catch Throwable e
        (.rollback conn)
        (throw (ex-info (format "Error executing:\n%s\n%s"
                                sql e)
                        {:error e}))))))


(defn dataset->table-name
  [dataset]
  (cond
    (string? dataset)
    dataset
    (keyword? dataset)
    (->str dataset)
    (symbol? dataset)
    (->str dataset)
    :else
    (->str (ds/dataset-name dataset))))


(defn- as-bitmap ^RoaringBitmap [item] item)


(defmacro ^:private add-pstmt-value
  [datatype stmt col-idx value]
  (case datatype
    :boolean `(.setBoolean ~stmt ~col-idx ~value)
    :int8 `(.setByte ~stmt ~col-idx ~value)
    :int16 `(.setShort ~stmt ~col-idx ~value)
    :int32 `(.setInt ~stmt ~col-idx ~value)
    :int64 `(.setLong ~stmt ~col-idx ~value)
    :float32 `(.setFloat ~stmt ~col-idx ~value)
    :float64 `(.setDouble ~stmt ~col-idx ~value)
    :string `(.setString ~stmt ~col-idx ~value)
    :uuid `(.setObject ~stmt ~col-idx ~value)
    :local-date `(.setDate ~stmt ~col-idx (java.sql.Date/valueOf
                                           (dtype-dt/as-local-date ~value)))
    :local-date-time `(.setDate ~stmt ~col-idx
                                (java.sql.Date.
                                 (dtype-dt/local-date-time->milliseconds-since-epoch
                                  (dtype-dt/as-local-date-time ~value))))))


(defmacro apply-pstmt-fn
  [datatype col-idx stmt reader missing]
  `(let [reader# (typecast/datatype->reader ~datatype ~reader)
         missing# (as-bitmap ~missing)
         stmt# ~stmt
         col-idx# (unchecked-int ~col-idx)
         sql-type-index# (-> (.getParameterMetaData stmt#)
                             (.getParameterType col-idx#))]
     (fn [^long row-idx#]
       (if-not (.contains missing# (unchecked-int row-idx#))
         (add-pstmt-value ~datatype stmt# col-idx# (.read reader# row-idx#))
         (.setNull stmt# col-idx# sql-type-index#)))))


(defn make-prep-statement-applier
  [^PreparedStatement stmt column-idx col]
  (let [^RoaringBitmap missing (ds-col/missing col)
        dtype (dtype/get-datatype col)
        [dtype rdr] (if (dtype-dt/packed-datatype? dtype)
                      (let [new-rdr (dtype-dt/unpack col)]
                        [(dtype/get-datatype new-rdr) new-rdr])
                      [dtype (dtype/->reader col)])
        column-idx (unchecked-int (inc column-idx))]
    (case dtype
      :boolean (apply-pstmt-fn :boolean column-idx stmt rdr missing)
      :int8 (apply-pstmt-fn :int8 column-idx stmt rdr missing)
      :int16 (apply-pstmt-fn :int16 column-idx stmt rdr missing)
      :int32 (apply-pstmt-fn :int32 column-idx stmt rdr missing)
      :int64 (apply-pstmt-fn :int64 column-idx stmt rdr missing)
      :float32 (apply-pstmt-fn :float32 column-idx stmt rdr missing)
      :float64 (apply-pstmt-fn :float64 column-idx stmt rdr missing)
      :string (apply-pstmt-fn :string column-idx stmt rdr missing)
      :uuid (apply-pstmt-fn :uuid column-idx stmt rdr missing)
      :local-date (apply-pstmt-fn :local-date column-idx stmt rdr missing)
      :local-date-time (apply-pstmt-fn :local-date-time column-idx stmt rdr missing))))


(defn db-insert-sql
  [dataset options]
  (let [table-name (->str (or (:table-name options) (ds/dataset-name dataset)))
        postgres-upsert-keys
        (or (:postgres-upsert-keys options)
            (when (:postgres-upsert? options)
              (if-let [keyseq (seq (:primary-keys (meta dataset)))]
                keyseq
                (throw (Exception. "Failed to find primary keys for upsert
Expected dataset metadata to contain non-empty :primary-keys")))))]
    (apply str
           "INSERT INTO " table-name "( "
           (concat
            (->> dataset
                 (map #(->str (:name (meta %))))
                 (interpose ", "))
            [") VALUES ("]
            (->> (repeat (ds/column-count dataset) "?")
                 (interpose  ", "))
            [")"]
            (when-let [upsert-keys postgres-upsert-keys]
              (concat ["\nON CONFLICT ("]
                      (interpose ", " (map ->str upsert-keys))
                      [") DO UPDATE SET\n"]
                      (->> (ds/column-names
                            (ds/drop-columns dataset upsert-keys))
                           (map (fn [cname]
                                  (let [str-name (->str cname)]
                                    (format "%s=excluded.%s"
                                            str-name str-name))))
                           (interpose ",\n"))))
            [";"]))))


(defn execute-prepared-statement-batches
  [^Connection conn ^String sql dataset options]
  (let [n-rows (ds/row-count dataset)
        batch-size (long (or (:batch-size options) 32))]
    (try
      (with-open [stmt (.prepareStatement conn sql)]
        (let [inserters (->> dataset
                             (map-indexed
                              (fn [idx col]
                                (make-prep-statement-applier
                                 stmt idx col))))]
          (dotimes [idx n-rows]
            (doseq [inserter inserters]
              (inserter idx))
            (.addBatch stmt)
            (when (and (== 0 (rem idx batch-size))
                       (not= 0 idx))
              (.executeBatch stmt))))
        (.executeBatch stmt))
      (.commit conn)
      (catch Throwable e
        (.rollback conn)
        (throw e)))))