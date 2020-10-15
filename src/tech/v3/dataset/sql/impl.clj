(ns ^:no-doc tech.v3.dataset.sql.impl
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.dataset.impl.column-base :as col-base]
            [clojure.set :as set])
  (:import [java.sql Connection ResultSetMetaData PreparedStatement DatabaseMetaData
            ResultSet]
           [java.util List UUID]
           [org.roaringbitmap RoaringBitmap]
           [java.time Instant LocalDate LocalDateTime ZonedDateTime]))


(set! *warn-on-reflection* true)



(defn jdbc-postgre-connect-str
  ^String [hoststr database user pwd]
  (format "jdbc:postgresql://%s/%s?user=%s&password=%s"
          hoststr database user pwd))



(def java-cls->datatype-map
  (merge
   {"java.lang.String" :string
    "java.lang.Boolean" :boolean
    "java.sql.Time" :duration
    "java.sql.Date" :local-date
    "java.sql.Timestamp" :instant
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


(defn- sql-time->duration
  ^java.time.Duration [^java.sql.Time time]
  (dtype-dt/milliseconds->duration
   ;;On postgres times are coming back as jan 2nd, not jan 1st.
   (rem (.getTime time)
        dtype-dt/milliseconds-in-day)))


(defn- sql-date->local-date
  ^java.time.LocalDate [^java.sql.Date date]
  (dtype-dt/milliseconds-since-epoch->local-date
   (.getTime date)))


(defn- sql-timestamp->instant
  ^Instant [^java.sql.Timestamp date]
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
    :duration `(-> (.getTime ~results ~idx)
                   (sql-time->duration))
    :local-date `(-> (.getDate ~results ~idx)
                     (sql-date->local-date))
    :instant `(-> (.getTimestamp ~results ~idx)
                  (sql-timestamp->instant))))


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
        missing-value (col-base/datatype->missing-value datatype)]
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
      :duration (impl-read-fn :duration results container
                              missing missing-value idx)
      :local-date (impl-read-fn :local-date results container
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
      :float64 "double precision"
      :string "varchar"
      :uuid "uuid"
      :local-date "date"
      :local-date-time "timestamp"
      :zoned-date-time "timestamp"
      :instant "timestamp"
      :duration "time"]
     (partition 2)
     (map (partial apply add-datatype-mapping))
     (dorun))


(defn datatype->sql-datatype
  [dtype]
  (let [dtype (if (packing/packed-datatype? dtype)
                (packing/unpack-datatype dtype)
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


(defn- as-local-date ^LocalDate [item] item)
(defn- as-local-date-time ^LocalDateTime [item] item)


(defmacro ^:private add-pstmt-value
  [datatype stmt col-idx rdr row-idx]
  (case datatype
    :boolean `(.setBoolean ~stmt ~col-idx (.readBoolean ~rdr ~row-idx))
    :int8 `(.setByte ~stmt ~col-idx (.readByte ~rdr ~row-idx))
    :int16 `(.setShort ~stmt ~col-idx (.readShort ~rdr ~row-idx))
    :int32 `(.setInt ~stmt ~col-idx (.readInt ~rdr ~row-idx))
    :int64 `(.setLong ~stmt ~col-idx (.readLong ~rdr ~row-idx))
    :float32 `(.setFloat ~stmt ~col-idx (.readFloat ~rdr ~row-idx))
    :float64 `(.setDouble ~stmt ~col-idx (.readDouble ~rdr ~row-idx))
    :string `(.setString ~stmt ~col-idx (.readObject ~rdr ~row-idx))
    :uuid `(.setObject ~stmt ~col-idx (.readObject ~rdr ~row-idx))
    :duration `(let [tt# (java.sql.Time.
                          (dtype-dt/datetime->milliseconds
                           (.readObject ~rdr ~row-idx)))]
                 (.setTime ~stmt ~col-idx tt#))
    :local-date `(.setDate ~stmt ~col-idx (java.sql.Date/valueOf
                                           (as-local-date
                                            (.readObject ~rdr ~row-idx))))
    :local-date-time `(.setTimestamp ~stmt ~col-idx
                                (java.sql.Timestamp.
                                 (dtype-dt/local-date-time->milliseconds-since-epoch
                                  (as-local-date-time
                                   (.readObject ~rdr ~row-idx)))))
    :zoned-date-time `(.setTimestamp ~stmt ~col-idx
                                (java.sql.Timestamp.
                                 (dtype-dt/zoned-date-time->milliseconds-since-epoch
                                  (.readObject ~rdr ~row-idx))))
    :instant `(.setTimestamp ~stmt ~col-idx
                        (java.sql.Timestamp.
                         (dtype-dt/instant->milliseconds-since-epoch
                          (.readObject ~rdr ~row-idx))))))


(defmacro apply-pstmt-fn
  [datatype col-idx stmt reader missing sql-type-idx]
  `(fn [^long row-idx#]
     (if-not (.contains ~missing (unchecked-int row-idx#))
       (add-pstmt-value ~datatype ~stmt ~col-idx ~reader row-idx#)
       (.setNull ~stmt ~col-idx ~sql-type-idx))))


(defn make-prep-statement-applier
  [^PreparedStatement stmt column-idx col]
  (let [^RoaringBitmap missing (ds-col/missing col)
        dtype (dtype/get-datatype col)
        [dtype rdr] (if (packing/packed-datatype? dtype)
                      (let [new-rdr (packing/unpack col)]
                        [(dtype/get-datatype new-rdr) new-rdr])
                      [dtype (dtype/->reader col)])
        column-idx (unchecked-int (inc column-idx))
        rdr (dtype/->reader rdr)
        column-idx (int column-idx)
        sql-type-index (-> (.getParameterMetaData stmt)
                           (.getParameterType column-idx))]
    (case dtype
      :boolean (apply-pstmt-fn :boolean column-idx stmt rdr missing sql-type-index)
      :int8 (apply-pstmt-fn :int8 column-idx stmt rdr missing sql-type-index)
      :int16 (apply-pstmt-fn :int16 column-idx stmt rdr missing sql-type-index)
      :int32 (apply-pstmt-fn :int32 column-idx stmt rdr missing sql-type-index)
      :int64 (apply-pstmt-fn :int64 column-idx stmt rdr missing sql-type-index)
      :float32 (apply-pstmt-fn :float32 column-idx stmt rdr missing sql-type-index)
      :float64 (apply-pstmt-fn :float64 column-idx stmt rdr missing sql-type-index)
      :string (apply-pstmt-fn :string column-idx stmt rdr missing sql-type-index)
      :duration (apply-pstmt-fn :duration column-idx stmt rdr missing sql-type-index)
      :uuid (apply-pstmt-fn :uuid column-idx stmt rdr missing sql-type-index)
      :local-date (apply-pstmt-fn :local-date column-idx stmt rdr missing sql-type-index)
      :local-date-time (apply-pstmt-fn :local-date-time column-idx stmt rdr missing
                                       sql-type-index)
      :zoned-date-time (apply-pstmt-fn :zoned-date-time column-idx stmt rdr missing
                                       sql-type-index)
      :instant (apply-pstmt-fn :instant column-idx stmt rdr missing sql-type-index))))


(defn insert-sql
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
            (->> (ds/columns dataset)
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


(defn create-sql
  ([dataset table-name primary-key]
   (let [n-cols (ds/column-count dataset)]
     (apply str "CREATE TABLE "
            table-name
            " (\n"
            (concat
             (->> (ds/columns dataset)
                  (map-indexed (fn [idx column]
                                 (let [colmeta (meta column)
                                       colname (->str (:name colmeta))
                                       col-dtype (datatype->sql-datatype
                                                  (:datatype colmeta))]
                                   (if-not (== idx (dec n-cols))
                                     [" " colname " " col-dtype ",\n"]
                                     [" " colname " " col-dtype]))))
                  (apply concat))
             (when (seq primary-key)
               (concat [",\n PRIMARY KEY ("]
                       (interpose ", " (map ->str primary-key))
                       [")"]))
             "\n);"))))
  ([dataset]
   (create-sql dataset (ds/dataset-name dataset)
               (:primary-key (meta dataset)))))
