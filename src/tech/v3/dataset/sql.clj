(ns tech.v3.dataset.sql
  "Pathways to transform dataset to and from SQL databases.  Built directly on
  java.sql interfaces.

  ```clojure
user> (def stocks (-> (ds/->dataset \"https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv\" {:key-fn keyword})
                      (vary-meta assoc
                                 :name \"stocks\"
                                 :primary-keys [\"date\" \"symbol\"])))

#'user/stocks
user> (ds/head stocks)
stocks [5 3]:

| :symbol |      :date | :price |
|---------|------------|-------:|
|    MSFT | 2000-01-01 |  39.81 |
|    MSFT | 2000-02-01 |  36.35 |
|    MSFT | 2000-03-01 |  43.22 |
|    MSFT | 2000-04-01 |  28.37 |
|    MSFT | 2000-05-01 |  25.45 |
user> (sql/table-exists? dev-conn stocks)
false
user> (sql/create-table! dev-conn stocks)
nil
user> (sql/table-exists? dev-conn stocks)
true
user> (sql/insert-dataset! dev-conn stocks)
nil
user> ;;Note the column names are now strings.
user> (def sql-stocks (sql/sql->dataset dev-conn \"Select * from stocks\"))
#'user/sql-stocks
user> (ds/head sql-stocks)
_unnamed [5 3]:

| symbol |       date | price |
|--------|------------|------:|
|   MSFT | 2000-01-01 | 39.81 |
|   MSFT | 2000-02-01 | 36.35 |
|   MSFT | 2000-03-01 | 43.22 |
|   MSFT | 2000-04-01 | 28.37 |
|   MSFT | 2000-05-01 | 25.45 |
user> (ds/head (sql/sql->dataset dev-conn \"Select * from stocks\" {:key-fn keyword}))
_unnamed [5 3]:

| :symbol |      :date | :price |
|---------|------------|-------:|
|    MSFT | 2000-01-01 |  39.81 |
|    MSFT | 2000-02-01 |  36.35 |
|    MSFT | 2000-03-01 |  43.22 |
|    MSFT | 2000-04-01 |  28.37 |
|    MSFT | 2000-05-01 |  25.45 |
```"
  (:require [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.io.column-parsers :as col-parsers]
            [tech.v3.dataset.impl.column-base :as col-base]
            [clojure.tools.logging :as log])
  (:import [java.util List]
           [org.roaringbitmap RoaringBitmap]
           [java.time Instant LocalDate LocalTime]
           [java.sql Connection ResultSetMetaData PreparedStatement
            DatabaseMetaData ResultSet ParameterMetaData]
           [tech.v3.dataset.io.column_parsers PParser]
           [tech.v3.dataset Text]))


(set! *warn-on-reflection* true)


(defn postgre-connect-str
  ^String [hoststr database user pwd]
  (format "jdbc:postgresql://%s/%s?user=%s&password=%s"
    hoststr database user pwd))


(defn sql-server-connect-str
  ^String [hoststr database user pwd]
  (format "jdbc:sqlserver://%s;databaseName=%s;user=%s;password=%s"
          hoststr database user pwd))


(defn database-name
  [conn]
  (if (string? conn)
    conn
    (let [^Connection conn conn]
      (-> conn
          (.getMetaData)
          (.getDatabaseProductName)))))


(def default-sql-types {:int8 "tinyint"
                        :int16 "smallint"
                        :int32 "int"
                        :int64 "bigint"
                        :float32 "float"
                        :float64 "double precision"
                        :string "varchar(4096)"
                        :text "text"
                        :local-date "date"
                        :local-time "time"
                        :instant "timestamp"})


(defn default-sql-datatype
  [dt-kwd]
  (if-let [retval (get default-sql-types dt-kwd)]
    retval
    (throw (Exception. (format "Failed to find default sql datatype for %s"
                               dt-kwd)))))


(def database-data* (atom {"PostgreSQL" {:uint8 :int16
                                         :int8 :int16
                                         :uint16 :int32
                                         :uint32 :int64
                                         :string "varchar"
                                         :uuid {:sql-datatype "uuid"
                                                ;;prepared statement insert sql
                                                :insert-sql "? ::UUID"}}}))


(defn column-metadata->sql-datatype
  [database-name column-metadata]
  (if-let [sql-dt (get column-metadata :sql-datatype)]
    sql-dt
    (let [col-dt (packing/unpack-datatype
                  (:datatype column-metadata))]
      (if-let [db-entry (get-in @database-data* [database-name col-dt])]
        (cond
          (keyword? db-entry)
          (default-sql-datatype db-entry)
          (map? db-entry)
          (get db-entry :sql-datatype)
          (string? db-entry)
          db-entry
          :else
          (throw (Exception. (format "Unrecognized database entry %s" db-entry))))
        (default-sql-datatype col-dt)))))


(defn- ->str
  ^String [item]
  (if (or (keyword? item)
          (symbol? item))
    (name item)
    (str item)))


(defn sanitize
  ^String [item]
  (-> (->str item)
      (.replace "-" "_")))


(defn table-name
  (^String [dataset options]
   (if (string? dataset)
     dataset
     (sanitize (or (:table-name options) (get (meta dataset) :name)))))
  ([dataset]
   (table-name dataset nil)))


(defn primary-key
  ([dataset options]
   (when (nil? dataset)
     (throw (Exception. "No dataset provided")))
   (let [pk (or (:primary-key options) (get (meta dataset) :primary-key))]
     (if (or (string? pk) (not (seqable? pk)))
       [(sanitize pk)]
       (map sanitize pk))))
  ([dataset]
   (primary-key dataset nil)))


(defn- column-metadata
  [dataset]
  (cond
    (ds-impl/dataset? dataset)
    (map meta (vals dataset))
    (seqable? dataset)
    dataset
    :else
    (throw (Exception. (format "Unrecognized column metadata type: %s" (type dataset))))))


(defn create-sql
  "Return a create table statement for this datasets.  Datatype appropriate columns
  will be created based on each column's metadata.

  * `conn-or-db-name` - Either a Connection object or a string database name to use to map
  database-specific datatypes, such as UUID's, to the appropriate column datatype.
  * `dataset` - dataset or sequence of column metadata maps each of which must contain
  :name.  `dataset`'s metadata itself must contain a `:name` key.


  Options:

  All options may be provided as metadata on the table itself.

   * `:table-name` - table name to use.
   * `:primary-key` - Either a string or a sequence of strings indicating the primary key
       clause.

  Column Metadata:

  Column metadata must have :name.  Each column metadata map may have :sql-datatype in which
  case this will be used unchangd or :datatype which will be matched against the database
  name to decide the specific datatype to use.

  "
  ([conn-or-db-name dataset options]
   (let [database-name (database-name conn-or-db-name)
         table-name (table-name dataset options)
         primary-key (primary-key dataset options)
         column-metadata (column-metadata dataset)
         n-cols (ds/column-count dataset)]
     (apply str "CREATE TABLE "
            table-name
            " (\n"
            (concat
             (->> column-metadata
                  (map-indexed (fn [idx colmeta]
                         (let [colname (sanitize (:name colmeta))
                               col-dtype (column-metadata->sql-datatype
                                          database-name
                                          colmeta)]
                           (if-not (= idx (dec n-cols))
                             [" " colname " " col-dtype ",\n"]
                             [" " colname " " col-dtype]))))
                  (apply concat))
             (when (seq primary-key)
               (concat [",\n PRIMARY KEY ("]
                       (interpose ", " (map ->str primary-key))
                       [")"]))
             "\n);"))))
  ([conn-or-db-name dataset]
   (create-sql conn-or-db-name dataset nil)))


(defn execute-update!
  "Executes the given SQL statement, which may be an INSERT, UPDATE, or DELETE statement or
  an SQL statement that returns nothing, such as an SQL DDL statement.
  This statement will be immediately committed."
  [^Connection conn sql]
  (let [ac (.getAutoCommit conn)]
    (with-open [stmt (.createStatement conn)]
      (try
        (.executeUpdate stmt sql)
        (when-not ac (.commit conn))
        (catch Throwable e
          (try
            (when-not ac (.rollback conn))
            (catch Throwable ee nil))
          (throw (ex-info (format "Error executing:\n%s\n%s"
                                  sql e)
                          {:error e})))))))


(defn create-table!
  "Create a table.  Exception upon failure to drop the table.

  - conn - java.sql.Connection
  - dataset - dataset to use.  The dataset-name will be used as the table-name and the
     column names and datatypes will be used for the sql names and datatypes.

  The dataset's dataset-name will be used for the table and if the dataset's metadata
  has a :primary-key member this will be interpreted as a sequence of column names
  to be used as the primary key.

  Options:

  * `:table-name` - set the name of the table to use.  Overrides the dataset metadata.
  * `:primary-key` - Array of column names to use as the primary key of this table.
     Overrides the dataset metadata.
  "
  ([^Connection conn dataset options]
   (execute-update! conn (create-sql conn dataset options)))
  ([conn dataset]
   (create-table! conn dataset {})))


(declare sql->dataset)


(defn table-exists?
    "Test if a table exists.

  * conn - java.sql.Connection
  * dataset - string, keyword, symbol, or dataset

  Example:

```clojure
  user> (sql/table-exists? dev-conn stocks)
  true
```
  "
    [conn dataset]
    (try
      (sql->dataset conn (format "Select COUNT(*) from %s where 1 = 0"
                                 (table-name dataset nil)))
      true
      (catch Throwable e
        false)))


(defn drop-table!
  "Drop a table.  Exception upon failure to drop the table.

  * conn - java.sql.Connection
  * dataset - string, keyword, symbol, or dataset"
  [conn dataset]
  (execute-update! conn (format "DROP TABLE %s" (table-name dataset nil))))


(defn drop-table-when-exists!
  "Drop the table indicated by this dataset if it exists."
  [conn dataset]
  (when (table-exists? conn dataset)
    (drop-table! conn dataset)))


(defn ensure-table!
  "Create a table if it does not exist.  See documentation for create-table!"
  ([^Connection conn dataset options]
   (if-not (table-exists? conn dataset)
     (do
       (create-table! conn dataset options)
       true)
     false))
  ([^Connection conn dataset]
   (ensure-table! conn dataset {})))


(defn column-metadata->insert-sql
  ^String [database-name colmeta]
  (if-let [ins-sql (:insert-sql colmeta)]
    ins-sql
    (get-in @database-data*
            [database-name
             (packing/unpack-datatype (:datatype colmeta))
             :insert-sql]
            "?")))


(defn insert-sql
  ([conn-or-db-name dataset options]
   (let [database-name (database-name conn-or-db-name)
         table-name (table-name dataset options)
         column-metadata (column-metadata dataset)
         terminate? (get options :terminate? true)]
     (apply str
            "INSERT INTO " table-name "( "
            (concat
             (->> column-metadata
                  (map #(sanitize (:name %)))
                  (interpose ", "))
             [") VALUES ("]
             (->> column-metadata
                  (map (partial column-metadata->insert-sql database-name))
                  (interpose  ", "))
             [")"]
             (when terminate? [";"])))))
  ([conn-or-db-name dataset]
   (insert-sql conn-or-db-name dataset nil)))


(defn postgres-upsert-sql
  ([conn-or-db-name dataset options]
   (let [upsert-keys (->> (or (:postgres-upsert-keys options)
                              (primary-key dataset options))
                          (map sanitize))
         _ (when-not (seq upsert-keys)
             (throw (Exception. "Upsert requires primary keys.  These may be supplied either
via the options map or as the key :primary-key in the dataset metadata")))
         cnames (->> (column-metadata dataset)
                     (map (comp sanitize :name) column-metadata)
                     (remove (set upsert-keys)))]
     (apply str
            (insert-sql conn-or-db-name dataset (assoc options :terminate? false))
            (concat
             (apply str
                    (concat [()]["\nON CONFLICT ("]
                            (interpose ", " upsert-keys)
                            [") DO UPDATE SET\n"]
                            (->> (map #(format "%s=excluded.%s" % %))
                                 (interpose ",\n"))))
             [";"]))))
  ([conn-or-db-name dataset]))


(defn column-generic-read
  [col idx]
  (col idx))


(defn make-column-convert-read
  [convert-fn]
  (fn [col idx]
    (when-let [retval (col idx)]
      (convert-fn retval))))


(defn sql-datatype->column-read-fn
  [database-name sql-datatype]
  (if-let [read-fn (get-in database-data* [database-name sql-datatype :column->sql])]
    read-fn
    (case sql-datatype
      "text" (make-column-convert-read str)
      "date" (make-column-convert-read #(java.sql.Date/valueOf ^LocalDate %))
      "time" (make-column-convert-read #(java.sql.Time/valueOf ^LocalTime %))
      "timestamp" (make-column-convert-read #(java.sql.Timestamp/from ^Instant %))
      column-generic-read)))


(defn column->sql-fn
  [database-name column-metadata]
  (if-let [read-fn (get column-metadata :column->sql)]
    read-fn
    (let [sql-datatype (column-metadata->sql-datatype database-name column-metadata)]
      (sql-datatype->column-read-fn database-name sql-datatype))))


(defn execute-prepared-statement-batches!
  "Internal method to, using a dataset, execute prepared statement batches
  drawn from the rows of the dataset.  For this to work correctly, the
  connection needs to have autoCommit set to false and the sql statement's datatypes need
  to match the dataset datatypes.

  * conn - java.sql.Connection
  * stmt-or-sql - Either a prepared statement or a string in which case
    the connection's .prepareStatement method will be called.
  * dataset - dataset

  Options

  - batch-size - integer, defaults to 1024
  "
    [^Connection conn stmt-or-sql dataset options]
    (let [n-rows (ds/row-count dataset)
          batch-size (long (or (:batch-size options) 1024))
          database-name (database-name conn)
          ac (.getAutoCommit conn)]
      (when ac (log/warnf "AutoCommit should be disabled on connections when using prepared statements."))
      (try
        (with-open [^PreparedStatement stmt
                    (cond
                      (instance? PreparedStatement stmt-or-sql)
                      stmt-or-sql
                      (string? stmt-or-sql)
                      (.prepareStatement conn ^String stmt-or-sql)
                      :else
                      (throw (Exception.
                              (format "Arg must either be sql or statement: %s"
                                      stmt-or-sql))))]
          (let [appliers
                (->> (ds/columns dataset)
                     (map-indexed
                      (fn [idx col]
                        (let [sql-idx (inc (long idx))
                              read-fn (column->sql-fn database-name (meta col))]
                          #(.setObject stmt sql-idx (read-fn col %)))))
                     (object-array))
                n-cols (alength appliers)]
            (dotimes [row-idx n-rows]
              (dotimes [col-idx n-cols]
                ((aget appliers col-idx) row-idx))
              (.addBatch stmt)
              (when (and (== 0 (rem row-idx batch-size))
                         (not= 0 row-idx))
                (.executeBatch stmt))))
          (.executeBatch stmt))
        (when-not ac (.commit conn))
        (catch Throwable e
          (when-not ac (.rollback conn))
          (throw e)))))


(defn insert-dataset!
  ([conn dataset options]
   (execute-prepared-statement-batches! conn
                                        (insert-sql conn dataset options)
                                        dataset options))
  ([conn dataset]
   (insert-dataset! conn dataset nil)))


(defn result-set-metadata->data
  [^ResultSetMetaData metadata col-idx]
  (let [sql-idx (int (inc col-idx))]
    {:name (.getColumnName metadata sql-idx )
     :label (.getColumnLabel metadata sql-idx)
     :type-name (.toLowerCase (str (.getColumnTypeName metadata sql-idx)))
     :type-index (.getColumnType metadata sql-idx)
     :class-name (.getColumnClassName metadata sql-idx)
     :scale (.getScale metadata sql-idx)
     :precision (.getPrecision metadata sql-idx)
     :nullable? (.isNullable metadata sql-idx)}))


(defn generic-sql->column
  [^ResultSet rs ^long sql-col-idx]
  (.getObject rs sql-col-idx))


(defn make-convert-sql->column
  [convert-fn]
  (fn [^ResultSet rs ^long sql-col-idx]
    (when-let [item (.getObject rs sql-col-idx)]
      (convert-fn item))))


(defn- sql->column-fn
  [database-name result-set-metadata options]
  (if-let [result-read-fn (get-in options [:parser-fn (:label result-set-metadata)])]
    result-read-fn
    (if-let [result-read-fn (get-in @database-data* [database-name
                                                     (:type-name result-set-metadata)
                                                     :sql->column])]
      result-read-fn
      (if (= (:type-name result-set-metadata) "text")
        (make-convert-sql->column #(Text. (str %)))
        (case (:class-name result-set-metadata)
          "java.sql.Time" (make-convert-sql->column #(.toLocalTime ^java.sql.Time %))
          "java.sql.Date" (make-convert-sql->column #(.toLocalDate ^java.sql.Date %))
          "java.sql.Timestamp" (make-convert-sql->column #(.toInstant ^java.sql.Timestamp %))
          generic-sql->column)))))


(defn- iterate-res-set
  [^ResultSet rs max-batch-idx col-data close? options]
  (let [max-batch-idx (long max-batch-idx)
        parsers (->> col-data
                     (map (fn [{:keys [name read-fn col-idx]}]
                            (let [^PParser parser
                                  (col-parsers/promotional-object-parser name nil)
                                  sql-idx (int (inc col-idx))]
                              (fn
                                ([row-idx]
                                 (let [cval (read-fn)]
                                   (when-not (.wasNull rs)
                                     (.addValue parser row-idx cval))))
                                ([n-rows n-rows]
                                 (-> (.finalize parser n-rows)
                                     (assoc :tech.v3.dataset/name name)))))))
                     (object-array))
        n-cols (alength parsers)]
    (loop [continue? (.next rs)
           local-row-idx 0]
      (when continue?
        (dotimes [col-idx n-cols]
          ((aget parsers col-idx) local-row-idx)))
      (let [next-row-idx (unchecked-inc local-row-idx)]
        (if (and continue? (< (.getRow rs) max-batch-idx))
          (recur (.next rs) next-row-idx)
          (do
            (when (and (not continue?) close?)
              (.close rs))
            (cons (ds/new-dataset
                   options
                   (map #(% next-row-idx next-row-idx) parsers))
                  (lazy-seq
                   (iterate-res-set rs (+ max-batch-idx (long (options :batch-size))) close?
                                    options)))))))))


(defn result-set->dataset-seq
  ([conn-or-db-name ^ResultSet results options]
   (let [batch-size (get options :batch-size 64000)
         metadata (.getMetaData results)
         key-fn (get-in options :key-fn identity)
         options (dissoc options :key-fn)
         database-name (database-name conn-or-db-name)
         close? (get options :close? true)
         column-data
         (->> (range (.getColumnCount metadata))
              (map-indexed (fn [col-idx]
                             (let [res-meta (-> (result-set-metadata->data metadata col-idx)
                                                (update :label key-fn))
                                   res-read-fn (sql->column-fn database-name res-meta)
                                   sql-idx (int (inc col-idx))]
                               {:name (res-meta :label)
                                :col-idx col-idx
                                :read-fn #(res-read-fn results sql-idx)}))))]
     (iterate-res-set results batch-size column-data
                      (assoc options :batch-size batch-size))))
  ([conn-or-db-name results]
   (result-set->dataset-seq conn-or-db-name results nil)))


(defn result-set->dataset
  ([conn-or-db-name ^ResultSet results options]
   (-> (result-set->dataset-seq conn-or-db-name results
                                (assoc options :batch-size Long/MAX_VALUE))
       (first)))
  ([conn-or-db-name ^ResultSet results]
   (result-set->dataset conn-or-db-name results nil)))


(defn sql->dataset-seq
  ([^Connection conn sql options]
   (let [ac (.getAutoCommit conn)
         database-name (database-name conn)]
     (try
       (with-open [statement (.createStatement conn)]
         (let [rs (.executeQuery statement sql)]
           (result-set->dataset-seq database-name rs options)))
       (catch Throwable e
         (.rollback conn)
         (throw e)))))
  ([conn sql]
   (sql->dataset-seq conn sql nil)))


(defn sql->dataset
  ([^Connection conn sql options]
   (let [database-name (database-name conn)]
     (try
       (with-open [statement (.createStatement conn)]
         (let [rs (.executeQuery statement sql)]
           (result-set->dataset database-name rs options)))
       (catch Throwable e
         (.rollback conn)
         (throw e)))))
  ([conn sql]
   (sql->dataset conn sql nil)))
