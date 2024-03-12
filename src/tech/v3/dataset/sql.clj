(ns tech.v3.dataset.sql
  "Pathways to transform dataset to and from SQL databases.  Built directly on
  java.sql interfaces.  Generically SQL doesn't support very many datatypes but
  an effort has been made to enable database-specific support for these base datatypes:

  * Integer Types - `:int16`, `:int32`, `:int64`
  * Float Types - `:float32`, `:float64`
  * String, Text
  * LocalDate (date), Instant (database specific, defaults to timestamp), LocalTime (time).
  * UUID (uuid in postgres, uniqueidentifier in sql server)


  Users can serialize result sets into one dataset or a sequence of datasets depending
  on their downstream use case.


## Column Metadata For Writing To The Database

  For creating tables and uploading data to the database column metadata may be used
  to extend how the system writes data.

  * `:sql-datatype` - Change the sql datatype used during create-table.  This string
  will be written in verbatim into the create-table sql.
  * `:column->sql` - pair of *integer* sql type and function that takes a column and row and
  must return a datatype in the SQL space of `sql-datatype`.  To find the integer sql type
  see the [[tech.v3.datatset.sql.datatypes]] namespace or use [[database-type-table]] to find
  the type table for your particular database.
  * `:insert-sql` - When creating the prepared statement the default will be to use `?` but
  users can override this to setup specialized types in the prepared statement.  For example
  the postgres UUID type requires `? ::UUID` in order to correctly serialize.


## Metadata For Reading A ResultSet

  When reading the result set the normal database option may be used such as `key-fn` and
  `dataset-name`.  In addition, similar to parsing datasets via `->dataset` users can pass
  in a keyed map as `:parser-fn` in which case the keys must match the post-key-fn'd
  result column name.  Entries in `parser-fn` must be tuples of
  `[datatype result-set-read-fn]` where `result-set-read-fn` takes a resultset and a column
  index and returns data of the appropriate datatype.

  If you are unsure of the datatype
  you can return `nil` for the datatype and the system will divine the datatype to use
  from the return value of `result-set-read-fn`.  This has the drawback that columns
  with all missing values will be `:boolean` columns as per the dataset default and thus
  this may break round-tripping.



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
            [tech.v3.dataset.sql.datatypes :as sql-datatypes]
            [clojure.tools.logging :as log])
  (:import [java.util List UUID]
           [org.roaringbitmap RoaringBitmap]
           [java.time Instant LocalDate LocalTime]
           [java.sql Connection ResultSetMetaData PreparedStatement
            DatabaseMetaData ResultSet ParameterMetaData Timestamp Time Date]
           [tech.v3.dataset Text]))


(set! *warn-on-reflection* true)


(defn postgre-sql-connect-str
  ^String [hoststr database user pwd]
  (format "jdbc:postgresql://%s/%s?user=%s&password=%s"
    hoststr database user pwd))


(defn sql-server-connect-str
  ^String [hoststr database user pwd]
  (format "jdbc:sqlserver://%s;databaseName=%s;user=%s;password=%s"
          hoststr database user pwd))


(defn database-name
  "Return the database name for this connection."
  [conn]
  (if (string? conn)
    conn
    (let [^Connection conn conn]
      (-> conn
          (.getMetaData)
          (.getDatabaseProductName)))))


(declare result-set->dataset)


(defn database-type-table
  "Return table mapping sql datatype to sql datatype integer for all datatypes
  this database supports."
  [^Connection conn]
  (-> (result-set->dataset conn (-> (.getMetaData conn)
                                    (.getTypeInfo)))
      (ds/select-columns ["TYPE_NAME" "DATA_TYPE"])))


(def ^:private default-sql-types {:int8 "tinyint"
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


(defn- default-sql-datatype
  [dt-kwd]
  (if-let [retval (get default-sql-types dt-kwd)]
    retval
    (throw (Exception. (format "Failed to find default sql datatype for %s"
                               dt-kwd)))))


(defn generic-column->sql
  "Generic function that reads a value from a column."
  [col idx]
  (col idx))


(defn make-convert-column->sql
  [convert-fn]
  (fn [col idx]
    (when-let [retval (col idx)]
      (convert-fn retval))))


(defn generic-sql->column
  "Generic function that reads a column from a result set."
  [^ResultSet rs ^long sql-col-idx]
  (.getObject rs sql-col-idx))


(defn make-convert-sql->column
  [convert-fn]
  (fn [^ResultSet rs ^long sql-col-idx]
    (when-let [item (.getObject rs sql-col-idx)]
      (convert-fn item))))


(def ^:private database-data*
  (atom
   ;;postgre has no byte column type
   {"PostgreSQL"
    {:uint8 :int16
     :int8 :int16
     :uint16 :int32
     :uint32 :int64
     :string "varchar"
     :uuid {:sql-datatype "uuid"
            ;;prepared statement insert sql
            :insert-sql "? ::UUID"}
     "int2" {:sql->column [:int16 #(.getShort ^ResultSet %1 (unchecked-int %2))]}}

    "Microsoft SQL Server"
    {:uint8 :int16
     :uint16 :int32
     :uint32 :int64
     :instant "datetime2"
     :uuid {:sql-datatype "uniqueidentifier"}
     "uniqueidentifier" {:column->sql [(sql-datatypes/type-index "char")
                                       #(str (%1 %2))]
                         :sql->column [:uuid (make-convert-sql->column
                                              #(UUID/fromString (str %)))]}
     "datetime" {:column->sql [(sql-datatypes/type-index "datetime")
                               #(Timestamp/from ^Instant (%1 %2))]}
     "datetime2" {:column->sql [(sql-datatypes/type-index "datetime2")
                                #(Timestamp/from ^Instant (%1 %2))]}}
    }))


(defn set-datatype-mapping!
  "Add a database specific datatype mapping.

  * `database-name` - name of database
  * `datatype` - dtype-next datatype
  * `sql-datatype` - sql datatype to to map to.
  * `sql-type-index` - type index to use to set missing.  See [[datatype-type-table]]
    for a way to find the sql type index from the sql datatype.
  * `result-set-read-fn` - function that takes a result set and a column index and returns
  the data in dtype-next space e.g. a `java.time.LocalTime` object as opposed
  to a `java.sql.Time` object.
  * `col-read-fn` - Function takes a column and a row idx and returns data as an sql datatype
  e.g. a `java.sql.Time` object as opposed to a `java.time.LocalTime` object.


  Your type should be a proper tech.v3.datatype datatype meaning it is either an existing
  datatype or added via tech.v3.datatype.casting/add-object-datatype!.


  Example:

  For our example let's say we would like to support postgreSQL's jsonb datatype.  We first
  create a record type to denote json data and register it with dtype-next.  Next we build
  a dataset with a column of this datatype.

  Next we register a way to input and get back this data from sql.  After that columns of
  the new datatype will be automatically marshalled into/out of sql.

```clojure
user> (def conn (tech.v3.dataset.sql-test-utils/connect :postgre-sql))
#'user/conn
user> conn
#object[org.postgresql.jdbc.PgConnection 0x421726aa \"org.postgresql.jdbc.PgConnection@421726aa\"]
user> (require '[tech.v3.dataset :as ds])
nil
user> (require '[tech.v3.datatype.casting :as casting])
nil
user> (require '[tech.v3.dataset.sql :as sql])
nil
user> (require '[clojure.data.json :as json])
nil
user> (defrecord JSONData [json-data])
JSONData
user> (casting/add-object-datatype! :json-data JSONData)
:ok
user> (def json-type-index (-> (sql/database-type-table conn)
                              (ds/filter-column \"TYPE_NAME\" #(= % \"jsonb\"))
                              (ds/row-at -1)
                              (get \"DATA_TYPE\")))
#'user/json-type-index
user> json-type-index
1111
user> (sql/database-name conn)
\"PostgreSQL\"
user> (sql/set-datatype-mapping!
       \"PostgreSQL\" :json-data \"jsonb\" json-type-index
       (fn [^java.sql.ResultSet rs col-idx]
         (when-let [json-obj (.getObject rs col-idx)]
           (-> json-obj
               (str)
               (json/read-str :key-fn keyword)
               (JSONData.))))
       (fn [col idx]
         (when-let [col-obj (col idx)]
           (-> col-obj
               (:json-data)
               (json/write-str)))))
:ok
user> (def table-name \"jsontable\")
#'user/table-name
user> (def test-ds (ds/->dataset {:jsoncol [(JSONData. {:a 1 :b 2})
                                            (JSONData. {:c 2 :d 3})]}
                                    {:dataset-name table-name}))
#'user/test-ds
user> test-ds
jsontable [2 1]:

|                  :jsoncol |
|---------------------------|
| {:json-data {:a 1, :b 2}} |
| {:json-data {:c 2, :d 3}} |
user> (test-ds :jsoncol)
#tech.v3.dataset.column<json-data>[2]
:jsoncol
[user.JSONData@db1bf962, user.JSONData@db1bf861]
user> (sql/create-table! conn test-ds)
nil
user> (sql/insert-dataset! conn test-ds)
nil
user> (sql/sql->dataset conn (str \"select * from \" table-name)
                        {:key-fn keyword})
_unnamed [2 1]:

|                  :jsoncol |
|---------------------------|
| {:json-data {:a 1, :b 2}} |
| {:json-data {:c 2, :d 3}} |
```"
  [database-name datatype sql-datatype sql-type-index
   result-set-read-fn col-read-fn]
  (swap! database-data*
         (fn [db-data-map]
           (-> db-data-map
               (assoc-in [database-name datatype :sql-datatype] sql-datatype)
               (assoc-in [database-name sql-datatype] {:column->sql
                                                       [sql-type-index col-read-fn]
                                                       :sql->column
                                                       [datatype result-set-read-fn]}))))
  :ok)


(set-datatype-mapping! "PostgreSQL" :boolean "bool" -7
                       generic-sql->column
                       generic-column->sql)


(set-datatype-mapping! "Microsoft SQL Server" :boolean "bit" -7
                       generic-sql->column
                       generic-column->sql)


(defn- column-metadata->sql-datatype
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


(defn ^:no-doc sanitize
  ^String [item]
  (-> (->str item)
      (.replace "-" "_")))


(defn ^:no-doc table-name
  (^String [dataset options]
   (if (string? dataset)
     dataset
     (sanitize (or (:table-name options) (get (meta dataset) :name)))))
  ([dataset]
   (table-name dataset nil)))


(defn ^:no-doc primary-key
  ([dataset options]
   (when (nil? dataset)
     (throw (Exception. "No dataset provided")))
   (let [pk (or (:primary-key options) (get (meta dataset) :primary-key))]
     (if (or (string? pk) (not (seqable? pk)))
       [(sanitize pk)]
       (map sanitize pk))))
  ([dataset]
   (primary-key dataset nil)))


(defn ^:no-doc column-metadata
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
    [conn dataset & [options]]
    (try
      (sql->dataset conn (format "Select COUNT(*) from %s where 1 = 0"
                                 (table-name dataset options)))
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
   (if-not (table-exists? conn dataset options)
     (do
       (create-table! conn dataset options)
       true)
     false))
  ([^Connection conn dataset]
   (ensure-table! conn dataset {})))




(defn- column-metadata->insert-sql
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


(defn- sql-datatype->column-read-fn
  [database-name sql-datatype]
  (if-let [read-fn (get-in @database-data* [database-name sql-datatype :column->sql])]
    read-fn
    [(sql-datatypes/type-index sql-datatype)
     (case sql-datatype
       "text" (make-convert-column->sql str)
       "date" (make-convert-column->sql #(java.sql.Date/valueOf ^LocalDate %))
       "time" (make-convert-column->sql #(java.sql.Time/valueOf ^LocalTime %))
       "timestamp" (make-convert-column->sql #(java.sql.Timestamp/from ^Instant %))
       generic-column->sql)]))


(defn- column->sql-fn
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

  See namespace documentation about column metadata and various options that may be
  associated on a per-column basis to change the way data is uploaded.

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
                              cmeta (meta col)
                              [sql-type-index read-fn] (column->sql-fn database-name cmeta)
                              sql-type-index (int sql-type-index)
                              cname (:name cmeta)
                              missing (ds/missing col)]
                          #(try
                             (let [row-idx (int %)]
                               (if (.contains missing row-idx)
                                 (.setNull stmt sql-idx sql-type-index)
                                 (.setObject stmt sql-idx (read-fn col row-idx)
                                             sql-type-index)))
                             (catch Throwable e
                               (throw (ex-info (format "Failed to insert column %s" cname)
                                               {:error e})))))))
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
  "Insert a dataset into a table.  Column datatypes will be inferred from the
  dataset datatypes or may be provided as metadata on each column.  See namespace
  documentation for `:column->sql`."
  ([conn dataset options]
   (execute-prepared-statement-batches! conn
                                        (insert-sql conn dataset options)
                                        dataset options))
  ([conn dataset]
   (insert-dataset! conn dataset nil)))


(defn result-set-metadata->data
  "Given result set metadata and a column index return the metadata.  Note that the
  result set metadata for a particular column is save as :result-set-metadata in the
  column's metadata."
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


(defn- sql->column-fn
  [database-name result-set-metadata options]
  (if-let [result-read-fn (get-in options [:parser-fn (:label result-set-metadata)])]
    result-read-fn
    (if-let [result-read-fn (get-in @database-data* [database-name
                                                     (:type-name result-set-metadata)
                                                     :sql->column])]
      result-read-fn
      (if (= (:type-name result-set-metadata) "text")
        [:text (make-convert-sql->column #(Text. (str %)))]
        (case (:class-name result-set-metadata)
          "java.sql.Time"
          [:packed-local-time (make-convert-sql->column #(.toLocalTime ^java.sql.Time %))]
          "java.sql.Date"
          [:packed-local-date (make-convert-sql->column #(.toLocalDate ^java.sql.Date %))]
          "java.sql.Timestamp"
          [:packed-instant (make-convert-sql->column #(.toInstant ^java.sql.Timestamp %))]
          "java.lang.String" [:string generic-sql->column]
          "java.lang.Byte" [:int8 generic-sql->column]
          "java.lang.Short" [:int16 generic-sql->column]
          "java.lang.Integer" [:int32 generic-sql->column]
          "java.lang.Long" [:int64 generic-sql->column]
          "java.lang.Float" [:float32 generic-sql->column]
          "java.lang.Double" [:float64 generic-sql->column]
          [nil generic-sql->column])))))


(defn- iterate-res-set
  [^ResultSet rs max-batch-idx col-data options]
  (let [max-batch-idx (long max-batch-idx)
        close? (get options :close? true)
        parsers
        (->> col-data
             (map (fn [{:keys [name read-fn col-idx datatype
                               result-set-metadata]}]
                    (let [parser
                          (if datatype
                            (col-parsers/make-fixed-parser name datatype nil)
                            (col-parsers/promotional-object-parser name nil))
                          sql-idx (int (inc col-idx))]
                      (fn
                        ([row-idx]
                         (let [cval (read-fn)]
                           (when-not (.wasNull rs)
                             (col-parsers/add-value! parser row-idx cval))))
                        ([n-rows n-rows]
                         (-> (col-parsers/finalize! parser n-rows)
                             (assoc :tech.v3.dataset/name name
                                    :tech.v3.dataset/metadata {:result-set-metadata
                                                               result-set-metadata})))))))
             (object-array))
        n-cols (alength parsers)]
    (loop [continue? (.next rs)
           local-row-idx 0]
      (when continue?
        (dotimes [col-idx n-cols]
          ((aget parsers col-idx) local-row-idx)))
      (if (and continue? (< (.getRow rs) max-batch-idx))
        (recur (.next rs) (unchecked-inc local-row-idx))
        (cons (ds/new-dataset
               options
               (map #(% local-row-idx local-row-idx) parsers))
              (if continue?
                (lazy-seq
                 (iterate-res-set rs (+ max-batch-idx (long (options :batch-size)))
                                  col-data
                                  options))
                (do
                  (when close?
                    (.close rs)
                    (when-let [^java.lang.AutoCloseable stmt (:statement options)]
                      (.close stmt))))))))))


(defn result-set->dataset-seq
  "Given a result set return a sequence of datasets.  Each dataset will be at most
  `:batch-size` num-rows in length.  Note that the statement will stay open as long as
  the lazy sequence isn't fully realized which means if you do not realize the sequence
  you will leave a hanging statement open.  There is currently no way to prematurely
  end the dataset sequence and close the sql statement via the public API.

  Options:

  * `:batch-size` - defaults to 64000.  Returned datasets will be at most this number of
  rows long.
  * `:key-fn` - Apply this to the column names -- an example would be `keyword`.
  * `:parser-fn` - Map of post-key-fn column-name to tuple of `[datatype result-set-read-fn]`
  where result-set-read-fn takes a result-set and a column-index and must return data
  of the appropriate datatype.  If the datatype is unknown then nil can be provided and the
  dataset will use the widest datatype that will fit the returned data or `:boolean` if all
  values are missing.
  * `:statement` - The sql statement to close when finished.
  * `:close?` - Close the result set when finished.  Defaults to true."
  ([conn-or-db-name ^ResultSet results options]
   (let [batch-size (get options :batch-size 64000)
         metadata (.getMetaData results)
         key-fn (get options :key-fn identity)
         options (dissoc options :key-fn)
         database-name (database-name conn-or-db-name)
         column-data
         (->> (range (.getColumnCount metadata))
              (map (fn [col-idx]
                     (let [res-meta (-> (result-set-metadata->data metadata col-idx)
                                        (update :label key-fn))
                           [datatype res-read-fn]
                           (sql->column-fn database-name res-meta options)
                           sql-idx (int (inc col-idx))]
                       {:name (res-meta :label)
                        :col-idx col-idx
                        :result-set-metadata res-meta
                        :datatype datatype
                        :read-fn #(res-read-fn results sql-idx)}))))]
     (iterate-res-set results batch-size column-data
                      (assoc options :batch-size batch-size))))
  ([conn-or-db-name results]
   (result-set->dataset-seq conn-or-db-name results nil)))


(defn result-set->dataset
  "Given a result set return a dataset that serialized the entire resultset into
  one dataset.  See options for [[result-set->dataset-seq]]."
  ([conn-or-db-name ^ResultSet results options]
   (-> (result-set->dataset-seq conn-or-db-name results
                                (assoc options :batch-size Long/MAX_VALUE))
       (first)))
  ([conn-or-db-name ^ResultSet results]
   (result-set->dataset conn-or-db-name results nil)))


(defn sql->dataset-seq
  "Given a connection and an sql statement return a sequence of datasets.
  See options for [[result-set->dataset-seq]]."
  ([^Connection conn sql options]
   (let [ac (.getAutoCommit conn)
         database-name (database-name conn)]
     (try
       (let [statement (.createStatement conn)]
         (let [rs (.executeQuery statement sql)]
           (result-set->dataset-seq database-name rs (assoc options :statement statement))))
       (catch Throwable e
         (try
           (.rollback conn)
           (catch Throwable e nil))
         (throw e)))))
  ([conn sql]
   (sql->dataset-seq conn sql nil)))


(defn sql->dataset
  "Given a connection and an sql statement return a single dataset.
  See options for [[result-set->dataset-seq]]."
  ([^Connection conn sql options]
   (let [database-name (database-name conn)]
     (try
       (with-open [statement (.createStatement conn)]
         (let [rs (.executeQuery statement sql)]
           (result-set->dataset database-name rs options)))
       (catch Throwable e
         (try
           (.rollback conn)
           (catch Throwable e nil))
         (throw e)))))
  ([conn sql]
   (sql->dataset conn sql nil)))
