(ns tech.v3.dataset.sql
  "Pathways to transform dataset to and from SQL databases.  Built directly on
  java.sql interfaces."
  (:require [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.impl.column-base :as col-base]
            [tech.v3.dataset.sql.impl :as sql-impl])
  (:import [java.util List]
           [org.roaringbitmap RoaringBitmap]
           [java.time Instant]
           [java.sql Connection ResultSetMetaData PreparedStatement
            DatabaseMetaData ResultSet]))


(set! *warn-on-reflection* true)

(defonce crap-atom (atom nil))

(defn result-set->dataset
  "Given a result set, return a dataset.

  Options:

  * `:close?` - if true, then .close is called on the resultset - always - including when
    there is an exception.  Defaults to true."
  ([^ResultSet results {:keys [close?]
                        :or {close? true}
                        :as options}]
   (reset! crap-atom (.getMetaData results))
   (try
     (let [columns (->> (sql-impl/result-set-metadata->data (.getMetaData results))
                        (map-indexed
                         (fn [idx {:keys [datatype name label]}]
                           (let [container (col-base/make-container datatype)
                                 missing (bitmap/->bitmap)]
                             {:name label
                              :datatype datatype
                              :data container
                              :missing missing
                              :parse-fn (sql-impl/make-read-fn results datatype
                                                               container missing
                                                               (inc idx))}))))
           parse-fns (mapv :parse-fn columns)]
       (loop [continue? (.next results)]
         (when continue?
           (let [row-idx (.getRow results)]
             (doseq [parse-fn parse-fns]
               (parse-fn row-idx)))
           (recur (.next results))))
       (ds-impl/new-dataset options columns))
     (finally
       (when close? (.close results)))))
  ([results]
   (result-set->dataset results {})))


(defn sql->dataset
  "Given a connection and an sql statement, convert the results of executing the
  statement to a dataset. For options, see `result-set->dataset`"
  ([^Connection conn sql options]
   (try
     (with-open [statement (.createStatement conn)]
       (-> (.executeQuery statement sql)
           (result-set->dataset options)))
     (catch Throwable e
       (.rollback conn)
       (throw e))))
  ([^Connection conn sql]
   (sql->dataset conn sql {})))


(defn sanitize-dataset-names-for-sql
  "Given a dataset, sanitize the dataset name and the names of all the columns
  such that they are safe for insert to SQL."
  [ds]
  (-> (ds/rename-columns ds (->> (ds/column-names ds)
                                 (map (fn [cname]
                                        [cname (sql-impl/sanitize-name-for-sql cname)]))
                                 (into {})))
      (ds/set-dataset-name
       (-> (ds/dataset-name ds)
           (sql-impl/sanitize-name-for-sql)))))


(defn table-exists?
  "Test if a table exists.

  * conn - java.sql.Connection
  * dataset - string, keyword, symbol, or dataset"
  [conn dataset]
  (try
    (sql->dataset conn (format "Select COUNT(*) from %s where 1 = 0"
                               (sql-impl/dataset->table-name dataset)))
    true
    (catch Throwable e
      false)))


(defn drop-table!
  "Drop a table.  Exception upon failure to drop the table.

  * conn - java.sql.Connection
  * dataset - string, keyword, symbol, or dataset"
  [conn dataset]
  (sql-impl/execute-update! conn (format "DROP TABLE %s"
                                         (sql-impl/dataset->table-name dataset))))


(defn drop-table-when-exists!
  "Drop the table indicated by this dataset if it exists."
  [conn dataset]
  (when (table-exists? conn dataset)
    (drop-table! conn dataset)))


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
     Overrides the dataset metadata."
  ([^Connection conn dataset options]
   (let [table-name (sql-impl/->str (or (:table-name options)
                                        (sql-impl/dataset->table-name dataset)))
         primary-key (or (:primary-key options)
                         (:primary-key (meta dataset)))
         primary-key (when primary-key
                       (cond
                         (string? primary-key) [primary-key]
                         (seq primary-key) primary-key
                         :else [primary-key]))
         sql (sql-impl/create-sql dataset table-name primary-key)]
     (sql-impl/execute-update! conn sql)))
  ([conn dataset]
   (create-table! conn dataset {})))


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



(defn execute-prepared-statement-batches
  "Internal method to, using a dataset, execute prepared statement batches
  drawn from the rows of the dataset.  For this to work correctly, the
  connection needs to have autoCommit set to false.

  * conn - java.sql.Connection
  * stmt-or-sql - Either a prepared statement or a string in which case
    the connection's .prepareStatement method will be called.
  * dataset - dataset

  Options

  - batch-size - integer, defaults to 32
  "
  [^Connection conn stmt-or-sql dataset options]
  (let [n-rows (ds/row-count dataset)
        batch-size (long (or (:batch-size options) 32))]
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
        (let [inserters (->> (ds/columns dataset)
                             (map-indexed
                              (fn [idx col]
                                (sql-impl/make-prep-statement-applier
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


(defn insert-dataset!
  "Insert a dataset into a table indicated by the dataset name.

Options:

  - `:postgres-upsert?` - defaults to false.  When true, generates postgres-specific sql
     that performs an upsert operation."
  ([^Connection conn dataset options]
   (execute-prepared-statement-batches
    conn (sql-impl/insert-sql dataset options) dataset options))
  ([conn dataset]
   (insert-dataset! conn dataset {})))


(comment
  (def conn (doto (-> (sql-impl/jdbc-postgre-connect-str
                       "localhost:5432" "dev-user" "dev-user" "unsafe-bad-password")
                      (jdbc/get-connection {:auto-commit false}))
              (.setCatalog "dev-user")))

  (def conn-meta (.getMetaData conn))

  (def prepared-stmt (jdbc/prepare
                      conn
                      [(format "Select * from %s" "pg_catalog.pg_tables")]))

  (def metadata (.getMetaData prepared-stmt))

  (def stocks (-> (ds/->dataset "test/data/stocks.csv"
                                {:key-fn keyword})
                  (#(with-meta %
                      (assoc (meta %)
                             :name "STOCKS"
                             :primary-keys ["date" "symbol"])))))

  (create-table conn stocks)

  (insert-dataset! conn stocks)


  (insert-dataset! conn stocks) ;;error
  (require '[tech.v2.datatype.functional :as dfn])
  (def new-stocks (ds/update-column stocks :price (partial dfn/* 2.0)))
  ;;Update or insert all the values
  (insert-dataset! conn new-stocks {:postgres-upsert? true})
  (def sql-ds (sql->dataset conn "SELECT * from stocks" {:key-fn keyword}))
  (def local-date-ds
    (ds/column-cast sql-ds :date
                    [:local-date
                     #(-> %
                          (dtype-dt/instant->milliseconds-since-epoch)
                          (dtype-dt/milliseconds-since-epoch->local-date))]))




  )
