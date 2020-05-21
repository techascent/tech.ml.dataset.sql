(ns tech.ml.dataset.sql
  (:require [clojure.datafy :as datafy]
            [clojure.data.json :as json]
            [clojure.set :as set]
            [next.jdbc :as jdbc]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.bitmap :as bitmap]
            [tech.v2.datatype.datetime :as dtype-dt]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype :as dtype]
            [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset.impl.dataset :as ds-impl]
            [tech.ml.dataset.impl.column :as col-impl]
            [tech.ml.dataset.sql.impl :as sql-impl])
  (:import [java.util List]
           [org.roaringbitmap RoaringBitmap]
           [java.time Instant]
           [java.sql
            Connection
            ResultSetMetaData
            PreparedStatement
            DatabaseMetaData
            ResultSet]))


(set! *warn-on-reflection* true)


(defn result-set->dataset
  "Given a result set, return a dataset.
  options -
  :close? - if true, then .close is called on the resultset - always - including when
  there is an exception.  Defaults to true."
  ([^ResultSet results {:keys [close?]
                        :or {close? true}
                        :as options}]
   (try
     (let [columns (->> (sql-impl/result-set-metadata->data (.getMetaData results))
                        (map-indexed
                         (fn [idx {:keys [datatype name]}]
                           (let [container (col-impl/make-container datatype)
                                 missing (bitmap/->bitmap)]
                             {:name name
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
  statement to a dataset.
  For options, see result-set->dataset"
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

  conn - java.sql.Connection
  dataset - string, keyword, symbol, or dataset."
  [conn dataset]
  (try
    (sql->dataset conn (format "Select COUNT(*) from %s where 1 = 0"
                               (sql-impl/dataset->table-name dataset)))
    true
    (catch Throwable e
      false)))


(defn drop-table!
  "Drop a table.  Exception upon failure to drop the table.

  conn - java.sql.Connection
  dataset - string, keyword, symbol, or dataset."
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

  conn - java.sql.Connection
  dataset - dataset to use.  The dataset-name will be used as the table-name and the
     column names and datatypes will be used for the sql names and datatypes.
  The dataset's dataset-name will be used for the table and if the dataset's metadata
  has a :primary-key member this will be interpreted as a sequence of column names
  to be used as the primary key.

  options
  :table-name - set the name of the table to use.  Overrides the dataset metadata.
  :primary-key - Array of column names to use as the primary key of this table.
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
         n-cols (ds/column-count dataset)
         sql
         (apply str "CREATE TABLE "
                table-name
                " (\n"
                (concat
                 (->> dataset
                      (map-indexed (fn [idx column]
                                     (let [colmeta (meta column)
                                           colname (sql-impl/->str (:name colmeta))
                                           col-dtype (sql-impl/datatype->sql-datatype
                                                      (:datatype colmeta))]
                                       (if-not (== idx (dec n-cols))
                                         [" " colname " " col-dtype ",\n"]
                                         [" " colname " " col-dtype]))))
                      (apply concat))
                 (when (seq primary-key)
                   (concat [",\n PRIMARY KEY ("]
                           (interpose ", "
                                      (map sql-impl/->str primary-key))
                           [")"]))
                 "\n);"))]
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


(defn insert-dataset!
  "Insert a dataset into a table indicated by the dataset name.
  options
  :postgres-upsert? - defaults to false.  When true, generates postgres-specific sql
  that affects an upsert operation."
  ([^Connection conn dataset options]
   (sql-impl/execute-prepared-statement-batches
    conn (sql-impl/db-insert-sql dataset options) dataset options))
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
