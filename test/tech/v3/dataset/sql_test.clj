(ns tech.v3.dataset.sql-test
  (:require [tech.v3.dataset :as ds]
            [tech.v3.dataset.sql :as sql]
            ;; [tech.v3.dataset.sql.impl :as sql-impl]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.dataset.sql-test-utils :refer [def-db-test dev-conn] :as sql-utils]
            [clojure.data.json :as json]
            [next.jdbc :as jdbc]
            [clojure.test :refer [deftest is]])
  (:import [java.util UUID]
           [tech.v3.dataset Text]
           [java.time LocalTime]))


(defn- uuid-table-name
  []
  (-> (UUID/randomUUID)
      (str)
      (#(.replace ^String % "-" "_"))
      (#(str "aa" %))))


(defn supported-datatype-ds
  []
  (-> (ds/->dataset {:boolean [true false true true false false true false false true]
                     :bytes (byte-array (range 10))
                     :shorts (short-array (range 10))
                     :ints (int-array (range 10))
                     :longs (long-array (range 10))
                     :floats (float-array (range 10))
                     :doubles (double-array (range 10))
                     :strings (map str (range 10))
                     :text (map (comp #(Text. %) str) (range 10))
                     :uuids (repeatedly 10 #(UUID/randomUUID))
                     :instants (repeatedly dtype-dt/instant)
                     ;;sql doesn't support dash-case
                     :local_dates (repeatedly dtype-dt/local-date)
                     ;;some sql engines (or the jdbc api) don't support more than second
                     ;;resolution for sql time objects
                     :local_times (->> (repeatedly dtype-dt/local-time)
                                       (map (fn [^LocalTime lt]
                                              (LocalTime/ofSecondOfDay
                                               (.toSecondOfDay lt)))))})
      (vary-meta assoc
                 :primary-key :uuids
                 :name :testtable)))


(defmacro with-temp-table
  [table-var & code]
  `(let [~table-var (uuid-table-name)]
     (try
       ~@code
       (finally
         (try
           (sql/drop-table! (dev-conn) ~table-var)
           (catch Throwable e# nil))))))


(def-db-test base-datatype-test
  (with-temp-table table-name
    (let [ds (-> (supported-datatype-ds)
                 (vary-meta assoc :name table-name))]
      (sql/create-table! (dev-conn) ds)
      (sql/insert-dataset! (dev-conn) ds)
      (let [sql-ds (-> (sql/sql->dataset (dev-conn)
                                         (format "Select * from %s" table-name)
                                         {:key-fn keyword})
                       (ds/sort-by-column :longs))]
        (doseq [column (vals ds)]
          (is (= (vec column)
                 (vec (sql-ds (:name (meta column)))))))))))


(def-db-test stocks-dataset
  (with-temp-table table-name
    (let [stocks (-> (ds/->dataset "test/data/stocks.csv")
                     (vary-meta assoc
                                :name table-name
                                :primary-keys ["date" "symbol"]))]
      (sql/create-table! (dev-conn) stocks)
      (sql/insert-dataset! (dev-conn) stocks)
      (let [sql-stocks (sql/sql->dataset
                        (dev-conn) (format "Select * from %s"
                                           table-name))
            stocks (ds/sort-by stocks
                               #(vector
                                 (get % "date")
                                 (get % "symbol")))
            sql-stocks (ds/sort-by sql-stocks
                                   #(vector
                                     (get % "date")
                                     (get % "symbol")))]
        (is (= (ds/row-count sql-stocks)
               (ds/row-count stocks)))
        (is (dfn/equals (stocks "price") (sql-stocks "price")))))))


(def-db-test small-missing
  (with-temp-table table-name
    (let [test-ds (ds/->dataset [{:a 1 :b 2}
                                 {:b 3}
                                 {:a 4}]
                                {:dataset-name table-name})]
      (sql/create-table! (dev-conn) test-ds)
      (sql/insert-dataset! (dev-conn) test-ds)
      (let [sql-ds (sql/sql->dataset
                    (dev-conn) (format "Select * from %s"
                                       (ds/dataset-name test-ds)))]
        (is (= (ds/row-count sql-ds)
               (ds/row-count test-ds)))
        (is (= (ds/missing test-ds)
               (ds/missing sql-ds)))
        (is (= (vec (test-ds :a))
               (vec (sql-ds "a"))))
        (is (= (vec (test-ds :b))
               (vec (sql-ds "b"))))))))


(def-db-test ames-ds
  (with-temp-table table-name
    (let [test-ds (-> (ds/->dataset "test/data/ames-train.csv.gz"
                                    {:dataset-name table-name})
                      (ds/select-rows (range 20))
                      ;;Append an 'a_' to the start of every column name because
                      ;;these names start with numbers
                      (#(ds/select-columns
                         % (->> (ds/column-names %)
                                (map (fn [^String cname]
                                       [cname (str "a_" (.toLowerCase cname))]))
                                (into {})))))
          n-rows (ds/row-count test-ds)]
      (sql/create-table! (dev-conn) test-ds)
      (sql/insert-dataset! (dev-conn) test-ds)
      (let [sql-ds (sql/sql->dataset (dev-conn)
                                     (format "Select * from %s"
                                             (ds/dataset-name test-ds)))]
        (is (= (ds/row-count test-ds)
               (ds/row-count sql-ds)))
        (doseq [col (ds/columns test-ds)]
          (let [cname (ds-col/column-name col)
                sql-col (sql-ds cname)
                col-dtype (dtype/get-datatype col)]
            (is (= (ds-col/missing col)
                   (ds-col/missing sql-col))
                (format "Missing for column %s" cname))
            (is (= col-dtype
                   (dtype/get-datatype sql-col))
                (format "Datatype for column %s" cname))
            (let [src-rdr col
                  dst-rdr sql-col]
              (if (casting/numeric-type? col-dtype)
                (is (dfn/equals (dtype/->array :float64 {:nan-strategy :remove} src-rdr)
                                (dtype/->array :float64 {:nan-strategy :remove} dst-rdr))
                    (format "Numeric equals for column %s" cname))
                (is (= (vec src-rdr)
                       (vec dst-rdr))
                    (format "Object equals for column %s" cname))))))))))


(def-db-test ensure-take-table-name-opt
  (let [table-name (uuid-table-name)
        ds (ds/->dataset {:a 1} {:dataset-name "_unnamed"})
        ensure (sql/ensure-table! (dev-conn) ds {:table-name table-name})]
    (is (= ensure true))))


(def-db-test batch-size
  (with-temp-table table-name
    (let [test-ds (ds/->dataset {:a (range 100)}
                                {:dataset-name table-name})]
      (sql/create-table! (dev-conn) test-ds)
      (sql/insert-dataset! (dev-conn) test-ds)
      (let [ds-seq (sql/sql->dataset-seq (dev-conn) (str "select * from " table-name)
                                         {:batch-size 25
                                          :key-fn keyword})
            full-ds (apply ds/concat ds-seq)]
        ;;Due to iterator design returns one empty dataset.
        (is (= 5 (count ds-seq)))
        (is (= 100 (ds/row-count full-ds)))
        (is (= (vec (test-ds :a))
               (vec (full-ds :a))))))))


;;Postgre specific test

(defrecord JSONData [json-data])
(casting/add-object-datatype! :json-data JSONData)


(deftest jsonb
  (sql-utils/with-connection :postgre-sql
    (let [json-type-index (-> (sql/database-type-table (dev-conn))
                              (ds/filter-column "TYPE_NAME" #(= % "jsonb"))
                              (ds/row-at -1)
                              (get "DATA_TYPE"))]
      (when-not json-type-index
        (throw (Exception. "Failed to find json type index")))

      (sql/set-datatype-mapping!
       "PostgreSQL" :json-data "jsonb" json-type-index
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

      (with-temp-table table-name
        (let [test-ds (ds/->dataset {:jsoncol [(JSONData. {:a 1 :b 2})
                                                (JSONData. {:c 2 :d 3})]}
                                    {:dataset-name table-name})]
          (sql/create-table! (dev-conn) test-ds)
          (sql/insert-dataset! (dev-conn) test-ds)
          (let [nds (sql/sql->dataset (dev-conn) (str "select * from " table-name)
                                      {:key-fn keyword})]
            (is (= (vec (test-ds :jsoncol))
                   (vec (nds :jsoncol))))))))))


(comment

  (def datasource (jdbc/get-datasource
                   {:dbtype   "postgres"
                    :dbname   "aact"
                    :host     "aact-db.ctti-clinicaltrials.org"
                    :user     "chrisn"
                    :password "azBA0IlZZG6T"
                    :port     "5432"}))
  (def conn
    (doto (jdbc/get-connection datasource {:auto-commit false, :read-only true})
      (.setCatalog "postgres")))

  (def studies (sql/sql->dataset conn "table studies limit 501"))


  (def response-data-both (sql/sql->dataset conn "
select sponsors.name,
(count(*) FILTER (WHERE extract('year' from AGE(date_trunc('year', current_date),
                              date_trunc('year', studies.study_first_submitted_date))) <= 2) / 3) as \"AvgPerYearSubmitted\",
(count(*) FILTER (WHERE extract('year' from AGE(date_trunc('year', current_date),
                              date_trunc('year', studies.start_date))) <= 2) / 3) as \"AvgPerYearStarted\"
from sponsors
left join studies on sponsors.nct_id = studies.nct_id
group by sponsors.name
order by \"AvgPerYearSubmitted\" DESC
LIMIT 100"))
  )
