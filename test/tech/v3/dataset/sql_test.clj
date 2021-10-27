(ns tech.v3.dataset.sql-test
  (:require [tech.v3.dataset :as ds]
            [tech.v3.dataset.sql :as sql]
            [tech.v3.dataset.sql.impl :as sql-impl]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype :as dtype]
            [clojure.data.json :as json]
            [next.jdbc :as jdbc]
            [clojure.test :refer [deftest is]])
  (:import [java.util UUID]))


(def dev-conn* (delay (doto (-> (sql-impl/jdbc-postgre-connect-str
                                 "localhost:5432" "dev-user"
                                 "dev-user" "unsafe-bad-password")
                                (jdbc/get-connection {:auto-commit false}))
                        (.setCatalog "dev-user"))))


(defn- uuid-table-name
  []
  (-> (UUID/randomUUID)
      (str)
      (#(.replace ^String % "-" "_"))
      (#(str "aa" %))))


(deftest stocks-dataset
  (let [table-name (uuid-table-name)
        stocks (-> (ds/->dataset "test/data/stocks.csv")
                   (vary-meta assoc
                              :name table-name
                              :primary-keys ["date" "symbol"]))]
    (try
      (sql/create-table! @dev-conn* stocks)
      (sql/insert-dataset! @dev-conn* stocks)
      (let [sql-stocks (sql/sql->dataset
                        @dev-conn* (format "Select * from %s"
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
        (is (dfn/equals (stocks "price") (sql-stocks "price"))))
      (finally
        (try
          (sql/drop-table! @dev-conn* stocks)
          (catch Throwable e nil))))))


(deftest small-missing
  (let [test-ds (ds/->dataset [{:a 1 :b 2}
                               {:b 3}
                               {:a 4}]
                              {:dataset-name (uuid-table-name)})]
    (try
      (sql/create-table! @dev-conn* test-ds)
      (sql/insert-dataset! @dev-conn* test-ds)
      (let [sql-ds (sql/sql->dataset
                    @dev-conn* (format "Select * from %s"
                                       (ds/dataset-name test-ds)))]
        (is (= (ds/row-count sql-ds)
               (ds/row-count test-ds)))
        (is (= (ds/missing test-ds)
               (ds/missing sql-ds)))
        (is (= (vec (test-ds :a))
               (vec (sql-ds "a"))))
        (is (= (vec (test-ds :b))
               (vec (sql-ds "b")))))
      (finally
        (try
          (sql/drop-table! @dev-conn* test-ds)
          (catch Throwable e nil))))))

(deftest ames-ds
  (let [test-ds (-> (ds/->dataset "test/data/ames-train.csv.gz"
                                  {:dataset-name (uuid-table-name)})
                    (ds/select-rows (range 20))
                    ;;Append an 'a_' to the start of every column name because
                    ;;these names start with numbers
                    (#(ds/select-columns
                       % (->> (ds/column-names %)
                              (map (fn [^String cname]
                                     [cname (str "a_" (.toLowerCase cname))]))
                              (into {})))))]
    (try
      (sql/create-table! @dev-conn* test-ds)
      (sql/insert-dataset! @dev-conn* test-ds)
      (let [sql-ds (sql/sql->dataset @dev-conn*
                                     (format "Select * from %s"
                                             (ds/dataset-name test-ds)))]
        (is (= (ds/row-count test-ds)
               (ds/row-count sql-ds)))
        (doseq [col (ds/columns test-ds)]
          (let [cname (ds-col/column-name col)
                sql-col (sql-ds cname)
                col-dtype (dtype/get-datatype col)
                col-dtype (if (= col-dtype :int16)
                            :int32
                            col-dtype)]
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
                    (format "Object equals for column %s" cname)))))))
      (finally
        (try
          (sql/drop-table! @dev-conn* test-ds)
          (catch Throwable e nil))))))


(deftest sql-uuid-test
  (let [test-ds (ds/->dataset [{:a 1 :b (UUID/randomUUID)}
                               {:b (UUID/randomUUID)}]
                              {:dataset-name (uuid-table-name)})]
    (try
      (sql/create-table! @dev-conn* test-ds)
      (sql/insert-dataset! @dev-conn* test-ds)
      (let [sql-ds (sql/sql->dataset
                    @dev-conn* (format "Select * from %s"
                                       (ds/dataset-name test-ds)))]
        (is (= (ds/row-count sql-ds)
               (ds/row-count test-ds)))
        (is (= (ds/missing test-ds)
               (ds/missing sql-ds)))
        (is (= (vec (test-ds :a))
               (vec (sql-ds "a"))))
        (is (= (vec (test-ds :b))
               (vec (sql-ds "b")))))
      (finally
        (try
          (sql/drop-table! @dev-conn* test-ds)
          (catch Throwable e nil))))))


(deftest zoned-date-time
  (let [test-ds (ds/->dataset [{:a 1 :b (dtype-dt/zoned-date-time)}
                               {:a 2 :b (dtype-dt/zoned-date-time)}]
                              {:dataset-name (uuid-table-name)})]

    (try
      (sql/create-table! @dev-conn* test-ds)
      (sql/insert-dataset! @dev-conn* test-ds)
      (let [sql-ds (sql/sql->dataset
                    @dev-conn* (format "Select * from %s"
                                       (ds/dataset-name test-ds)))]
        (is (= (ds/row-count sql-ds)
               (ds/row-count test-ds)))
        (is (= (ds/missing test-ds)
               (ds/missing sql-ds)))
        (is (= (vec (test-ds :a))
               (vec (sql-ds "a"))))
        (is (= (vec (map #(-> %
                            dtype-dt/zoned-date-time->instant
                            dtype-dt/instant->milliseconds-since-epoch)
                      (test-ds :b)))
                (vec (map #(-> %
                             dtype-dt/instant->milliseconds-since-epoch)
                       (sql-ds "b"))))))
      (finally
        (try
          (sql/drop-table! @dev-conn* test-ds)
          (catch Throwable e nil))))))


(deftest duration
  (let [test-ds (ds/->dataset [{:a 1 :b (dtype-dt/milliseconds->duration 400)}
                               {:a 2 :b (dtype-dt/milliseconds->duration 10000)}]
                              {:dataset-name (uuid-table-name)})]

    (try
      (sql/create-table! @dev-conn* test-ds)
      (sql/insert-dataset! @dev-conn* test-ds)
      (let [sql-ds (sql/sql->dataset
                    @dev-conn* (format "Select * from %s"
                                       (ds/dataset-name test-ds)))]
        (is (= (ds/row-count sql-ds)
               (ds/row-count test-ds)))
        (is (= (ds/missing test-ds)
               (ds/missing sql-ds)))
        (is (= (vec (test-ds :a))
               (vec (sql-ds "a"))))
        (is (= (vec (test-ds :b))
               (vec (sql-ds "b")))))
      (finally
        (try
          (sql/drop-table! @dev-conn* test-ds)
          (catch Throwable e nil))))))


(deftest jsonb
  (try
    (let [test-ds (-> (ds/->dataset [{:jsondata {:a 1 :b 2}}
                                     {:jsondata {:c 2 :d 3}}]
                                    {:dataset-name "jsonb_test"})
                      (ds/column-map :jsondata json/json-str :string identity)
                      (ds/update-column :jsondata #(with-meta %
                                                    {:sql-datatype "jsonb"})))]
      (sql-impl/execute-update! @dev-conn*
                                "create table jsonb_test (jsondata jsonb);")
      (sql/insert-dataset! @dev-conn* test-ds)
      (let [new-ds (sql/sql->dataset @dev-conn*
                                     "select * from jsonb_test"
                                     {:key-fn keyword})]
        (is (= (mapv json/read-str (test-ds :jsondata))
               (mapv json/read-str (new-ds :jsondata))))))
    (finally
      (try
        (sql/drop-table! @dev-conn* "jsonb_test")
        (catch Throwable e nil)))))


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
