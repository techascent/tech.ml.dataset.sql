(ns tech.v3.dataset.sql-test-utils
  (:require  [tech.v3.dataset.sql :as sql]
             [next.jdbc :as jdbc]
             [clojure.test :refer [deftest] :as t]))


(def ^:private connection-data
  {:postgre-sql {:url (sql/postgre-sql-connect-str
                      "localhost:5432" "dev-user"
                      "dev-user" "unsafe-bad-password")
                :catalog "dev-user"}
   :sql-server {:url (sql/sql-server-connect-str
                      "localhost:1433" ""
                      "sa" "unsafe-bad-password-0")
                :catalog "master"}
   ;; Their JDBC driver is very nonstandard.
   ;; :duckdb {:url "jdbc:duckdb:"}
   })


(defn- do-connect
  [{:keys [url catalog]}]
  (let [conn (jdbc/get-connection url {:auto-commit false})]
    (when catalog
      (.setCatalog conn catalog))
    conn))


(def ^:private connections* (atom {}))


(defn connect
  [db-kwd]
  (->
   (swap! connections* update db-kwd #(or % (do-connect (get connection-data db-kwd))))
   (get db-kwd)))


(def ^{:private true
       :dynamic true} *active-connection*)


(defn dev-conn
  []
  (connect *active-connection*))


(defmacro with-connection
  [conn-kwd & code]
  `(with-bindings {#'*active-connection* ~conn-kwd}
     ~@code))


(defmacro def-db-test
  "Define a test for each registered database.  This means the database name is in the
  printed testname so if a test fails the db under which it failed is clear."
  [testname & code]
  `(do
     ~@(->> (keys connection-data)
            (map (fn [conn-kwd]
                   (let [testname (symbol (str (name testname) "-" (name conn-kwd)))]
                     `(deftest ~testname
                        (with-connection ~conn-kwd
                          ~@code))))))))
