(ns tech.v3.dataset.test-fixtures
  (:require  [tech.v3.dataset.sql.impl :as sql-impl]
             [next.jdbc :as jdbc]
             [clojure.test :as t]))

(def connection-data
  {:postgres {:url (sql-impl/jdbc-postgre-connect-str
                    "localhost:5432" "dev-user"
                    "dev-user" "unsafe-bad-password")
              :catalog "dev-user"}
   :sql-server {:url (sql-impl/jdbc-sql-server-connect-str
                      "localhost:1433" ""
                      "sa" "unsafe-bad-password-0")
                :catalog "master"}
   :duckdb {:url "jdbc:duckdb:"}})


(defn connect
  [{:keys [url catalog]}]
  (let [conn (jdbc/get-connection url {:auto-commit false})]
    (when catalog
      (.setCatalog conn catalog))
    conn))


(def db-spec (atom nil))

(defn dev-conn []
  @@db-spec)


(defn with-test-db [t]
  (doseq [db test-db-specs]
    (reset! db-spec
      (delay (doto (-> (:url db)
                     (jdbc/get-connection {:auto-commit false}))
               (.setCatalog (:catalog db)))))
    (t)))


(comment
  @db-spec
  (conn)
  @(dev-conn)
  )
