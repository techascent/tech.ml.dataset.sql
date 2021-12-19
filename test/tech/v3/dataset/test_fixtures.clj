(ns tech.v3.dataset.test-fixtures
  (:require  [tech.v3.dataset.sql.impl :as sql-impl]
             [next.jdbc :as jdbc]
             [clojure.test :as t]))


(def postgres-url {:url (sql-impl/jdbc-postgre-connect-str
                           "localhost:5432" "dev-user"
                           "dev-user" "unsafe-bad-password")
                   :catalog "dev-user"})


(def sql-server-url {:url (sql-impl/jdbc-sql-server-connect-str
                            "localhost:1433" ""
                            "sa" "unsafe-bad-password-0")
                     :catalog "master"})


(def test-db-specs [postgres-url sql-server-url])


(defn make-dev-conn-fixture [url catalog]
  (fn [] (def dev-conn* (delay (doto (-> postgres-url
                                     (jdbc/get-connection {:auto-commit false}))
                                (.setCatalog catalog))))))


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
