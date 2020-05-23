# tech.ml.dataset.sql

Minimal SQL bindings for
[`tech.ml.dataset`](https://github.com/techascent/tech.ml.dataset).


## Usage

This library provides versions of jdbc.next and honeysql.  `tech.ml.dataset`
is expected to be transitively provided.

Recommended driver: `[postgresql/postgresql "42.2.12"]`

Provided in namespace `tech.ml.dataset.sql`:

 * `result-set->dataset` - given a result set, read all the data into a dataset.
 * `sql->dataset` - Given a string sql statement, return a dataset.
 * `sanitize-dataset-names-for-sql` - Transform the dataset name and the column names
   to string and replace "-" with "_".
 * `table-exists?` - Return true of the table of this name exists.
 * `drop-table!` - Drop the table of this name.
 * `drop-table-when-exists!` - Drop the table if it exists.
 * `create-table!` - Using a dataset for the table name for the column names and
    datatypes, create a new table.
 * `ensure-table!` - Ensure that a given table exists.
 * `insert-dataset!` - Insert/upsert a dataset into a table.  Upsert is postgresql-only.


 For efficiency when inserting/upserting a dataset the connection should be created with
 {:auto-commit false}.


 Want to see more functions above?  We accept PRs :-).  The sql/impl namespace
 provides many utility functions (like creating connection strings for postgresql
 servers) that may be helpful along with required helpers if you want to implement
 bindings to a different sql update/insert pathway.


 Included in this repo is a nice, one-stop
 [docker pathway](scripts/start-local-postgres) for development purposes that will
 start the a server with the expected settings used by the unit testing system.


## Example

```clojure
user> (require '[tech.ml.dataset :as ds])
nil
user> (def ds (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv"))
#'user/ds
user> (ds/head ds)
https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv [5 3]:

| symbol |       date | price |
|--------|------------|-------|
|   MSFT | 2000-01-01 | 39.81 |
|   MSFT | 2000-02-01 | 36.35 |
|   MSFT | 2000-03-01 | 43.22 |
|   MSFT | 2000-04-01 | 28.37 |
|   MSFT | 2000-05-01 | 25.45 |
user> (require '[tech.ml.dataset.sql :as ds-sql])
nil
user> (require '[tech.ml.dataset.sql.impl :as ds-sql-impl])
nil
user> ;;Connections should be created with auto-commit false so that inserts are batched.
user> (require '[next.jdbc :as jdbc])
nil
user> (def dev-conn (doto (-> (ds-sql-impl/jdbc-postgre-connect-str
                               "localhost:5432" "dev-user"
                               "dev-user" "unsafe-bad-password")
                              (jdbc/get-connection {:auto-commit false}))
                      (.setCatalog "dev-user")))
#'user/dev-conn
user> dev-conn
#object[org.postgresql.jdbc.PgConnection 0x3256d7ea "org.postgresql.jdbc.PgConnection@3256d7ea"]
user> ;;set the table name and the primary keys
user> (def ds (with-meta ds
                (assoc (meta ds)
                       :name "stocks"
                       :primary-key ["symbol" "date"])))
#'user/ds
user> ;;see the sql created for this table
user> (println (ds-sql-impl/create-sql ds))
CREATE TABLE stocks (
 symbol varchar,
 date date,
 price float,
 PRIMARY KEY (symbol, date)
);
nil
user> (ds-sql/create-table! dev-conn ds)
nil
user> (ds-sql/insert-dataset! dev-conn ds)
nil
user> (def sql-ds (ds-sql/sql->dataset
                   dev-conn "SELECT * FROM stocks"))
#'user/sql-ds
user> (ds/head sql-ds)
_unnamed [5 3]:

| symbol |                 date | price |
|--------|----------------------|-------|
|   MSFT | 2000-01-01T07:00:00Z | 39.81 |
|   MSFT | 2000-02-01T07:00:00Z | 36.35 |
|   MSFT | 2000-03-01T07:00:00Z | 43.22 |
|   MSFT | 2000-04-01T07:00:00Z | 28.37 |
|   MSFT | 2000-05-01T06:00:00Z | 25.45 |
user> (ds/head ds)
stocks [5 3]:

| symbol |       date | price |
|--------|------------|-------|
|   MSFT | 2000-01-01 | 39.81 |
|   MSFT | 2000-02-01 | 36.35 |
|   MSFT | 2000-03-01 | 43.22 |
|   MSFT | 2000-04-01 | 28.37 |
|   MSFT | 2000-05-01 | 25.45 |
```

Note that local-dates are converted to instants in UTC.  The same is true for all
date/time types; all are just converted to java.sql.Date objects.  Numeric datatypes,
date/time types, strings and UUID's are supported datatypes.


## License

Copyright © 2020 TechAscent, LLC

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.
