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
 
 
 Included in this repo is a nice, one-stop [docker pathway](scripts/start-local-postgres) 
 for development purposes that will start the a server with the expected settings used  by 
 the unit testing system.


## License

Copyright © 2020 TechAscent, LLC

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.
