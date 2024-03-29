pgsqlite
=======
Load SQLite3 databases into PostgreSQL.

Install with pipx

```sh
pipx install pgsqlite
```

Usage:
```
usage: pgsqlite.py [-h] -f SQLITE_FILENAME -p POSTGRES_CONNECT_URL [-d DEBUG] [--drop_tables DROP_TABLES] [--drop_everything DROP_EVERYTHING] [--drop_tables_after_import DROP_TABLES_AFTER_IMPORT]

optional arguments:
  -h, --help            show this help message and exit
  -f SQLITE_FILENAME, --sqlite_filename SQLITE_FILENAME
                        sqlite database to import
  -p POSTGRES_CONNECT_URL, --postgres_connect_url POSTGRES_CONNECT_URL
                        Postgres URL for the database to import into
  -d DEBUG, --debug DEBUG
                        Set log level to DEBUG
  --drop_tables DROP_TABLES
                        Prior to import, drop tables in the target database that have the same name as tables in the source database
  --drop_everything DROP_EVERYTHING
                        Prior to import, drop everything (tables, views, triggers, etc, etc) in the target database before the import
  --drop_tables_after_import DROP_TABLES_AFTER_IMPORT
                        Drop all tables in the target database after import; useful for testing
```


Examples:

Import into the bit.io database `adam/AMEND`, with DEBUG-level logging.
```
python pgsqlite.py  -f ../example_dbs/Chinook_Sqlite.sqlite -p postgresql://adam:<password>@db.bit.io/adam/AMEND --debug true
```

Import into the bit.io database `adam/AMEND`, dropping all tables in the target database that match tables in the source database: 
```
python pgsqlite.py  -f ../example_dbs/Chinook_Sqlite.sqlite -p postgresql://adam:<password>@db.bit.io/adam/AMEND --drop_tables true
```

Most of the drop options are used for testing - be aware they are destructive operations!

Testing
=======
There's a set of open-source databases in the `example_dbs/` directory, and
`./import_examples.sh` script that will test importing of all those databases. 
You'll need to set `POSTGRES_CREDS_STRING` to your connect string before hand, 
and also be aware this script will drop everything in the target database, so be careful!

How This Works
==============

For more details, read: https://innerjoin.bit.io/introducing-pgsqlite-a-pure-python-module-to-import-sqlite-databases-into-postgres-bf3940cfa19f


SQLite is far more forgiving a database then Postgres. Look at this `CREATE TABLE`:

```
CREATE TABLE Customer_Ownership(
  customer_id INTEGER NOT NULL,
  vin INTEGER NOT NULL,
  purchase_date DATE NOT NULL,
  purchase_price INTEGER NOT NULL,
  warantee_expire_date DATE,
  dealer_id INTEGER NOT NULL,
  FOREIGN KEY (customer_id) REFERENCES Customers(customer_id),
  FOREIGN KEY (vin) REFERENCES Car_Vins(vin),
  FOREIGN KEY (dealer_id) REFERENCES Dealers(dealer_id)
  PRIMARY KEY (customer_id, vin)
);
```

This is totally valid in SQLite and is missing a comma on the second to last line. In fact, this is what
you'd get back from `.schema` in the sqlite command line tool. 

For pgsqlite, this means we cannot use the excellent `sqlglot` module to transpile the schema creation SQL 
as the module is too strict for some sqlite databases. 
We need the (also excellent) `sqlite-utils` module. `sqlite-utils` gives us python objects that represent
the database entities, which lets us then create Postgres-valid SQL to create these entities.


We use psycopg (version 3) to gain access to the very fast `COPY` protocol. We filter that incoming data 
to make sure we have nulls set correctly, and to do any transforms on the literal values that are required 
(like the BOOLEAN example in Known Issues, below). 


Known Issues
============
Most of the issues are around constraints that involve SQL that requires literals. For example, a `BOOLEAN` column may have a `CHECK` constraint
like `IN (1, 0)` which is valid in SQLite but not in Postgres (in SQLite the integers `1` and `0` are true/false, but not in Postgres). To fix
this we'd need to parse the SQL, identify the literals and which columns they map to, then "fix" the literal's type. This also impacts views & triggers.


TODOS
=====
* Unit tests
* Append mode
* Async loading of data
  * With async, a status property that tells us, eg "x of y rows loaded in table z"


