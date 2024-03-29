from sqlite_utils import Database
from typing import List, Any, Dict, Union, Optional
import sqlite_utils
import datetime
import sqlglot
import json
import sqlite3
import psycopg
from psycopg.rows import dict_row
from pprint import pprint
from psycopg.sql import SQL, Identifier, Literal
import sys
import logging
import structlog
import argparse
import asyncio


_IGNORE_CHECKS=True
_IGNORE_TRIGGERS=True
_IGNORE_VIEWS=True
SQLITE_SYSTEM_TABLES = ["sqlite_sequence", "sqlite_stat1", "sqlite_user"]

logger = structlog.get_logger(__name__)


class SchemaError(Exception):
    """Raise for schema conditions that are invalid for pgsqlite"""
    pass


# We currently use both sqlite_utils and sqglot to extract and transpile database
# schemas. ParsedTable (ParsedColumn) wraps both representations of each table
# (column) object so that equivalent objects remain synced.
class ParsedTable(object):
    """Wraps a parsed sqlite_utils.db.Table and exposes transpiled identifiers."""
    
    def __init__(self, table: sqlite_utils.db.Table):
        self.src_table = table
        self.parsed_table = sqlglot.parse_one(table.schema, read="sqlite")
        # When we compose SQL w/ psycopg2.sql, values wrapped in Identifier are
        # automatically quoted in Postgres. Therefore, we need to inspect for
        # sqlite quotes and lower when quotes are not present to avoid mapping
        # identifiers like `FOO` -> `"FOO"` as opposed to the expected `foo`
        table_identifier = (self.parsed_table.find(sqlglot.exp.Table)
                                             .find(sqlglot.exp.Identifier))
        self._tsp_table_name = table_identifier.this
        if not table_identifier.quoted:
            self._tsp_table_name = self._tsp_table_name.lower()
        # Populate parsed columns                      
        parsed_cols = []
        # Unlike Postgres, SQLite allows columns without data types.
        # NOTE: see test_schema/typeless_columns.sql for an example
        # When sqlglot parses a table with typeless columns, if parses
        # the typless column as a bare Identifier rather than as a ColumnDef.
        # e.g.
        # Table
        #  - ColDef
        #  - Identifier
        #  - ColDef
        #  ...
        # Therefore, we need to iterate over the direct children of the Table
        # expression, and replace any bare Identifiers as ColumnDef w/ TEXT
        # data type. The rationale for TEXT datatype is that SQLite uses a
        # similar fallback when data does not match its column type. 
        for exp in self.parsed_table.this.expressions:
            # Use ColumDefs directly
            if isinstance(exp, sqlglot.exp.ColumnDef):
                parsed_cols.append(exp)
            # Use base Identifiers to construct ColumnDefs w/ TEXT data type
            elif isinstance(exp, sqlglot.exp.Identifier):
                col_def = sqlglot.exp.ColumnDef(
                    this=exp,
                    kind=sqlglot.exp.DataType(
                        this=sqlglot.exp.DataType.Type.TEXT
                    ),
                )
                parsed_cols.append(col_def)
            # We could raise on an else here, but can just ignore unexpected
            # expressions and try to finish processing
        if len(self.src_table.columns) != len(parsed_cols):
            raise SchemaError(f"sqlite_utils and sqlglot disagree on number of columns in table {self.source_name}")
        self._columns = {
            col.name: ParsedColumn(col, parsed_col)
            for col, parsed_col in zip(self.src_table.columns, parsed_cols)
        }
    
    @property
    def source_name(self):
        return self.src_table.name

    @property
    def transpiled_name(self):
        return self._tsp_table_name

    @property
    def columns(self):
        return self._columns.values()

    def get_transpiled_colname(self, source_colname: str) -> str:
        try:
            return self._columns[source_colname].transpiled_name
        except KeyError as e:
            raise ValueError("Requested transpiled name for unrecognized source column") from e


class ParsedColumn(object):
    """Wraps a parsed column and exposes source and transpiled identifiers.
    
    NOTE: During object construction, the parsed AST is mutated as-needed for
    the generated CREATE TABLE statements. 
    """
    def __init__(self, column: sqlite_utils.db.Column, parsed_column: sqlglot.expressions.ColumnDef):
        self.src_column = column
        # NOTE: must pop embedded foreign key constraints from col def AST to
        # avoid data-loading conflicts. The foreign keys are added back after
        # data loading using schema data from sqlite_utils.
        if (fk := parsed_column.find(sqlglot.exp.Reference)):
            fk.pop()
        self.parsed_column = parsed_column
        # When we compose SQL w/ psycopg2.sql, values wrapped in Identifier are
        # automatically quoted in Postgres. Therefore, we need to inspect for
        # sqlite quotes and lower when quotes are not present to avoid mapping
        # identifiers like `FOO` -> `"FOO"` as opposed to the expected `foo`
        column_identifier = self.parsed_column.find(sqlglot.expressions.Identifier)
        self._tsp_column_name = column_identifier.this
        if not column_identifier.quoted:
            self._tsp_column_name = self._tsp_column_name.lower()   
    
    @property
    def source_name(self):
        return self.src_column.name

    @property
    def transpiled_name(self):
        return self._tsp_column_name


class PGSqlite(object):
    # From https://stackoverflow.com/a/61478547
    async def gather_with_concurrency(self, max_coros: int, *coros: Any) -> Any:
        semaphore = asyncio.Semaphore(max_coros)
        async def sem_task(coro):
            async with semaphore:
                return await coro
        return await asyncio.gather(*(sem_task(coro) for coro in coros))

    def boolean_transformer(self, val: Any, nullable: bool) -> Union[bool, None]:
        if nullable and not val:
            return None
        if not nullable and not val:
            raise Exception("Value is None but column is not nullable")

        if val == 1 or val.lower() == "true":
            return "TRUE"
        return "FALSE"



    def __init__(self, sqlite_filename: str, pg_conninfo: str, show_sample_data: bool = False, max_import_concurrency: int = 10) -> None:
        self.sqlite_filename = sqlite_filename
        self.pg_conninfo = pg_conninfo
        self._tables = None
        self.tables_sql = []
        self.fks_sql = []
        self.indexes_sql = []
        self.checks_sql_by_table = {}
        self.summary = {}
        self.summary["tables"] = {}
        self.summary["tables"]["columns"] = {}
        self.summary["tables"]["pks"] = {}
        self.summary["tables"]["fks"] = {}
        self.summary["tables"]["checks"] = {}
        self.summary["tables"]["data"] = {}
        self.summary["tables"]["indexes"] = {}
        self.summary["views"] = {}
        self.summary["triggers"] = {}
        self.transformers = {}
        self.transformers['BOOLEAN'] = self.boolean_transformer
        self.show_sample_data = show_sample_data
        self.max_import_concurrency = max_import_concurrency
        db = Database(self.sqlite_filename)
        self._tables = {t.name: ParsedTable(t) for t in db.tables}

    @property
    def tables(self):
        return self._tables.values()

    def get_transpiled_tablename(self, source_tablename: str) -> str:
        try:
            return self._tables[source_tablename].transpiled_name
        except KeyError as e:
            raise ValueError("Requested transpiled name for unrecognized source table") from e

    def get_transpiled_colname(self, source_tablename: str, source_colname: str) -> str:
        try:
            return self._tables[source_tablename].get_transpiled_colname(source_colname)
        except KeyError as e:
            raise ValueError("Requested transpiled name for unrecognized source table") from e

    def get_table_sql(self, table: ParsedTable) -> SQL:

        # This is a little interesting. We can't use sqlglot.transpile directly, since we need to
        # avoid creating the foreign keys until we've loaded the data, and we don't support views/etc. 
        # So we assemble the rest of the table DDL by hand.
        create_sql = SQL("CREATE TABLE {table_name} (").format(table_name=Identifier(table.transpiled_name))
        columns_sql = []
        cols = {}
        already_created_pks = [] 
        for col in table.columns:
            # Fix for issues in sqlglot
            # NOTE: embedded foreign key references are already stripped from parsed_column 
            col_sql_str = col.parsed_column.sql(dialect="postgres")
            if "SERIAL" in col_sql_str:
                col_sql_str = col_sql_str.replace("INT", "")

            if "INT NOT NULL" in col_sql_str and col.source_name in table.src_table.pks:
                col_sql_str = col_sql_str.replace("INT", "SERIAL")

            if "PRIMARY KEY SERIAL" in col_sql_str:
                col_sql_str = col_sql_str.replace("PRIMARY KEY SERIAL", "SERIAL PRIMARY KEY")
            cols[col.source_name] = SQL(col_sql_str)
            if "PRIMARY KEY" in col_sql_str:
                # don't re-add this constraint later
                already_created_pks.append(col.source_name)

        # columns are sorted by column id, so they are created in the "correct" order for any later INSERTS that use the order from, eg, sqlite3.iterdump()
        for column in table.columns:
            columns_sql.append(cols[column.source_name])
        self.summary["tables"]["columns"][table.source_name] = {
            "status": "PREPARED",
            "count": len(table.columns),
        }
        all_column_sql = SQL(",\n").join(columns_sql)

        # sqlite appears to generate PK names by splitting on the CamelCasing for the first word, concatting, and prefixing with PK_
        # So let's do something similar
        pks_to_add = set(table.src_table.pks) - set(already_created_pks)
        if pks_to_add and not table.src_table.use_rowid:
            # Need to map pk columns to transpiled identifiers
            transpiled_pks_to_add = [table.get_transpiled_colname(pk) for pk in pks_to_add]
            all_column_sql = all_column_sql + SQL(",\n")
            pk_name = f"PK_{table.source_name}_" + ''.join(pks_to_add)
            pk_sql = SQL("    CONSTRAINT {pk_name} PRIMARY KEY ({pks})").format(
                    table_name=Identifier(table.transpiled_name),
                    pk_name=Identifier(pk_name), pks=SQL(", ").join(
                        [Identifier(t) for t in transpiled_pks_to_add]
                    ),
            )
            all_column_sql = SQL("    ").join([all_column_sql, pk_sql])
        self.summary["tables"]["pks"][table.source_name] = {
            "status": "PREPARED",
            "count": len(table.src_table.pks),
        }

        self.summary["tables"]["checks"][table.source_name] = {}
        if self.checks_sql_by_table[table.source_name] and not _IGNORE_CHECKS:
            all_column_sql = all_column_sql + SQL(",\n")
            check_sql = SQL(",\n").join(self.checks_sql_by_table[table.source_name])
            all_column_sql = SQL("").join([all_column_sql, check_sql])
            self.summary["tables"]["checks"][table.source_name]["status"] = "PREPARED"
        else:
            self.summary["tables"]["checks"][table.source_name]["status"] = "IGNORED"
        self.summary["tables"]["checks"][table.source_name]["count"] = len(self.checks_sql_by_table[table.source_name])

        create_sql = SQL("\n").join([create_sql, all_column_sql, SQL(");")])

        return create_sql


    def get_fk_sql(self, table: ParsedTable) -> SQL:
        sql = []
        # create the foreign keys after the tables to avoid having to figure out the dep graph
        for fk in table.src_table.foreign_keys:
            fk_name = f"FK_{fk.other_table}_{fk.other_column}"
            fk_sql = SQL("ALTER TABLE {table_name} ADD CONSTRAINT {key_name}  FOREIGN KEY ({column}) REFERENCES {other_table} ({other_column})").format(
                table_name=Identifier(table.transpiled_name),
                column=Identifier(table.get_transpiled_colname(fk.column)),
                key_name=Identifier(fk_name),
                other_table=Identifier(self.get_transpiled_tablename(fk.other_table)),
                other_column=Identifier(self.get_transpiled_colname(fk.other_table, fk.other_column)),
            )
            sql.append(fk_sql)
        self.summary["tables"]["fks"][table.source_name] = {
            "status": "PREPARED",
            "count": len(table.src_table.foreign_keys),
        }
        return sql

    def get_index_sql(self, table: ParsedTable) -> SQL:
        sql = []
        for index in table.src_table.xindexes:
            col_sql = []
            for col in index.columns:
                if not col.name:
                    continue
                order = "ASC"
                if col.desc:
                    order="DESC"
                col_sql.append(SQL("{name} {sort_order}").format(
                    name=Identifier(table.get_transpiled_colname(col.name)),
                    sort_order=SQL(order)),
                )

            index_sql = SQL("CREATE INDEX {index_name} ON {table_name} ({columns})").format(
                index_name = Identifier(index.name),
                table_name=Identifier(table.transpiled_name),
                columns=SQL(",").join(col_sql)
            )
            sql.append(index_sql)
        self.summary["tables"]["indexes"][table.source_name] = {
            "status": "PREPARED",
            "count": len(table.src_table.xindexes),
        }
        return sql

    def _drop_tables(self):
        with psycopg.connect(conninfo=self.pg_conninfo) as conn:
            with conn.cursor() as cur:
                for table in self.tables:
                    cur.execute(
                        SQL("DROP TABLE IF EXISTS {table_name} CASCADE;").format(table_name=Identifier(table.transpiled_name))
                    )

    def get_all_tables_in_postgres(self) -> Optional[List[Any]]:
        tables_in_postgres = []
        with psycopg.connect(conninfo=self.pg_conninfo,  row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(SQL("""
                    SELECT
                        table_name, column_name, ordinal_position, is_nullable, data_type
                    FROM
                        information_schema.columns
                    WHERE
                        table_name
                    IN (
                        SELECT
                            table_name
                        FROM
                            information_schema.tables
                        WHERE
                            table_type='BASE TABLE'
                        AND
                            table_schema
                        NOT IN ('pg_catalog', 'information_schema')
                        )
                    ORDER BY
                        table_name, column_name, ordinal_position; """))
                tables_in_postgres = cur.fetchall()
        return tables_in_postgres

    def check_for_matching_tables(self) -> bool:
        # Note for later PR: implement me
        db = Database(self.sqlite_filename)
        tables_in_postgres = self.get_all_tables_in_postgres()
        return False

    def load_schema(self, drop_existing_postgres_tables: bool = False) -> None:
        db = Database(self.sqlite_filename)
        if drop_existing_postgres_tables:
            self._drop_tables()

        self.checks_sql_by_table = self.get_check_constraints()
        for table in self.tables:
            if table.source_name in SQLITE_SYSTEM_TABLES:
                logger.debug(f"sqlite system table found: {table.source_name}")
                continue
            self.tables_sql.append(self.get_table_sql(table))
            self.fks_sql.extend(self.get_fk_sql(table))
            self.indexes_sql.extend(self.get_index_sql(table))

        if not _IGNORE_VIEWS:
            logger.debug("Ignoring views", db_filename=self.sqlite_filename)
            for view in db.views:
                # there's a bug here in the sqlite_utils library where this fails
                logger.debug(f"DB view: {view}", view=view)
                self.summary["views"][view.name] = {
                    "status": "IGNORED",
                }
        if not _IGNORE_TRIGGERS:
            logger.debug("Ignoring views")
            for trigger in db.triggers:
                logger.debug(f"DB trigger: {trigger}", trigger=trigger)
                self.summary["triggers"][trigger.name] = {
                    "status": "IGNORED",
                }

    async def create_index(self, index_sql: str) -> None:
        async with await psycopg.AsyncConnection.connect(conninfo=self.pg_conninfo) as conn:
            async with conn.cursor() as pg_cur:
                index_str = index_sql.as_string(conn)
                logger.debug(f"Creating index with: {index_str}")
                await pg_cur.execute(index_sql)
                logger.debug(f"Finished creating index with: {index_str}")

    async def write_table_data(self, table: ParsedTable) -> None:
        sl_conn = sqlite3.connect(self.sqlite_filename)
        sl_cur = sl_conn.cursor()
        logger.info(f"Loading data into {table}", table=table.transpiled_name)

        pk_col_name = next((col.name for col in table.src_table.columns if col.is_pk and col.notnull and col.type == "INTEGER"), None)
        last_index = None
        if pk_col_name:
            last_index, = sl_cur.execute(f'SELECT MAX("{pk_col_name}") FROM "{table.source_name}"').fetchone()

        # Given the table name came from the SQLITE database, and we're using it
        # to read from the sqlite database, we are okay with the literal substitution here
        sl_cur.execute(f'SELECT * FROM "{table.source_name}"')
        nullable_column_indexes = []
        for idx, c in enumerate(table.columns):
            if not c.src_column.notnull:
                nullable_column_indexes.append(idx)

        # For any non-null column, allow convert from empty string to None
        async with await psycopg.AsyncConnection.connect(conninfo=self.pg_conninfo) as conn:
            async with conn.cursor() as pg_cur:
                async with pg_cur.copy(f'COPY "{table.transpiled_name}" FROM STDIN') as copy:
                    rows_copied = 0
                    for row in sl_cur:
                        row = list(row)
                        for idx, c in enumerate(table.columns):
                            if c.src_column.type in self.transformers:
                                row[idx] = self.transformers[c.src_column.type](row[idx], not c.src_column.notnull)
                            if not c.src_column.notnull:
                                # for numeric types, we need to be we don't evaluate False on a 0
                                if row[idx] != 0 and not row[idx]:
                                    row[idx] = None

                        await copy.write_row(row)
                        rows_copied += 1
                        if rows_copied % 1000 == 0:
                            self.summary["tables"]["data"][table.source_name]["status"] = f"LOADED {rows_copied}"

                    self.summary["tables"]["data"][table.source_name]["status"] = f"LOADED {rows_copied}"
                logger.info(f"Finished loading {rows_copied} rows of data into {table.transpiled_name}")
                if pk_col_name and last_index:
                    logger.info(f"Updating sequence data for {table.transpiled_name}.{pk_col_name}")
                    await pg_cur.execute(f'ALTER SEQUENCE "{table.transpiled_name}_{pk_col_name}_seq" RESTART WITH {last_index + 1}')

        sl_conn.close()

    def load_data_to_postgres(self):
        db = Database(self.sqlite_filename)
        sl_conn = sqlite3.connect(self.sqlite_filename)
        sl_cur = sl_conn.cursor()
        for table in db.tables:
            # Given the table name came from the SQLITE database, and we're using it
            # to read from the sqlite database, we are okay with the literal substitution here
            sl_cur.execute(f'SELECT count(*) FROM "{table.name}"')
            self.summary["tables"]["data"][table.name] = {
                "row_count": sl_cur.fetchone()[0],
                "status": "PREPARED",
            }
        sl_conn.close()

        async def load_all_data():
            await self.gather_with_concurrency(
                self.max_import_concurrency,
                *[
                    self.write_table_data(table)
                    for table in self.tables
                    if table.source_name not in SQLITE_SYSTEM_TABLES
                ],
            )
        load_results = asyncio.run(load_all_data())

        if self.show_sample_data:
            for table in self.tables:
                with psycopg.connect(conninfo=self.pg_conninfo) as conn:
                    with conn.cursor() as cur:
                        cur.execute(f'SELECT * from "{table.transpiled_name}" LIMIT 10')
                        logger.debug(f"Data in {table.transpiled_name}")
                        logger.debug(cur.fetchall())

    def get_summary(self) -> Dict[str, Any]:
        return self.summary

    def get_check_constraints(self):
        sl_conn = sqlite3.connect(self.sqlite_filename)
        sl_cur = sl_conn.cursor()
        sl_cur.execute('select name, sql from sqlite_master where type="table"')
        checks = {}
        for row in sl_cur:
            checks[row[0]] = []
            transpile = ""
            for line in row[1].split('\n'):
                if "CHECK" in line:
                    start = line.index("(")
                    end = line.rindex(")")
                    sql_expr= line[start + 1:end]
                    clean_check_str = "    " + line.strip().rstrip(',')
                    checks[row[0]].append(SQL(clean_check_str))
                else:
                    transpile = transpile + "\n" + line
            transpile = transpile.replace('[', '"').replace(']', '"') # Handle SQLite table names that are [foo]
            transpile = transpile.replace('`', '"') # Handle SQLLite table names that are `foo`
        sl_conn.close()

        return checks

    def populate_postgres(self)-> None:
        with psycopg.connect(conninfo=self.pg_conninfo) as conn:
            with conn.cursor() as cur:
                for create_sql in self.tables_sql:
                    logger.debug("Running SQL:")
                    logger.debug(create_sql.as_string(conn))
                    cur.execute(create_sql)
            for column in self.summary["tables"]["columns"].values():
                column["status"] = "CREATED"
            for pk in self.summary["tables"]["pks"].values():
                pk["status"] = "CREATED"

        self.load_data_to_postgres()

        async def create_all_indexes():
            await self.gather_with_concurrency(self.max_import_concurrency, *[self.create_index(index) for index in self.indexes_sql])
            for table in self.summary["tables"]["indexes"]:
                self.summary["tables"]["indexes"][table]["status"] = "CREATED"

        asyncio.run(create_all_indexes())

        with psycopg.connect(conninfo=self.pg_conninfo) as conn:
            with conn.cursor() as cur:
                for fk in self.fks_sql:
                    logger.debug("Running SQL:")
                    logger.debug(fk.as_string(conn))
                    cur.execute(fk)
                for table in self.summary["tables"]["fks"]:
                    self.summary["tables"]["fks"][table]["status"] = "CREATED"
                # todo: add checks, views, triggers.


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-f",
        "--sqlite_filename",
        type=str,
        help="sqlite database to import",
        required=True
    )
    parser.add_argument(
        "-p",
        "--postgres_connect_url",
        type=str,
        help="Postgres URL for the database to import into",
        required=True
    )
    parser.add_argument(
        "--max_import_concurrency",
        type=int,
        help="Number of concurrent data import coroutines to run",
        default=10,
    )
    parser.add_argument(
        "-d",
        "--debug",
        type=bool,
        default=False,
        help="Set log level to DEBUG",
    )
    parser.add_argument(
        "--show_sample_data",
        type=bool,
        default=False,
        help="After import, show up to 10 rows of the imported data in each table.",
    )
    parser.add_argument(
        "--drop_tables",
        type=bool,
        default=False,
        help="Prior to import, drop tables in the target database that have the same name as tables in the source database",
    )
    parser.add_argument(
        "--drop_everything",
        type=bool,
        default=False,
        help="Prior to import, drop everything (tables, views, triggers, etc, etc) in the target database before the import",
    )
    parser.add_argument(
        "--drop_tables_after_import",
        type=bool,
        default=False,
        help="Drop all tables in the target database after import; useful for testing",
    )
    args = parser.parse_args()

    if args.debug:
        structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG))
    else:
        structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.INFO))

    sqlite_filename = args.sqlite_filename
    pg_conninfo = args.postgres_connect_url

    loader = PGSqlite(sqlite_filename, pg_conninfo, show_sample_data=args.show_sample_data, max_import_concurrency=args.max_import_concurrency)
    loader.load_schema(drop_existing_postgres_tables=args.drop_tables)
    loader.populate_postgres()
    logger.debug(json.dumps(loader.get_summary(), indent=2))

    if args.drop_tables_after_import:
        loader._drop_tables()