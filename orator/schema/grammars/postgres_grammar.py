# -*- coding: utf-8 -*-

from .grammar import SchemaGrammar
from ..blueprint import Blueprint
from ...query.expression import QueryExpression
from ...support.fluent import Fluent


class PostgresSchemaGrammar(SchemaGrammar):

    _modifiers = ["increment", "nullable", "default"]

    _serials = [
        "big_integer",
        "integer",
        "medium_integer",
        "small_integer",
        "tiny_integer",
    ]

    marker = "%s"

    def compile_upsert(self, query, values, conflict_keys, conflict_columns):
        """
        Compile an upsert SQL statement

        :param query: A QueryBuilder instance
        :type query: QueryBuilder

        :param values: The values to insert
        :type values: dict or list

        :param conflict_keys: The list of keys

        :param conflict_columns: The columns to update on conflict
        :type  conflict_columns: list

        :return: The compiled statement
        :rtype: str
        """
        # Essentially we will force every insert to be treated as a batch insert which
        # simply makes creating the SQL easier for us since we can utilize the same
        # basic routine regardless of an amount of records given to us to insert.
        table = self.wrap_table(query.from__)

        if not isinstance(values, list):
            values = [values]

        columns = self.columnize(values[0].keys())

        # We need to build a list of parameter place-holders of values that are bound
        # to the query. Each insert should have the exact same amount of parameter
        # bindings so we can just go off the first list of values in this array.
        parameters = self.parameterize(values[0].values())

        value = ["(%s)" % parameters] * len(values)

        parameters = ", ".join(value)

        conflict_key_join = ", ".join(conflict_keys)
        conflict_update_statements = [
            "%s = EXCLUDED.%s" % (col, col) for col in conflict_columns
        ]
        conflict_update_join = ", ".join(conflict_update_statements)

        return "INSERT INTO %s (%s) VALUES %s ON CONFLICT (%s) DO UPDATE SET %s" % (
            table,
            columns,
            parameters,
            conflict_key_join,
            conflict_update_join,
        )

    def compile_rename_column(self, blueprint, command, connection):
        """
        Compile a rename column command.

        :param blueprint: The blueprint
        :type blueprint: Blueprint

        :param command: The command
        :type command: Fluent

        :param connection: The connection
        :type connection: orator.connections.Connection

        :rtype: list
        """
        table = self.get_table_prefix() + blueprint.get_table()

        column = self.wrap(command.from_)

        return "ALTER TABLE %s RENAME COLUMN %s TO %s" % (
            table,
            column,
            self.wrap(command.to),
        )

    def compile_table_exists(self):
        """
        Compile the query to determine if a table exists

        :rtype: str
        """
        return (
            "SELECT * "
            "FROM information_schema.tables "
            "WHERE table_name = %(marker)s" % {"marker": self.get_marker()}
        )

    def compile_column_exists(self, table):
        """
        Compile the query to determine the list of columns.
        """
        return (
            "SELECT column_name "
            "FROM information_schema.columns "
            "WHERE table_name = '%s'" % table
        )

    def compile_create(self, blueprint, command, _):
        """
        Compile a create table command.
        """
        columns = ", ".join(self._get_columns(blueprint))

        return "CREATE TABLE %s (%s)" % (self.wrap_table(blueprint), columns)

    def compile_add(self, blueprint, command, _):
        table = self.wrap_table(blueprint)

        columns = self.prefix_list("ADD COLUMN", self._get_columns(blueprint))

        return "ALTER TABLE %s %s" % (table, ", ".join(columns))

    def compile_primary(self, blueprint, command, _):
        columns = self.columnize(command.columns)

        return "ALTER TABLE %s ADD PRIMARY KEY (%s)" % (
            self.wrap_table(blueprint),
            columns,
        )

    def compile_unique(self, blueprint, command, _):
        columns = self.columnize(command.columns)

        table = self.wrap_table(blueprint)

        return "ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s)" % (
            table,
            command.index,
            columns,
        )

    def compile_index(self, blueprint, command, _):
        columns = self.columnize(command.columns)

        table = self.wrap_table(blueprint)

        return "CREATE INDEX %s ON %s (%s)" % (command.index, table, columns)

    def compile_drop(self, blueprint, command, _):
        return "DROP TABLE %s" % self.wrap_table(blueprint)

    def compile_drop_if_exists(self, blueprint, command, _):
        return "DROP TABLE IF EXISTS %s" % self.wrap_table(blueprint)

    def compile_drop_column(self, blueprint, command, connection):
        columns = self.prefix_list("DROP COLUMN", self.wrap_list(command.columns))

        table = self.wrap_table(blueprint)

        return "ALTER TABLE %s %s" % (table, ", ".join(columns))

    def compile_drop_primary(self, blueprint, command, _):
        table = blueprint.get_table()

        return "ALTER TABLE %s DROP CONSTRAINT %s_pkey" % (
            self.wrap_table(blueprint),
            table,
        )

    def compile_drop_unique(self, blueprint, command, _):
        table = self.wrap_table(blueprint)

        return "ALTER TABLE %s DROP CONSTRAINT %s" % (table, command.index)

    def compile_drop_index(self, blueprint, command, _):
        return "DROP INDEX %s" % command.index

    def compile_drop_foreign(self, blueprint, command, _):
        table = self.wrap_table(blueprint)

        return "ALTER TABLE %s DROP CONSTRAINT %s" % (table, command.index)

    def compile_rename(self, blueprint, command, _):
        from_ = self.wrap_table(blueprint)

        return "ALTER TABLE %s RENAME TO %s" % (from_, self.wrap_table(command.to))

    def _type_char(self, column):
        return "CHAR(%s)" % column.length

    def _type_string(self, column):
        return "VARCHAR(%s)" % column.length

    def _type_text(self, column):
        return "TEXT"

    def _type_medium_text(self, column):
        return "TEXT"

    def _type_long_text(self, column):
        return "TEXT"

    def _type_integer(self, column):
        return "SERIAL" if column.auto_increment else "INTEGER"

    def _type_big_integer(self, column):
        return "BIGSERIAL" if column.auto_increment else "BIGINT"

    def _type_medium_integer(self, column):
        return "SERIAL" if column.auto_increment else "INTEGER"

    def _type_tiny_integer(self, column):
        return "SMALLSERIAL" if column.auto_increment else "SMALLINT"

    def _type_small_integer(self, column):
        return "SMALLSERIAL" if column.auto_increment else "SMALLINT"

    def _type_float(self, column):
        return self._type_double(column)

    def _type_double(self, column):
        return "DOUBLE PRECISION"

    def _type_decimal(self, column):
        return "DECIMAL(%s, %s)" % (column.total, column.places)

    def _type_boolean(self, column):
        return "BOOLEAN"

    def _type_enum(self, column):
        allowed = list(map(lambda a: "'%s'" % a, column.allowed))

        return 'VARCHAR(255) CHECK ("%s" IN (%s))' % (column.name, ", ".join(allowed))

    def _type_json(self, column):
        return "JSON"

    def _type_date(self, column):
        return "DATE"

    def _type_datetime(self, column):
        return "TIMESTAMP(6) WITHOUT TIME ZONE"

    def _type_time(self, column):
        return "TIME(6) WITHOUT TIME ZONE"

    def _type_timestamp(self, column):
        if column.use_current:
            return "TIMESTAMP(6) WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP(6)"

        return "TIMESTAMP(6) WITHOUT TIME ZONE"

    def _type_binary(self, column):
        return "BYTEA"

    def _type_uuid(self, column):
        return "UUID"

    def _modify_nullable(self, blueprint, column):
        if column.get("nullable"):
            return " NULL"

        return " NOT NULL"

    def _modify_default(self, blueprint, column):
        if column.get("default") is not None:
            return " DEFAULT %s" % self._get_default_value(column.default)

        return ""

    def _modify_increment(self, blueprint, column):
        if column.type in self._serials and column.auto_increment:
            return " PRIMARY KEY"

        return ""

    def _get_dbal_column_type(self, type_):
        """
        Get the dbal column type.

        :param type_: The fluent type
        :type type_: str

        :rtype: str
        """
        type_ = type_.lower()

        if type_ == "enum":
            return "string"

        return super(PostgresSchemaGrammar, self)._get_dbal_column_type(type_)
