# -*- coding: utf-8 -*-

from .grammar import QueryGrammar
from ...utils import basestring


class PostgresQueryGrammar(QueryGrammar):

    _operators = [
        "=",
        "<",
        ">",
        "<=",
        ">=",
        "<>",
        "!=",
        "like",
        "not like",
        "between",
        "ilike",
        "&",
        "|",
        "#",
        "<<",
        ">>",
    ]

    marker = "%s"

    def _compile_lock(self, query, value):
        """
        Compile the lock into SQL

        :param query: A QueryBuilder instance
        :type query: QueryBuilder

        :param value: The lock value
        :type value: bool or str

        :return: The compiled lock
        :rtype: str
        """
        if isinstance(value, basestring):
            return value

        if value:
            return "FOR UPDATE"

        return "FOR SHARE"

    def compile_update(self, query, values):
        """
        Compile an update statement into SQL

        :param query: A QueryBuilder instance
        :type query: QueryBuilder

        :param values: The update values
        :type values: dict

        :return: The compiled update
        :rtype: str
        """
        table = self.wrap_table(query.from__)

        columns = self._compile_update_columns(values)

        from_ = self._compile_update_from(query)

        where = self._compile_update_wheres(query)

        return ("UPDATE %s SET %s%s %s" % (table, columns, from_, where)).strip()

    def _compile_update_columns(self, values):
        """
        Compile the columns for the update statement

        :param values: The columns
        :type values: dict

        :return: The compiled columns
        :rtype: str
        """
        columns = []

        for key, value in values.items():
            columns.append("%s = %s" % (self.wrap(key), self.parameter(value)))

        return ", ".join(columns)

    def _compile_update_from(self, query):
        """
        Compile the "from" clause for an update with a join.

        :param query: A QueryBuilder instance
        :type query: QueryBuilder

        :return: The compiled sql
        :rtype: str
        """
        if not query.joins:
            return ""

        froms = []

        for join in query.joins:
            froms.append(self.wrap_table(join.table))

        if len(froms):
            return " FROM %s" % ", ".join(froms)

        return ""

    def _compile_update_wheres(self, query):
        """
        Compile the additional where clauses for updates with joins.

        :param query: A QueryBuilder instance
        :type query: QueryBuilder

        :return: The compiled sql
        :rtype: str
        """
        base_where = self._compile_wheres(query)

        if not query.joins:
            return base_where

        join_where = self._compile_update_join_wheres(query)

        if not base_where.strip():
            return "WHERE %s" % self._remove_leading_boolean(join_where)

        return "%s %s" % (base_where, join_where)

    def _compile_update_join_wheres(self, query):
        """
        Compile the "join" clauses for an update.

        :param query: A QueryBuilder instance
        :type query: QueryBuilder

        :return: The compiled sql
        :rtype: str
        """
        join_wheres = []

        for join in query.joins:
            for clause in join.clauses:
                join_wheres.append(self._compile_join_constraints(clause))

        return " ".join(join_wheres)

    def compile_insert_get_id(self, query, values, sequence=None):
        """
        Compile an insert and get ID statement into SQL.

        :param query: A QueryBuilder instance
        :type query: QueryBuilder

        :param values: The values to insert
        :type values: dict

        :param sequence: The id sequence
        :type sequence: str

        :return: The compiled statement
        :rtype: str
        """
        if sequence is None:
            sequence = "id"

        return "%s RETURNING %s" % (
            self.compile_insert(query, values),
            self.wrap(sequence),
        )

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

    def compile_truncate(self, query):
        """
        Compile a truncate table statement into SQL.

        :param query: A QueryBuilder instance
        :type query: QueryBuilder

        :return: The compiled statement
        :rtype: str
        """
        return {"TRUNCATE %s RESTART IDENTITY" % self.wrap_table(query.from__): {}}
