from __future__ import annotations

from typing import TYPE_CHECKING

from mysqlstmt import Insert

if TYPE_CHECKING:
    from collections.abc import Sequence

    from mysqlstmt.stmt import SQLPReturnT

######################################################################
# Upsert class


class Upsert(Insert):
    """A class used to represent an UPSERT SQL statement for MySQL.

    This class extends the Insert class to provide functionality for
    creating an UPSERT statement, which inserts a new row or updates
    an existing row if a duplicate key is found.

    Examples: ::

        >>> q = Upsert("t1")
        >>> q.on_conflict("t1c1")
        >>> q.set_value("t1c1", "t1v1")
        >>> q.set_value("t1c2", "t1v2")
        >>> q.sql()
        >>> ('INSERT INTO t1 (`t1c1`, `t1c2`) VALUES (?, ?) ON CONFLICT (`t1c1`) DO UPDATE SET `t1c2`=?', ['t1v1', 't1v2', 't1v2'])

    """

    def __init__(self, table: str | None, *args, **kwargs) -> None:  # noqa: D107
        super().__init__(table, *args, **kwargs)
        self._conflict_columns: Sequence[str] = []
        self._do_nothing = False

    def on_conflict(self, list_or_str: str | Sequence[str]) -> Upsert:
        """Specify the columns to check for conflicts.

        Args:
            list_or_str (str or list of str): The column(s) to check for conflicts.

        Returns:
            Upsert: The current Upsert instance.
        """
        self._conflict_columns = [list_or_str] if isinstance(list_or_str, str) else list_or_str
        return self

    def do_nothing(self) -> Upsert:
        """Specify that no action should be taken on conflict.

        Returns:
            Upsert: The current Upsert instance.
        """
        self._do_nothing = True
        return self

    def sqlp(self) -> SQLPReturnT:  # noqa: C901, D102, PLR0912, PLR0915
        col_names = self._columns  # Be careful not to overwrite!
        param_values = []

        if not self._table_name:
            msg = "No table is specified"
            raise ValueError(msg)

        # SQLite UPSERT syntax as of 2/2025:
        #
        # INSERT INTO tbl_name (col_name,...) VALUES (...)
        # ON CONFLICT(col_name)
        # DO UPDATE SET col_name=value, ...;

        sql = ["INSERT"]

        if self.query_options:
            sql.extend(self.query_options)

        sql.append("INTO")
        sql.append(self._table_name)

        if self._values or self._values_raw:
            if col_names:
                msg = "columns cannot be explicitly set when set_value or set_raw_value is used"
                raise ValueError(msg)
            if self._batch_values:
                msg = "set_batch_value is incompatible with set_value and set_raw_value"
                raise ValueError(msg)
            if self._select:
                msg = "set_value and set_raw_value are incompatible with INSERT...SELECT"
                raise ValueError(msg)

            col_names = []
            inline_values = []

            if self._values:
                for col, val in self._values.items():
                    col_names.append(col)
                    self.parameterize_values(val, inline_values, param_values)

            for col in self._values_raw:
                val, val_params = self._values_raw[col]
                col_names.append(col)
                inline_values.append(val)
                if val_params is not None and self.placeholder:
                    param_values.extend(val_params)

            assert len(col_names) == len(inline_values)

            sql.append(f"({', '.join([self.quote_col_ref(col) for col in col_names])})")
            sql.append(f"VALUES ({', '.join(inline_values)})")

        elif self._batch_values:
            msg = "Upsert() does not currently support batch values"
            raise ValueError(msg)

        elif self._select:
            msg = "Upsert() does not currently support INSERT...SELECT"
            raise ValueError(msg)

        else:
            msg = "No values are specified"
            raise ValueError(msg)

        sql.append("ON CONFLICT")
        sql.append(f"({', '.join([self.quote_col_ref(col) for col in self._conflict_columns])})")

        if not self._do_nothing:
            sql.append("DO UPDATE SET")

            if self._values:
                set_cols = []

                for col, val in self._values.items():
                    if col not in self._conflict_columns:
                        inline_values = []
                        self.parameterize_values(val, inline_values, param_values)
                        _col = self.quote_col_ref(col)
                        _val = inline_values[0] if inline_values else val
                        set_cols.append(f"{_col}={_val}")

                sql.append(", ".join(set_cols))

            for col in self._values_raw:
                set_cols = []

                if col not in self._conflict_columns:
                    val, val_params = self._values_raw[col]
                    _col = self.quote_col_ref(col)
                    _val = inline_values[0] if inline_values else val
                    set_cols.append(f"{_col}={_val}")
                    if val_params is not None and self.placeholder:
                        param_values.extend(val_params)

                sql.append(", ".join(set_cols))
        else:
            sql.append("DO NOTHING")

        if self.placeholder:
            return " ".join(sql), tuple(param_values) if param_values else None
        assert not param_values
        return " ".join(sql), None
