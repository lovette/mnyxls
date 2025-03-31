from __future__ import annotations

from typing import TYPE_CHECKING

from mysqlstmt import Select

if TYPE_CHECKING:
    from collections.abc import Sequence

    from mysqlstmt.stmt import SQLPReturnT

######################################################################
# CreateView class


class CreateView(Select):
    """A class used to represent a CREATE VIEW SQL statement for SQLite.

    This class extends the Select class to provide functionality for
    creating a view in the database.

    Examples: ::

        >>> q = CreateView("v1")
        >>> q.from_table("t1")
        >>> q.column("t1c1")
        >>> q.column("t1c2")
        >>> q.sql()
        >>> ('CREATE VIEW v1 AS SELECT `t1c1`, `t1c2` FROM t1', None)

    """

    def __init__(  # noqa: D417
        self,
        view_name: str,
        table_name: str | Sequence[str] | None = None,
        temp: bool = False,
        if_not_exists: bool = False,
        *args,
        **kwargs,
    ) -> None:
        """Constructor.

        Args:
            view_name (str): View name.
            table_name (str | Sequence[str] | None, optional): Select table name. Defaults to None.
            temp (bool, optional): True if view is temporary. Defaults to False.
            if_not_exists (bool, optional): True to create only if does not exist. Defaults to False.
        """
        super().__init__(table_name, *args, **kwargs)

        self.view_name = view_name
        self.temp = temp
        self.if_not_exists = if_not_exists

    def sqlp(self) -> SQLPReturnT:  # noqa: D102
        select_sql, select_params = super().sqlp()

        # SQLite UPSERT syntax as of 2/2025:
        #
        # > CREATE [TEMPORARY] VIEW [IF NOT EXISTS]
        # > view-name [(column, ...)]
        # > AS select_statement;

        sql = (
            s
            for s in [
                "CREATE",
                "TEMPORARY" if self.temp else None,
                "VIEW",
                "IF NOT EXISTS" if self.if_not_exists else None,
                self.view_name,
                "AS",
                select_sql,
            ]
            if s is not None
        )

        if self.placeholder:
            return " ".join(sql), tuple(select_params) if select_params else None
        assert not select_params
        return " ".join(sql), None
