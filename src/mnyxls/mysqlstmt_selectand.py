from mysqlstmt import Select

######################################################################
# SelectAnd class


class SelectAnd(Select):
    """A class used to represent an Select(where_predicate="AND")."""

    def __init__(self, *args, **kwargs) -> None:  # noqa: D107
        super().__init__(*args, where_predicate="AND", **kwargs)
