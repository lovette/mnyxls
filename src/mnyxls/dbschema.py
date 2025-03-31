from collections.abc import Sequence
from typing import NotRequired, TypedDict

######################################################################
# Types

TableSchemaTableT = str | Sequence[str]
TableSchemaForeignKeyT = tuple[TableSchemaTableT, str] | tuple[TableSchemaTableT, str, TableSchemaTableT]


class TableSchemaDict(TypedDict):
    """Schema for a table in the database."""

    columns: Sequence[Sequence[str]]
    primary_key: NotRequired[TableSchemaTableT | None]
    indexes: NotRequired[Sequence[tuple[TableSchemaTableT, bool]] | None]
    foreign_keys: NotRequired[Sequence[TableSchemaForeignKeyT] | None]


######################################################################
# Table names

TABLE_ACCOUNT_BALANCES = "AccountBalances"
TABLE_CATEGORY_BALANCES = "CategoryBalances"
TABLE_ACCOUNTS = "Accounts"
TABLE_CATEGORIES = "Categories"
TABLE_LOANS = "Loans"
TABLE_PAYEES = "Payees"
TABLE_TXNS = "Txns"
TABLE_TXNS_INV = "TxnsInv"

######################################################################
# Schemas for tables in the database

TABLE_SCHEMAS: dict[str, TableSchemaDict] = {
    TABLE_ACCOUNTS: {
        "columns": (
            ("Account", "TEXT", "PRIMARY KEY"),
            ("AccountClassification", "TEXT"),
            ("AccountCategory", "TEXT"),
            ("Abbreviation", "TEXT"),
            ("BankName", "TEXT"),
            ("AccountNumber", "TEXT"),
            ("AccountLimit", "DECIMAL(15,4)"),
            ("OpeningBalance", "DECIMAL(15,4)"),
            ("TxnDateMin", "DATE"),
            ("TxnDateMax", "DATE"),
            ("XferOnly", "INTEGER"),
            ("OpenedDate", "DATE"),
            ("ClosedDate", "DATE"),
        ),
        "indexes": (
            ("AccountClassification", False),
            ("AccountCategory", False),
        ),
    },
    TABLE_LOANS: {
        "columns": (
            ("Loan", "TEXT", "PRIMARY KEY"),
            ("Abbreviation", "TEXT"),
            ("AccountNumber", "TEXT"),
            ("Frequency", "TEXT"),
            ("Type", "TEXT"),
            ("InterestWhen", "TEXT"),
            ("Length", "TEXT"),
            ("LoanAmount", "DECIMAL(15,4)"),
            ("InterestRate", "DECIMAL(15,4)"),
            ("PrincipalInterest", "DECIMAL(15,4)"),
            ("BalloonAmount", "DECIMAL(15,4)"),
            ("Payment", "DECIMAL(15,4)"),
            ("Fees", "DECIMAL(15,4)"),
        ),
        "foreign_keys": (("Loan", TABLE_ACCOUNTS, "Account"),),
    },
    TABLE_PAYEES: {
        "columns": (
            ("Payee", "TEXT", "PRIMARY KEY"),
            ("TxnDateMin", "DATE"),
            ("TxnDateMax", "DATE"),
        ),
    },
    TABLE_CATEGORIES: {
        "columns": (
            ("Category", "TEXT"),
            ("Subcategory", "TEXT"),
            ("TxnType", "TEXT"),
            ("TxnClass", "TEXT"),
            ("TxnDateMin", "DATE"),
            ("TxnDateMax", "DATE"),
        ),
        "primary_key": ("Category", "Subcategory"),
        "indexes": (
            ("TxnType", False),
            ("TxnClass", False),
        ),
    },
    TABLE_ACCOUNT_BALANCES: {
        "columns": (
            ("Account", "TEXT", "NOT NULL"),
            ("Date", "DATE", "NOT NULL"),
            ("Balance", "DECIMAL(15,4)", "NOT NULL"),
        ),
        "indexes": ((("Account", "Date"), True),),
        "foreign_keys": (("Account", TABLE_ACCOUNTS),),
    },
    TABLE_CATEGORY_BALANCES: {
        "columns": (
            ("Category", "TEXT", "NOT NULL"),
            ("Subcategory", "TEXT"),  # Not used but required for foreign key reference
            ("Date", "DATE", "NOT NULL"),
            ("Balance", "DECIMAL(15,4)", "NOT NULL"),
        ),
        "indexes": ((("Category", "Date"), True),),
        "foreign_keys": ((("Category", "Subcategory"), TABLE_CATEGORIES),),
    },
    TABLE_TXNS: {
        "columns": (
            ("N", "INTEGER", "PRIMARY KEY"),
            ("Num", "TEXT"),
            ("Date", "DATE", "NOT NULL"),
            ("Account", "TEXT", "NOT NULL"),
            ("Payee", "TEXT"),
            ("Category", "TEXT", "NOT NULL"),
            ("Subcategory", "TEXT"),
            ("XferAccount", "TEXT"),
            ("Amount", "DECIMAL(15,4)", "NOT NULL"),
            ("C", "TEXT"),
            ("Split", "TEXT"),
            ("Memo", "TEXT"),
        ),
        "indexes": (
            ("Date", False),
            ("Account", False),
            ("Payee", False),
            (("Category", "Subcategory"), False),
            ("XferAccount", False),
        ),
        "foreign_keys": (
            ("Account", TABLE_ACCOUNTS),
            ("Payee", TABLE_PAYEES),
            ("XferAccount", TABLE_ACCOUNTS, "Account"),
            (("Category", "Subcategory"), TABLE_CATEGORIES),
        ),
    },
    TABLE_TXNS_INV: {
        "columns": (
            ("N", "INTEGER", "PRIMARY KEY"),
            ("Date", "DATE"),
            ("Account", "TEXT", "NOT NULL"),
            ("Investment", "TEXT", "NOT NULL"),
            ("Activity", "TEXT", "NOT NULL"),
            ("C", "TEXT"),
            ("Quantity", "DECIMAL(15,4)"),
            ("Price", "DECIMAL(15,4)"),
            ("Commission", "DECIMAL(15,4)"),
            ("Total", "DECIMAL(15,4)"),
            ("XferAccount", "TEXT"),
            ("Category", "TEXT"),
            ("Subcategory", "TEXT"),
            ("Memo", "TEXT"),
        ),
        "indexes": (
            ("Date", False),
            ("Account", False),
            ("XferAccount", False),
            ("Investment", False),
            (("Category", "Subcategory"), False),
        ),
        "foreign_keys": (
            ("Account", TABLE_ACCOUNTS),
            ("XferAccount", TABLE_ACCOUNTS, "Account"),
        ),
    },
}

######################################################################
# Helper functions


def table_schema_columns(table_name: str) -> list[str]:
    """Get table column names from TABLE_SCHEMAS.

    Args:
        table_name (str): Table name.

    Returns:
        list[str]
    """
    return [col[0] for col in TABLE_SCHEMAS[table_name]["columns"]]
