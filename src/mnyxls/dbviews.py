from mysqlstmt import Stmt

from .dbschema import TABLE_CATEGORIES, TABLE_TXNS
from .mysqlstmt_createview import CreateView
from .report import TxnClass, TxnType

ViewSchemaT = Stmt | str


######################################################################
# View names

VIEW_TXNS_WITHTYPEANDCLASS = "TxnsWithTypeAndClass"

######################################################################
# VIEW_TXNS_WITHTYPEANDCLASS


def _view_txns_withtypeandclass() -> CreateView:
    join_cond = (
        "Txns.Category = Categories.Category",
        "(Txns.Subcategory = Categories.Subcategory OR (Txns.Subcategory IS NULL AND Categories.Subcategory IS NULL))",
    )

    q_select = CreateView(VIEW_TXNS_WITHTYPEANDCLASS)
    q_select.from_table(TABLE_TXNS)

    # Left join because Transfer categories are not in the Categories table.
    # Conditions must be of type `list`.
    q_select.left_join(TABLE_CATEGORIES, list(join_cond))

    q_select.column(f"{TABLE_TXNS}.*")

    # Transfers are not in the Categories table so their TxnType and TxnClass are NULL.
    # Assign them a type so worksheets can use `select.txntype` to filter them.
    q_select.column_expr(
        f" \
        CASE \
        WHEN Txns.XferAccount IS NOT NULL THEN '{TxnType.TRANSFER.value}' \
        ELSE {TABLE_CATEGORIES}.`TxnType` \
        END",
        named="TxnType",
    )

    q_select.column_expr(
        f" \
        CASE \
        WHEN Txns.XferAccount IS NOT NULL THEN '{TxnClass.TRANSFER.value}' \
        ELSE {TABLE_CATEGORIES}.`TxnClass` \
        END",
        named="TxnClass",
    )

    q_select.column_expr(
        " \
        CASE \
        WHEN Txns.XferAccount IS NOT NULL THEN NULL \
        WHEN Txns.Subcategory IS NULL THEN Txns.Category \
        ELSE (Txns.Category || ' : ' || Txns.Subcategory) \
        END",
        named="CategorySubcategory",
    )

    return q_select


######################################################################
# View schemas

VIEW_SCHEMAS: dict[str, ViewSchemaT] = {
    VIEW_TXNS_WITHTYPEANDCLASS: _view_txns_withtypeandclass(),
}
