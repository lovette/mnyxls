from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from mysqlstmt import Update

from .configtypes import ConfigRewriteT
from .dbschema import TABLE_TXNS
from .dbsqlite import db_execute_stmt
from .shared import MnyXlsConfigError, config_warning, get_select_values, parse_yyyymmdd_flex, split_category_pair, validate_config_typed_dict
from .shared_txns import apply_txns_select_where

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Sequence

    from .configtypes import MainConfigFileT

# Use a named logger instead of root logger
logger = logging.getLogger("mnyxls")


######################################################################
# Public functions


def rewrites_validate_config(config: MainConfigFileT, config_rewrites: Sequence[ConfigRewriteT]) -> None:
    """Validate txn `rewrites` rules from user configuration file.

    Args:
        config (MainConfigFileT): Main configuration file.
        config_rewrites (Sequence[ConfigRewriteT]): Rewrite rules.

    Raises:
        MnyXlsConfigError
    """
    for idx, rewrite_config in enumerate(config_rewrites):
        config_keys = ("rewrites", str(idx))

        validate_config_typed_dict(rewrite_config, ConfigRewriteT, config, [])

        if len(rewrite_config) == 1:
            # "select" is the only key? we need an update directive!
            raise MnyXlsConfigError("At least one modify value field is required.", config, config_keys)

        rewrite_select = rewrite_config.get("select")
        assert rewrite_select is not None

        # Sanity check select criteria
        for select_key in rewrite_select:
            values = get_select_values(select_key, rewrite_select)
            if not values:
                raise MnyXlsConfigError("At least one value is required.", config, (*config_keys, select_key))

        if rewrite_select.get("date_from") or rewrite_select.get("date_to"):
            for k in ("date_from", "date_to"):
                v = rewrite_select.get(k)
                if v and not parse_yyyymmdd_flex(v):
                    raise MnyXlsConfigError(f"'{v}': Invalid date spec.", config, (*config_keys, k))

            if rewrite_select.get("yyyy"):
                config_warning("`date_from` and `date_to` are ignored when `yyyy` is set.", config, config_keys)

        new_txndate = rewrite_config.get("txndate")
        if new_txndate and not parse_yyyymmdd_flex(new_txndate):
            raise MnyXlsConfigError(f"'{new_txndate}': Invalid date spec.", config, (*config_keys, "txndate"))


def rewrites_apply_txns(conn: sqlite3.Connection, config: MainConfigFileT) -> None:  # noqa: C901
    """Apply Txn rewrites from user configuration file.

    Args:
        conn (sqlite3.Connection): SQLite connection.
        config (dict): Configuration file settings.
    """

    def _rewrite_modify_set_values(rewrite_modify: ConfigRewriteT, q_update: Update) -> None:
        for field in rewrite_modify:
            if field == "category":
                category_pair = rewrite_modify.get("category")
                assert category_pair is not None
                if category_pair:
                    category, subcategory = split_category_pair(category_pair)
                    q_update.set_value("Category", category)
                    q_update.set_value("Subcategory", subcategory)
            elif field == "memo":
                new_memo = rewrite_modify.get("memo")
                assert new_memo is not None
                q_update.set_value("Memo", new_memo)
            elif field == "payee":
                new_payee = rewrite_modify.get("payee")
                assert new_payee is not None
                if new_payee:
                    q_update.set_value("Payee", new_payee)
            elif field == "txndate":
                new_txndate = rewrite_modify.get("txndate")
                assert new_txndate is not None
                new_txndate = parse_yyyymmdd_flex(new_txndate)
                assert new_txndate is not None
                q_update.set_value("`Date`", new_txndate)
            elif field != "select":
                # TypedDict field validation should prevent this from being possible.
                raise AssertionError(f"'{field}': Unrecognized rewrite directive.")

    def _apply_rewrite(rewrite_config: ConfigRewriteT) -> int:
        rewrite_select = rewrite_config.get("select")
        assert rewrite_select is not None

        q_update = Update(TABLE_TXNS)

        apply_txns_select_where(conn, q_update, rewrite_select)
        _rewrite_modify_set_values(rewrite_config, q_update)

        with conn:
            cur = db_execute_stmt(conn, q_update)
            return cur.rowcount

    rewrites = config.get("rewrites")
    if rewrites:
        for idx, rewrite in enumerate(rewrites):
            rowcount = _apply_rewrite(rewrite)
            logger.debug(f"Rewrite txns rule[{idx}] modified {rowcount} transactions.")
