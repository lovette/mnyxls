from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

from .dbschema import TABLE_ACCOUNTS, TABLE_ERAS
from .dbviews import VIEW_TXNS_WITHTYPEANDCLASS
from .mysqlstmt_selectand import SelectAnd  # type: ignore[reportMissingImport]
from .shared import (
    config_select_remove,
    pd_read_sql,
    pluralize,
)
from .shared_txns import apply_txns_select_where
from .worksheet import MoneyWorksheet

if TYPE_CHECKING:
    import sqlite3

    from .configtypes import WorksheetConfigT
    from .workbook import MoneyWorkbook


######################################################################
# Transactions worksheet


class MoneyWorksheetTxnsBase(MoneyWorksheet):
    """Base class common to "txns:*" worksheets."""

    ######################################################################
    # Class methods for expanding `foreach` directives

    @classmethod
    def expand_foreach_txntype(
        cls,
        workbook: MoneyWorkbook,
        conn: sqlite3.Connection,
        worksheet_key: str,
        worksheet_base_config: WorksheetConfigT,
    ) -> int:
        """Expand `foreach` directive and add sheets to workbook.

        foreach: "txntype"

        Args:
            workbook (MoneyWorkbook): Workbook.
            conn (sqlite3.Connection): SQLite connection.
            worksheet_key (str): Worksheet configuration key (typically the sheet name.)
            worksheet_base_config (WorksheetConfigT): Worksheet configuration.

        Returns:
            int: Number of worksheets added. If 0, base worksheet will be added.
        """
        worksheet_count = 0

        def _list_txntypes() -> list[str]:
            q_select = SelectAnd(VIEW_TXNS_WITHTYPEANDCLASS).distinct().column("TxnType").order_by("TxnType")
            apply_txns_select_where(conn, q_select, worksheet_base_config, allow_directives=("txntype",))
            df_select = pd_read_sql(conn, q_select)
            return df_select["TxnType"].tolist() if not df_select.empty else []

        for txn_type_value in _list_txntypes():
            txntypeplural = pluralize(txn_type_value)

            sheet_name = cls.render_sheet_name(
                worksheet_key,
                {"foreach": txntypeplural},
                txntypeplural,
            )

            # Create a worksheet configuration unique to each worksheet.
            worksheet_config = cls.copy_worksheet_config(worksheet_base_config)
            assert "select" in worksheet_config

            # Set conditions that will be applied to the SELECT query.
            # This *overwrites* any existing condition.
            worksheet_config["select"]["txntype"] = txn_type_value

            workbook.add_worksheet(cls(workbook, sheet_name, worksheet_config))
            worksheet_count += 1

        return worksheet_count

    @classmethod
    def expand_foreach_txnclass(
        cls,
        workbook: MoneyWorkbook,
        conn: sqlite3.Connection,
        worksheet_key: str,
        worksheet_base_config: WorksheetConfigT,
    ) -> int:
        """Expand `foreach` directive and add sheets to workbook.

        foreach: "txnclass"

        Args:
            workbook (MoneyWorkbook): Workbook.
            conn (sqlite3.Connection): SQLite connection.
            worksheet_key (str): Worksheet configuration key (typically the sheet name.)
            worksheet_base_config (WorksheetConfigT): Worksheet configuration.

        Returns:
            int: Number of worksheets added. If 0, base worksheet will be added.
        """

        def _list_txnclasses() -> list[str]:
            q_select = SelectAnd(VIEW_TXNS_WITHTYPEANDCLASS).distinct().column("TxnClass").order_by("TxnClass")
            apply_txns_select_where(conn, q_select, worksheet_base_config, allow_directives=("txnclass",))
            df_select = pd_read_sql(conn, q_select)
            return df_select["TxnClass"].tolist() if not df_select.empty else []

        worksheet_count = 0

        for txn_class_value in _list_txnclasses():
            txnclassplural = pluralize(txn_class_value)

            sheet_name = cls.render_sheet_name(
                worksheet_key,
                {"foreach": txnclassplural},
                txnclassplural,
            )

            # Create a worksheet configuration unique to each worksheet.
            worksheet_config = cls.copy_worksheet_config(worksheet_base_config)
            assert "select" in worksheet_config

            # Set conditions that will be applied to the SELECT query.
            # This *overwrites* any existing condition.
            worksheet_config["select"]["txnclass"] = txn_class_value

            workbook.add_worksheet(cls(workbook, sheet_name, worksheet_config))
            worksheet_count += 1

        return worksheet_count

    @classmethod
    def expand_foreach_era(
        cls,
        workbook: MoneyWorkbook,
        conn: sqlite3.Connection,
        worksheet_key: str,
        worksheet_base_config: WorksheetConfigT,
    ) -> int:
        """Expand `foreach` directive and add sheets to workbook.

        foreach: "era"

        Args:
            workbook (MoneyWorkbook): Workbook.
            conn (sqlite3.Connection): SQLite connection.
            worksheet_key (str): Worksheet configuration key (typically the sheet name.)
            worksheet_base_config (WorksheetConfigT): Worksheet configuration.

        Returns:
            int: Number of worksheets added. If 0, base worksheet will be added.
        """
        worksheet_count = 0

        q_select = SelectAnd(TABLE_ERAS).column("EraName").order_by("rowid")
        apply_txns_select_where(conn, q_select, worksheet_base_config, allow_directives=("era",))

        df_select = pd_read_sql(conn, q_select)

        for row in df_select.itertuples(index=False):
            era_name = row.EraName

            assert isinstance(era_name, str)

            sheet_name = cls.render_sheet_name(
                worksheet_key,
                {"foreach": era_name},
                era_name,
            )

            # Create a worksheet configuration unique to each worksheet.
            worksheet_config = cls.copy_worksheet_config(worksheet_base_config)
            assert "select" in worksheet_config

            # Set conditions that will be applied to the SELECT query.
            # This *overwrites* any existing condition.
            worksheet_config["select"]["era"] = era_name

            workbook.add_worksheet(cls(workbook, sheet_name, worksheet_config))
            worksheet_count += 1

        return worksheet_count

    @classmethod
    def expand_foreach_account(
        cls,
        workbook: MoneyWorkbook,
        conn: sqlite3.Connection,
        worksheet_key: str,
        worksheet_base_config: WorksheetConfigT,
    ) -> int:
        """Expand `foreach` directive and add sheets to workbook.

        foreach: "account"

        Args:
            workbook (MoneyWorkbook): Workbook.
            conn (sqlite3.Connection): SQLite connection.
            worksheet_key (str): Worksheet configuration key (typically the sheet name.)
            worksheet_base_config (WorksheetConfigT): Worksheet configuration.

        Returns:
            int: Number of worksheets added. If 0, base worksheet will be added.
        """

        def _list_accounts() -> list[str]:
            q_select = SelectAnd(TABLE_ACCOUNTS).distinct().column("Account").order_by("Account")
            apply_txns_select_where(conn, q_select, worksheet_base_config, allow_directives=("account",))
            df_select = pd_read_sql(conn, q_select)
            return df_select["Account"].tolist() if not df_select.empty else []

        worksheet_count = 0
        worksheet_config_clean = cls.copy_worksheet_config(worksheet_base_config)
        assert "select" in worksheet_config_clean

        # Remove selection criteria we explicitly set for each sheet.
        config_select_remove(worksheet_config_clean["select"], ("account",))

        for account_name in _list_accounts():
            sheet_name = cls.render_sheet_name(
                worksheet_key,
                {"foreach": account_name},
                account_name,
            )

            # Create a worksheet configuration unique to each worksheet.
            worksheet_config = cls.copy_worksheet_config(worksheet_config_clean)
            assert "select" in worksheet_config

            # Set conditions that will be applied to the SELECT query.
            # This *overwrites* any existing condition.
            worksheet_config["select"]["account"] = account_name

            workbook.add_worksheet(cls(workbook, sheet_name, worksheet_config))
            worksheet_count += 1

        return worksheet_count

    @classmethod
    def expand_foreach_account_category(
        cls,
        workbook: MoneyWorkbook,
        conn: sqlite3.Connection,
        worksheet_key: str,
        worksheet_base_config: WorksheetConfigT,
    ) -> int:
        """Expand `foreach` directive and add sheets to workbook.

        foreach: "account_category"

        Args:
            workbook (MoneyWorkbook): Workbook.
            conn (sqlite3.Connection): SQLite connection.
            worksheet_key (str): Worksheet configuration key (typically the sheet name.)
            worksheet_base_config (WorksheetConfigT): Worksheet configuration.

        Returns:
            int: Number of worksheets added. If 0, base worksheet will be added.
        """

        def _list_account_categories() -> list[str]:
            q_select = SelectAnd(TABLE_ACCOUNTS).distinct().column("AccountCategory").order_by("AccountCategory")
            apply_txns_select_where(conn, q_select, worksheet_base_config, allow_directives=("account_category",))
            df_select = pd_read_sql(conn, q_select)
            return df_select["AccountCategory"].tolist() if not df_select.empty else []

        worksheet_count = 0
        worksheet_config_clean = cls.copy_worksheet_config(worksheet_base_config)
        assert "select" in worksheet_config_clean

        # Remove selection criteria we explicitly set for each sheet.
        config_select_remove(worksheet_config_clean["select"], ("account_category",))

        for account_category in _list_account_categories():
            sheet_name = cls.render_sheet_name(
                worksheet_key,
                {"foreach": account_category},
                account_category or "No category",
            )

            # Create a worksheet configuration unique to each worksheet.
            worksheet_config = cls.copy_worksheet_config(worksheet_config_clean)
            assert "select" in worksheet_config

            # Set conditions that will be applied to the SELECT query.
            # This *overwrites* any existing condition.
            worksheet_config["select"]["account_category"] = account_category

            workbook.add_worksheet(cls(workbook, sheet_name, worksheet_config))
            worksheet_count += 1

        return worksheet_count

    @classmethod
    def expand_foreach_account_classification(
        cls,
        workbook: MoneyWorkbook,
        conn: sqlite3.Connection,
        worksheet_key: str,
        worksheet_base_config: WorksheetConfigT,
    ) -> int:
        """Expand `foreach` directive and add sheets to workbook.

        foreach: "account_classification"

        Args:
            workbook (MoneyWorkbook): Workbook.
            conn (sqlite3.Connection): SQLite connection.
            worksheet_key (str): Worksheet configuration key (typically the sheet name.)
            worksheet_base_config (WorksheetConfigT): Worksheet configuration.

        Returns:
            int: Number of worksheets added. If 0, base worksheet will be added.
        """

        def _list_account_classifications() -> list[str]:
            q_select = SelectAnd(TABLE_ACCOUNTS).distinct().column("AccountClassification").order_by("AccountClassification")
            apply_txns_select_where(conn, q_select, worksheet_base_config, allow_directives=("account_classification",))
            df_select = pd_read_sql(conn, q_select)
            return df_select["AccountClassification"].tolist() if not df_select.empty else []

        worksheet_count = 0
        worksheet_config_clean = cls.copy_worksheet_config(worksheet_base_config)
        assert "select" in worksheet_config_clean

        # Remove selection criteria we explicitly set for each sheet.
        config_select_remove(worksheet_config_clean["select"], ("account_classification",))

        for account_classification in _list_account_classifications():
            sheet_name = cls.render_sheet_name(
                worksheet_key,
                {"foreach": account_classification},
                account_classification or "No classification",
            )

            # Create a worksheet configuration unique to each worksheet.
            worksheet_config = cls.copy_worksheet_config(worksheet_config_clean)
            assert "select" in worksheet_config

            # Set conditions that will be applied to the SELECT query.
            # This *overwrites* any existing condition.
            worksheet_config["select"]["account_classification"] = account_classification

            workbook.add_worksheet(cls(workbook, sheet_name, worksheet_config))
            worksheet_count += 1

        return worksheet_count

    @classmethod
    def expand_foreach_yyyy(
        cls,
        workbook: MoneyWorkbook,
        conn: sqlite3.Connection,
        worksheet_key: str,
        worksheet_base_config: WorksheetConfigT,
    ) -> int:
        """Expand `foreach` directive and add sheets to workbook.

        foreach: "yyyy"

        Args:
            workbook (MoneyWorkbook): Workbook.
            conn (sqlite3.Connection): SQLite connection.
            worksheet_key (str): Worksheet configuration key (typically the sheet name.)
            worksheet_base_config (WorksheetConfigT): Worksheet configuration.

        Returns:
            int: Number of worksheets added. If 0, base worksheet will be added.
        """

        def _list_txn_yyyy() -> list[int]:
            q_select = SelectAnd(VIEW_TXNS_WITHTYPEANDCLASS)
            q_select.column_expr("CAST(strftime('%Y', `Date`) AS INTEGER)", named="yyyy")
            q_select.group_by("yyyy")
            q_select.order_by("yyyy")
            apply_txns_select_where(conn, q_select, worksheet_base_config, allow_directives=("yyyy", "date_from", "date_to"))
            df_select = pd_read_sql(conn, q_select)
            return df_select["yyyy"].tolist() if not df_select.empty else []

        worksheet_count = 0

        for yyyy in _list_txn_yyyy():
            sheet_name = cls.render_sheet_name(
                worksheet_key,
                {"foreach": str(yyyy)},
                f"{worksheet_key} {yyyy}",
            )

            # Create a worksheet configuration unique to each worksheet.
            worksheet_config = cls.copy_worksheet_config(worksheet_base_config)
            assert "select" in worksheet_config

            # Set conditions that will be applied to the SELECT query.
            # This *overwrites* any existing condition.
            worksheet_config["select"]["yyyy"] = yyyy

            workbook.add_worksheet(cls(workbook, sheet_name, worksheet_config))
            worksheet_count += 1

        return worksheet_count

    @classmethod
    def expand_foreach_10y(
        cls,
        workbook: MoneyWorkbook,
        conn: sqlite3.Connection,
        worksheet_key: str,
        worksheet_base_config: WorksheetConfigT,
    ) -> int:
        """Expand `foreach` directive and add sheets to workbook.

        foreach: "10y"

        Args:
            workbook (MoneyWorkbook): Workbook.
            conn (sqlite3.Connection): SQLite connection.
            worksheet_key (str): Worksheet configuration key (typically the sheet name.)
            worksheet_base_config (WorksheetConfigT): Worksheet configuration.

        Returns:
            int: Number of worksheets added. If 0, base worksheet will be added.
        """

        def _list_txn_yyyy() -> list[int]:
            q_select = SelectAnd(VIEW_TXNS_WITHTYPEANDCLASS)
            q_select.column_expr("CAST(strftime('%Y', `Date`) AS INTEGER)", named="yyyy")
            q_select.group_by("yyyy")
            q_select.order_by("yyyy")
            apply_txns_select_where(conn, q_select, worksheet_base_config, allow_directives=("yyyy", "date_from", "date_to"))
            df_select = pd_read_sql(conn, q_select)
            return df_select["yyyy"].tolist() if not df_select.empty else []

        worksheet_count = 0

        decades: dict[int, list[int]] = defaultdict(list)
        for yyyy in _list_txn_yyyy():
            decades[yyyy // 10 * 10].append(yyyy)

        for decade, decade_years in decades.items():
            sheet_name = cls.render_sheet_name(
                worksheet_key,
                {"foreach": f"{decade}-{decade + 9}"},
                f"{worksheet_key} {decade}-{decade + 9}",
            )

            # Create a worksheet configuration unique to each worksheet.
            worksheet_config = cls.copy_worksheet_config(worksheet_base_config)
            assert "select" in worksheet_config

            # Set conditions that will be applied to the SELECT query.
            # This *overwrites* any existing condition.
            worksheet_config["select"]["yyyy"] = decade_years

            workbook.add_worksheet(cls(workbook, sheet_name, worksheet_config))
            worksheet_count += 1

        return worksheet_count
