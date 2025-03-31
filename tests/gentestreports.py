#!/usr/bin/env python3
"""Generate test report data files.

Usage:
% cd tests
% ./gentestreports.py --output sample_reports/txns.csv
"""

import random
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import MappingProxyType

import click
import numpy as np
import pandas as pd

from mnyxls.currencydecimal import CurrencyDecimal, currency_from_value

CHECKING_ACCOUNT = "Checking"  # source of income and transfers
SAVINGS_ACCOUNT = "Savings"
CREDIT_ACCOUNTS = (
    "Mastercard",
    "Visa",
)

# Categories not listed are assumed to be expenses
INCOME_CATEGORIES = (
    "Freelance Income",
    "Interest Income",
    "Wages & Salary : Bonus",
    "Wages & Salary : Gross Pay",
)

# > [account][payee] = [categories]
ACCOUNTS_PAYEES_CATEGORIES: Mapping[str, Mapping[str, Sequence[str | Sequence[str]]]] = {
    CHECKING_ACCOUNT: {
        "ACME Corp": (
            (
                "Wages & Salary : Gross Pay",
                "Wages & Salary : Bonus",
            ),
        ),
        "City of San Francisco": ("Taxes : Local Taxes",),
        "Highland Apartments": ("Rent",),
        "Joe's Consulting": ("Freelance Income",),
        "Local Utility Co": ("Utilities",),
        "United States Treasury": ("Taxes : Federal Taxes",),
    },
    SAVINGS_ACCOUNT: {
        "Wells Fargo": ("Interest Income",),
    },
    "Credit cards": {
        "Amazon": ("Shopping : Online",),
        "The Vet": ("Pets",),
        "CVS": ("Pharmacy : OTC",),
        "Kroger": ("Groceries : Grocery Store",),
        "McDonald's": ("Dining Out",),
        "Neighborhood Market": ("Groceries : Farmer's Market",),
        "Target": ("Shopping : Household",),
        "Walgreens": ("Pharmacy : General",),
        "Walmart": ("Shopping : Household",),
        "Wendy's": ("Dining Out",),
    },
}

# All credit card accounts have the same payees
for accout in CREDIT_ACCOUNTS:
    ACCOUNTS_PAYEES_CATEGORIES[accout] = ACCOUNTS_PAYEES_CATEGORIES["Credit cards"]
del ACCOUNTS_PAYEES_CATEGORIES["Credit cards"]

ACCOUNTS_PAYEES_CATEGORIES = MappingProxyType(ACCOUNTS_PAYEES_CATEGORIES)  # Change to read only
ACCOUNT_NAMES = tuple(sorted(ACCOUNTS_PAYEES_CATEGORIES.keys()))
FIVE_PCT = 0.05  # Assign Num, Memo to only 5% of transactions
CRLF = "\r\n"  # Write CSV with CRLF line endings to match Money's output
CURRENCY_ZERO = CurrencyDecimal("0")

######################################################################
# Helper functions


def _rand_currency(rand_min: float, rand_max: float) -> CurrencyDecimal:
    d = currency_from_value(str(round(random.uniform(rand_min, rand_max), 2)))
    assert d is not None
    return d


def _rand_currency_portion(amount: CurrencyDecimal, portion_min: float, portion_max: float) -> CurrencyDecimal:
    amount_f = float(amount)
    return _rand_currency(amount_f * portion_min, amount_f * portion_max)


def _generate_account_txns(account: str) -> pd.DataFrame:
    # Generate a dataframe with random transactions for the given account.
    # Each account has a different set of payees and categories.

    def _random_date(start: date, end: date) -> date:
        # Generate random dates within a range
        delta = end - start
        random_days = random.randint(0, delta.days)
        return start + timedelta(days=random_days)

    start_date = datetime(2018, 1, 1, tzinfo=UTC).date()
    end_date = datetime(2022, 12, 31, tzinfo=UTC).date()
    num_years = 5
    rows_per_month = 15
    total_rows = rows_per_month * 12 * num_years
    is_credit_account = account in CREDIT_ACCOUNTS

    assert account in ACCOUNTS_PAYEES_CATEGORIES
    account_payee_categories = ACCOUNTS_PAYEES_CATEGORIES.get(account, {})
    account_payees = list(account_payee_categories.keys())

    data = {
        "Num": [],
        "Date": [],
        "Payee": [],
        "Account": [],
        "Memo": [],
        "Category": [],
        "Amount": [],
        "Split": [],
    }

    for i in range(total_rows):
        txndate = _random_date(start_date, end_date)
        payee = random.choice(account_payees)
        categories = account_payee_categories[payee]
        category_or_splits = random.choice(categories)
        is_split = not isinstance(category_or_splits, str)
        is_income = (category_or_splits if not is_split else category_or_splits[0]) in INCOME_CATEGORIES
        amount_min, amount_max = (100.00, 5000.00) if is_income else (-1000.00, -1.00)
        amount_txn = _rand_currency(amount_min, amount_max)

        data["Date"].append(txndate)
        data["Num"].append(f"Check {len(data['Num']) + 1}" if (not is_credit_account and not is_income and random.random() < FIVE_PCT) else None)
        data["Payee"].append(payee)
        data["Account"].append(account)
        data["Memo"].append(f"Memo {i % 20 + 1}" if random.random() < FIVE_PCT else None)
        data["Category"].append(category_or_splits if not is_split else "Split/Multiple Categories")
        data["Amount"].append(amount_txn)
        data["Split"].append("X" if is_split else None)

        if is_split:
            for i_split, split_category in enumerate(category_or_splits, 1):
                amount_split = _rand_currency_portion(amount_txn, 0.25, 0.75) if i_split < len(category_or_splits) else amount_txn
                amount_txn -= amount_split

                # Splits have an Amount, Memo and Category but no Date, Payee or Account.
                # We set Date now to ensure the split is sorted correctly and will remove it later.
                data["Date"].append(txndate)
                data["Num"].append(None)
                data["Payee"].append(None)
                data["Account"].append(None)
                data["Memo"].append(None)
                data["Category"].append(split_category)
                data["Amount"].append(amount_split)
                data["Split"].append("Y")

            assert amount_txn == CURRENCY_ZERO

    return pd.DataFrame(data)


def _generate_credit_payoffs(checking_name: str, account_dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    # Add monthly balance payoff transfers from checking to credit accounts

    df_transfers = pd.DataFrame()
    df_checking_debits = pd.DataFrame()
    credit_account_dfs = {account: df for account, df in account_dfs.items() if account in CREDIT_ACCOUNTS}

    for credit_account, df_account in credit_account_dfs.items():
        df_account_credits = pd.DataFrame()

        df_account_periods = df_account.loc[:, ["Date", "Amount"]]  # new dataframe with only Date and Amount columns
        df_account_periods["Month"] = pd.to_datetime(df_account_periods["Date"]).dt.to_period("M")

        monthly_balance = df_account_periods.groupby("Month")["Amount"].sum().reset_index()

        for _, row in monthly_balance.iterrows():
            # Add transaction to the credit account
            credit_transaction = {
                "Num": None,
                "Date": row["Month"].end_time.date(),
                "Payee": None,
                "Account": credit_account,
                "Memo": None,
                "Category": f"Transfer From : {checking_name}",
                "Amount": -row["Amount"],
            }

            # Add corresponding transaction to checking account
            checking_transaction = {
                "Num": None,
                "Date": row["Month"].end_time.date(),
                "Payee": None,
                "Account": checking_name,
                "Memo": None,
                "Category": f"Transfer To : {credit_account}",
                "Amount": row["Amount"],
            }

            df_account_credits = pd.concat([df_account_credits, pd.DataFrame([credit_transaction])], ignore_index=True)
            df_checking_debits = pd.concat([df_checking_debits, pd.DataFrame([checking_transaction])], ignore_index=True)

        df_transfers = pd.concat([df_transfers, df_account_credits], ignore_index=True)

    df_transfers = pd.concat([df_transfers, df_checking_debits], ignore_index=True)

    # Verify debits offset credits
    assert df_transfers["Amount"].sum() == CURRENCY_ZERO

    return df_transfers


def _generate_savings_trasfers(checking_name: str, account_dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    # Add monthly transfer from checking to savings account

    df_savings_credits = pd.DataFrame()
    df_checking_debits = pd.DataFrame()

    df_checking = account_dfs[checking_name]
    df_account_periods = df_checking.loc[df_checking["Split"] != "Y", ["Date", "Amount"]]  # new dataframe with only Date and Amount columns
    df_account_periods["Month"] = pd.to_datetime(df_account_periods["Date"]).dt.to_period("M")

    monthly_balance = df_account_periods.groupby("Month")["Amount"].sum().reset_index()

    for _, row in monthly_balance.iterrows():
        transfer_amount = _rand_currency_portion(row["Amount"], 0.10, 0.15)  # 10-15% of balance

        # Add transaction to the savings account
        credit_transaction = {
            "Num": None,
            "Date": row["Month"].end_time.date(),
            "Payee": None,
            "Account": SAVINGS_ACCOUNT,
            "Memo": None,
            "Category": f"Transfer From : {checking_name}",
            "Amount": transfer_amount,
        }

        # Add corresponding transaction to checking account
        checking_transaction = {
            "Num": None,
            "Date": row["Month"].end_time.date(),
            "Payee": None,
            "Account": checking_name,
            "Memo": None,
            "Category": f"Transfer To : {SAVINGS_ACCOUNT}",
            "Amount": -transfer_amount,
        }

        df_savings_credits = pd.concat([df_savings_credits, pd.DataFrame([credit_transaction])], ignore_index=True)
        df_checking_debits = pd.concat([df_checking_debits, pd.DataFrame([checking_transaction])], ignore_index=True)

    df_transfers = pd.concat([df_savings_credits, df_checking_debits], ignore_index=True)

    # Verify debits offset credits
    assert df_transfers["Amount"].sum() == CURRENCY_ZERO

    return df_transfers


def _add_grand_total(df_txns: pd.DataFrame) -> pd.DataFrame:
    # Add a row with the grand total of all transactions.

    # Calculate grand total, excluding split transactions.
    grand_total = df_txns.loc[df_txns["Split"] != "Y", "Amount"].sum() if "Split" in df_txns.columns else df_txns["Amount"].sum()

    # Add row with Grand Total (the same format Money uses.)
    grand_total_row = {
        "Num": "Grand Total",
        "Amount": grand_total,
    }

    return pd.concat([df_txns, pd.DataFrame([grand_total_row])], ignore_index=True)


def _write_txns(output: Path, df_txns: pd.DataFrame) -> None:
    # Write transactions to the specified output file.

    df_dates = df_txns[df_txns["Date"].notna()]  # Split transactions have no Date
    date_start, date_end = df_dates["Date"].min(), df_dates["Date"].max()

    # Write currency as a float to avoid Excel marking cells with "Number Stored as Text".
    df_txns = df_txns.copy()  # don't modify original DataFrame
    df_txns["Amount"] = df_txns["Amount"].apply(lambda x: x.nofmt() if pd.notna(x) else np.nan).astype(float)

    with output.open("w") as f:
        f.write("Account transactions\n")
        f.write("All accounts\n")
        f.write(f"{date_start.strftime('%m/%d/%Y')} through {date_end.strftime('%m/%d/%Y')}\n")
        f.write("\n")
        df_txns.to_csv(f, index=False, lineterminator=CRLF)

    click.echo(f"{len(df_txns)} txns written to '{output}'")


######################################################################
# Main


@click.command()
@click.option(
    "--output",
    type=click.Path(
        exists=False,
        file_okay=True,
        dir_okay=False,
        path_type=Path,
        resolve_path=False,
    ),
    default="txns.csv",
    metavar="FILE",
    help="Output file name for the generated transactions.",
)
def cli(output: Path) -> None:
    """Generate test report data files."""
    output = output.absolute()

    # Generate transactions for each account
    account_dfs: dict[str, pd.DataFrame] = {account: _generate_account_txns(account) for account in ACCOUNT_NAMES}

    credit_transfer_dfs = _generate_credit_payoffs(CHECKING_ACCOUNT, account_dfs)
    savings_transfer_dfs = _generate_savings_trasfers(CHECKING_ACCOUNT, account_dfs)

    concat_dfs = [*account_dfs.values(), credit_transfer_dfs, savings_transfer_dfs]

    # Drop columns with all NaN values (as is with transfers that do not have Num, Payee, Memo).
    # https://github.com/pandas-dev/pandas/issues/55928
    concat_dfs = [df.dropna(axis=1, how="all") for df in concat_dfs]

    df_txns = pd.concat(concat_dfs, ignore_index=True)

    # Index will capture transaction order in the data file
    df_txns.index.name = "N"

    # Sort by Date while preserving report order (important for splits), then reset index
    df_txns = df_txns.sort_values(by=["Date", "N"], ignore_index=True)

    # Drop columns with all NaN values (as is with transfers that do not have Num, Payee, Memo)
    # to avoid `concat` warning.
    # https://github.com/pandas-dev/pandas/issues/55928
    df_txns = df_txns.dropna(axis=1, how="all")

    df_txns = _add_grand_total(df_txns)

    # Split column was dropped above if no transactions are split.
    if "Split" in df_txns.columns:
        df_txns.loc[df_txns["Split"] == "Y", "Date"] = pd.NaT  # Split transactions have no Date
        df_txns = df_txns.drop(columns=["Split"], axis=1)

    with pd.option_context("display.max_rows", 20):
        print(df_txns)  # noqa: T201

    # Write transactions to the specified output file
    _write_txns(output, df_txns)


if __name__ == "__main__":
    sys.exit(cli())
