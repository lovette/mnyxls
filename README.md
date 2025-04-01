# mnyxls

Create a Microsoft Excel workbook and/or a SQLite database from Microsoft Money reports.


## Usage

Running:

    cd mnyxls
    [activate virtual environment]
    (mnyxls) % mnyxls -vv "Account transactions.csv"

Will create:

- "Account transactions.sqlite3"
- "Account transactions.xlsx"


## Microsoft Money Reports

`mnyxls` can import various Microsoft Money reports.
Each report imported adds details to create a more complete snapshot of your Money data.
Run each report and use "Export to Microsoft Excel" and Money will save it in comma-delimited format.
(Note that Microsoft Money does not provide a way to report transactions for all accounts such as [loan accounts](#loan-terms).)

Technically, `mnyxls` is compatible with any comma-delimited data files that are formatted correctly.
There is nothing special about the files Microsoft Money creates.

### Account transactions

Provides transactions for non-investment accounts.

Choose Customize Report options:

- Fields: Account, Category (required)
- Show splits (optional)
- No subtotals
- Running balance (ignored)

### Account balances

Provides account classification (Asset, Liability) and category (Bank, Credit Card, Investment, etc.)

Note the report will not include closed accounts whose last transaction is prior to the report date.
(I have experienced reports including accounts that were opened after the report date.)

### Account balances with details

Provides bank details such as bank name, account number, account limit, opening balance.

Balances are shown as of the date the report is run, it cannot be customized.
Insert the line `As of MM/DD/YYYY` to the top of the saved report file to capture the balance as-of date.
The report file modification time is used by default.

### Income and spending

Choose Customize Report options:

- Rows: Categories (required)
- Columns: Years (required)

Provides:

- Category types (Income, Expenses)
- Transfers categorized as expense?
- Annual category totals that can be used to validate import.

Does not include some transfers. Mortgages and loans are included but liabilities are not.
Note that mortgage principal is shown as a transfer and interest is shown as an expense.

### Monthly income and expenses

Similar to "Income and spending" but includes transfers between more asset and liability accounts.

Choose Customize Report options:

- Rows: Categories (required)
- Columns: Years (required)

### Investment transactions

Provides investment account transactions.

Choose Customize Report options:

- Fields: Account (required)
- Group by: None

### Loan terms

Provides loan details such as loan amount, interest rate and balance.

Balances are shown as of the date the report is run, it cannot be customized.
Insert the line "As of MM/DD/YYYY" to the top of the saved report file to capture the balance as-of date.
Otherwise, the report file modification time is used.

Microsoft Money does not provide a way to report transactions for loan accounts.
Transfers to/from the loan will be included as transactions in other accounts,
but non-transfer transactions are not exportable in any way. 😒


## Configuration file

Configuration options can be read from a configuration file.
The default file is `mnyxls.yaml` in the working directory.
Use the `-c/--config-file` option to read options from a different file.
The configuration file is a [YAML](https://www.kerno.io/learn/yaml-file-format-complete-guide) formatted set of key/value pairs.

Here are a few of the most common options defined in the configuration file.

| Option | Type | Description |
| --- | --- | ---
| `reports` | List | List of Money comma-delimited report paths. |
| `workbook` | Key/values | Define the worksheets created in the Excel workbook. |

See the `mnyxls.sample.yaml` for descriptions of options.


## Excel Workbook

The primary purpose of `mnyxls` is to create an Excel workbook with worksheets containing
transaction details.

The workbook is defined by the configuration file directive `workbook`.

The `workbook` can be defined in the main configuration file or a separate configuration file.
The workbook configuration file `mnyxls_workbook.yaml` is used if it exists in the data directory
or the path can be set with the `--xls-config` command line option.

### Worksheets

Workbooks can contain one or more worksheets.
Worksheets are defined by the `workbook.worksheets` configuration directive.
The name of the sheet is the worksheet "key".
The options for each worksheet are defined with key/value pairs.

  ```yaml
  workbook:
    worksheets:
      "Expenses":
        sheet_type: "txns"
        select:
          txntype: "Expense"
        use_existing: True
  ```

### Sheet types

The `sheet_type` directive determines the data each worksheet contains.

| Type | Description |
| --- | --- |
| `txns` | List transactions. |
| `txns:pivot` | Transactions pivot table. |
| `accounts` | List accounts. |
| `categories` | List categories. |
| `categories:naked` | List categories that have subcategories but have transactions that do not reference any subcategories. |
| `categories:singlepayee` | List categories that are referenced by a single payee |

### Select/filter transactions

The `select` directive is a set of key/value pairs that filter transactions included in the sheet.

  ```yaml
  workbook:
    worksheets:
      "Credit Cards":
        sheet_type: "txns"
        select:
          txntype: "Expense"
          account_category:
            - "Credit Cards"
            - "Bank and Cash Accounts"
  ```

Each option can be set to a single value or a list of values.
The first entry can start with `!` to negate the condition.
A `select` directive can be defined for the workbook and each worksheet.
A workbook select will be applied to all worksheets and any worksheet select
criteria will be merged with and override the workbook select.
Some options do not apply to workbook selects.

| Option | Description |
| --- | --- |
| `account` | Select transactions for named accounts. |
| `account_category` | Select transactions for accounts by category. |
| `account_classification` | Select transactions for accounts by classification. |
| `category` | Select transactions with given category and subcategory; `"category"` will select a category and its subcategories; `"category:subcategory"` will select specific subcategories; `"category:"` will select only the "root" category (whether it has subcategories or not.) [Worksheet only]  |
| `payee` | Select transactions for a payee name. [Worksheet only] |
| `txnclass` | Select transactions by class. [Worksheet only] |
| `txntype` | Select transactions by type. [Worksheet only] |
| `date_from` | Select transactions after this date (inclusive); accepts formats `YYYY-MM-DD`, `YYYY-MM`, `YYYY`. `MM` defaults to January. `DD` defaults to the first of the month. |
| `date_to` | Select transactions before this date (inclusive); accepts formats `YYYY-MM-DD`, `YYYY-MM`, `YYYY`. `MM` defaults to December. `DD` defaults to the last day of the month. |
| `yyyy` | Select transactions in given year(s). |

### For each

The `foreach` directive creates multiple sheets based on a single criteria.

  ```yaml
  workbook:
    worksheets:
      "Expenses {{foreach}}":
        sheet_type: "txns"
        foreach: "yyyy"
        select:
          txntype: "Expense"
  ```

#### Options

| Option | Description | Applies to sheet |
| --- | --- | --- |
| `account` | A worksheet for each account. | `txns`, `txns:pivot` |
| `account_category` | A worksheet for each account category. | `txns`, `txns:pivot`, `accounts` |
| `account_classification` | A worksheet for each account classification. | `txns`, `txns:pivot`, `accounts` |
| `txnclass` | A worksheet for each transaction class. | `txns`, `txns:pivot` |
| `txntype` | A worksheet for each transaction type. | `txns`, `txns:pivot` |
| `yyyy` | A worksheet for each year. | `txns`, `txns:pivot` |
| `10y` | A worksheet for each decade. | `txns`, `txns:pivot` |

The value for each criteria can be included in the sheet name with `"{{foreach}}"`.

### Pivot options

The `txns:pivot` sheet type creates a pivot table that groups and summarizes transaction amounts.
The group for each dimension is set with the `rows` and `columns` options.
Each dimension can have multiple levels.

  ```yaml
  workbook:
    worksheets:
      "Txn types by category":
        sheet_type: "txns:pivot"
        options:
          rows:
            - "category"
            - "subcategory"
          columns:
            - "txntype"
  ```

Default `rows` is `category`, `subcategory`.
Default `columns` is `yyyy`.

#### `rows`

| Dimension |
| --- |
| `account` |
| `account_category` |
| `account_classification` |
| `payee` |
| `txnclass` |
| `txntype` |
| `category` |
| `subcategory` (implies `category`) |
| `category_subcategory` |

#### `columns`

| Dimension |
| --- |
| `account_category` |
| `account_classification` |
| `txnclass` |
| `txntype` |
| `category` |
| `total` |
| `yyyy` |
| `yyyymm` |

Use column value `total` to show only the total for each row.

### Worksheet options

#### General options

| Option | Description |
| --- | --- |
| `autofit` | Fit columns to their content. [Default: Workbook setting] |
| `use_existing` | Replace sheet with same name if it exists in the template file. A sheet can only be replaced once. [Default: No] |
| `skipempty` | Do not create sheet if it is empty. [Default: Yes] |

#### Txns options

| Option | Description |
| --- | --- |
| `columns` | Include or exclude columns from the worksheet. |
| `consolidate` | Consolidate transactions to reduce number of sheet transactions. Valid options are: `yyyymm`. |

### Workbook options

| Option | Description |
| --- | --- |
| `autofit` | Fit columns to their content. [Default: Yes] |

### Workbook template

The workbook created can be based on an existing Excel workbook.
This allows you to set formats, styles and base new sheets on existing sheets.
The template file `mnyxls_template.xlsx` is used if it exists in the data directory.
This path can be set with the `--xls-template` command line option or `xls_template` configuration file option.
The order of existing column headers will be preserved. Additional columns will be added to the end.
