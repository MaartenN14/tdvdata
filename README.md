# tdvdata

Export TDV database data to Parquet files.

## Prerequisites

- An ODBC DSN named **TDV PRD** must be configured and accessible.

## Setup

```
conda env create -f environment.yml
conda activate tdvdata
```

## Usage

```
python export_to_parquet.py
```

## Configuration

Edit the constants at the top of `export_to_parquet.py`:

- `REPORT_DATE` — reporting date in `yyyymmdd` format (e.g. `20231231`)
- `CLIENT_ID` — TDV client identifier (e.g. `"1001"`)
