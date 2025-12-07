# Sales Data Cleaning

This repository contains a small data cleaning script for a sales dataset.

- Input (raw): `data/raw/sales_data_raw.csv`
- Output (cleaned): `data/processed/sales_data_clean.csv`

Purpose
-------
Provide a repeatable, documented pipeline to transform raw sales CSV data into a cleaned CSV suitable for analysis. The script standardizes column names, strips extra whitespace, handles missing prices and quantities, removes invalid rows, and computes a `total_sales` column.

Requirements
------------
- Python 3.8+
- pandas, numpy

Install dependencies quickly:

```bash
python3 -m pip install --upgrade pip
python3 -m pip install pandas numpy
```

Usage
-----
Run the cleaning script from the repository root:

```bash
python3 src/data_cleaning.py
```

This reads `data/raw/sales_data_raw.csv` and writes `data/processed/sales_data_clean.csv`.

Notes
-----
- The script contains modular helper functions (load_data, clean_column_names, handle_missing_values, remove_invalid_rows, etc.) to make testing and extension easier.
- Copilot was used to scaffold helper functions; the code was reviewed and adapted for robustness.

Before / After (small example)
------------------------------
Below is a tiny illustrative example showing how a couple of raw rows are transformed by the cleaning pipeline.

Before (raw rows):

```csv
Product Name, Category , Price, Quantity
"  Widget A  ", Electronics, , 2
Gadget B, electronic, -5.00, 1
"Widget A", Electronics, 10.00, 3
```

After (cleaned rows):

```csv
product,category,price,quantity,total_sales
Widget A,Electronics,10.00,2,20.00
Widget A,Electronics,10.00,3,30.00
```

Notes about the example:
- The first raw row had a missing price and extra whitespace in `Product Name`; the pipeline fills the missing price (median) and trims whitespace.
- The second raw row contained a negative price and was removed as invalid.
- Column names are standardized to lowercase with underscores and a `total_sales` column is computed.
