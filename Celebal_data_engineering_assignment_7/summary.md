# Project Summary: Pandas Data Exploration & Cleaning

**Dataset used:** A locally generated sample dataset (`superstore_sample.csv`, 310 rows, 15 columns)
built to mirror the structure of the [Kaggle Superstore dataset](https://www.kaggle.com/datasets/vivek468/superstore-dataset-final)
(Order ID, Category, Region, Price, Quantity, etc.). Kaggle requires an authenticated account/API key to
download files, which isn't available in this environment, so a representative stand-in was generated
with the same kinds of issues (missing values, duplicate rows) you'd find in the real file. The notebook
works unmodified on the real Kaggle CSV — just swap the filename in Step 1.

## What was done
1. **Loaded** the CSV into a Pandas DataFrame with `pd.read_csv()`.
2. **Explored** it with `head()`, `tail()`, `shape`, `columns`, `dtypes`, `info()`, and `describe()`.
3. **Handled missing values**: found gaps in `Price`, `Quantity`, `Region`, and `Ship Mode` (36 cells
   total); filled numeric columns with the median, categorical columns with the mode, and demonstrated
   `dropna()` on a critical identifier column.
4. **Filtered rows and selected columns**: e.g. Technology orders over $100, bulk orders (quantity ≥ 5),
   and column subsets like `Order ID`, `Category`, `Price`, `Quantity`.
5. **Removed duplicates**: found and dropped 10 fully duplicated rows.
6. **Created a derived column**: `total_amount = Price * Quantity`.
7. **Saved** the cleaned result to `superstore_cleaned.csv`.

## Result
- Final cleaned dataset: **300 rows × 16 columns**
- **0** missing values, **0** duplicate rows remaining
- New `total_amount` column added for revenue-style analysis

## Files delivered
- `data_cleaning_pandas.ipynb` – the full Jupyter Notebook (code + explanations + outputs)
- `superstore_sample.csv` – the raw input data used
- `superstore_cleaned.csv` – the cleaned output data
