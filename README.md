# Banks_ETL_Project

This project is part of Coursera IBM Data Engineering course.
It performs a full ETL (Extract, Transform, Load) process:

- **Extracts** bank data from a Wikipedia archive (Web scraping)
- **Transforms** USD market caps to GBP, EUR, and INR using a currency rates CSV
- **Loads** the results into a local SQLite database and a CSV file
- Executes example SQL queries on the loaded data

## Technologies used:
- Python (requests, BeautifulSoup, pandas, numpy, sqlite3)
- SQL

## Run code:
```bash
python etl_banks.py
