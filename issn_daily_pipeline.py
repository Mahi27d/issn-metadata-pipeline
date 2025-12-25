name: ISSN Daily Metadata Update

on:
  schedule:
    - cron: "0 2 * * *"   # Daily 2 AM UTC
  workflow_dispatch:

jobs:
  run-etl:
    runs-on: ubuntu-latest

    steps:
    - name: Checkout repo
      uses: actions/checkout@v4

    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: "3.10"

    - name: Install dependencies
      run: pip install -r requirements.txt

    - name: Run ISSN ETL
      env:
        DB_HOST: ${{ secrets.DB_HOST }}
        DB_PORT: ${{ secrets.DB_PORT }}
        DB_NAME: ${{ secrets.DB_NAME }}
        DB_USER: ${{ secrets.DB_USER }}
        DB_PASSWORD: ${{ secrets.DB_PASSWORD }}
      run: python issn_daily_pipeline.py
