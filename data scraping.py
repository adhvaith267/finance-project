import os
import requests
from csv import reader
from rich.progress import Progress
from edgarpython.secapi import getSubmissionsByCik, getXlsxUrl
from edgarpython.exceptions import InvalidCIK


# Function to handle file downloading
def download_file(download_url, file_path):
    response = requests.get(
        download_url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; Edg/110.0; Win64; x64)"},
    )
    with open(file_path, "wb") as output_file:
        output_file.write(response.content)


# Read S&P 500 company data from CSV
with open("sp500.csv", encoding="utf-8") as csv_file:
    company_data = list(reader(csv_file))[1:]

# Create directory for storing all downloads
output_dir = "FinancialReports"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Iterate over the company list and fetch 10-K filings
with Progress() as progress:
    task = progress.add_task("Processing companies...", total=len(company_data))
    for company in company_data:
        company_name = company[1]
        cik_code = company[6]
        company_dir = os.path.join(output_dir, company_name)

        # Ensure directory for each company exists
        if not os.path.exists(company_dir):
            os.makedirs(company_dir)

        try:
            # Fetch company submission data using CIK
            submissions = getSubmissionsByCik(cik_code)
            ten_k_filings = [sub for sub in submissions if sub.form == "10-K"]
            print(f"{company_name} - {len(ten_k_filings)} 10-K reports found")

            # Download and save reports
            missed_reports = 0
            for idx, report in enumerate(ten_k_filings):
                try:
                    xlsx_url = getXlsxUrl(cik_code, report.accessionNumber)
                    file_name = f"{report.accessionNumber}.xlsx"
                    file_path = os.path.join(company_dir, file_name)

                    download_file(xlsx_url, file_path)
                    print(f"Downloaded {file_name} [{idx + 1}/{len(ten_k_filings)}]")
                except FileNotFoundError:
                    missed_reports += 1
                    print(f"Could not find report for {company_name} (missed {missed_reports})")

            print(f"Completed {company_name}: {len(ten_k_filings) - missed_reports}/{len(ten_k_filings)} reports downloaded.")
        
        except InvalidCIK:
            print(f"Error: Invalid CIK for {company_name}")
        finally:
            progress.update(task, advance=1)
