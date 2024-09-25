import os
import pandas as pd
import numpy as np

# Define the root directory for 10-K files
root_path = "C:\\Users\\adhva\\PycharmProjects\\finance\\.venv\\Output"

# Define the metrics to be extracted
metrics = [
    "Stock Ticker", "Year", "Sales", "Research and Development Expenses",
    "Profit Before Tax (EBIT)", "Corporate Tax (Provision)", "Total Assets",
    "Plant, Property, and Equipment (PPE)", "Intangible Assets", "Goodwill",
    "Inventories", "Officer's Compensation", "Tax Haven Subsidiaries",
    "Auditor Fees", "Foreign Income"
]

# Initialize a DataFrame to store the extracted data
df = pd.DataFrame(columns=metrics)

def extract_financial_data(file_path, company_name):
    # Initialize data dictionary with NaN for all metrics
    data = {metric: np.nan for metric in metrics}
    data["Stock Ticker"] = company_name
    
    try:
        excel_data = pd.ExcelFile(file_path)
        print(f"Processing file: {file_path}")
        
        # Iterate through each sheet in the Excel file
        for sheet in excel_data.sheet_names:
            df_sheet = excel_data.parse(sheet)
            print(f"Processing sheet: {sheet}")
            
            # Example logic to handle a specific sheet structure
            if sheet == 'Document and Entity Information':
                # Adjust these column names based on your actual sheet structure
                if 'Entity Registrant Name' in df_sheet.columns:
                    ticker_row = df_sheet[df_sheet['Entity Registrant Name'] == company_name]
                    if not ticker_row.empty:
                        data['Stock Ticker'] = ticker_row.iloc[0].get('Trading Symbol', np.nan)
                if 'Document Period End Date' in df_sheet.columns:
                    year_row = df_sheet[df_sheet['Document Period End Date'] == 'Dec. 31, 2023']
                    if not year_row.empty:
                        data['Year'] = year_row.iloc[0].get('Document Fiscal Year Focus', np.nan)
                
            if sheet == 'Financial Statements':
                # Example logic to extract financial metrics
                for metric in metrics[2:]:
                    if metric in df_sheet.columns:
                        metric_row = df_sheet[df_sheet['Metric'] == metric]
                        if not metric_row.empty:
                            data[metric] = metric_row.iloc[0].get('Value', np.nan)
                            
            print(f"Extracted data: {data}")

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
    
    return data

# Loop through each company's folder and process the Excel files
for company_name in os.listdir(root_path):
    company_path = os.path.join(root_path, company_name)
    if os.path.isdir(company_path):
        print(f"\nProcessing company: {company_name}")
        # Loop through each file in the company's folder
        for file in os.listdir(company_path):
            if file.endswith(".xlsx"):
                file_path = os.path.join(company_path, file)
                # Extract year from the file name or elsewhere if needed
                year = "Unknown"  # Adjust this based on your file naming convention
                # Extract financial data from the Excel file
                data = extract_financial_data(file_path, company_name)
                data["Year"] = year
                # Append the data to the DataFrame
                df = pd.concat([df, pd.DataFrame([data], columns=metrics)], ignore_index=True)

# Save the extracted data to a CSV file
df.to_csv("financial_data.csv", index=False)
print("Data extraction complete. Results saved to financial_data.csv.")
