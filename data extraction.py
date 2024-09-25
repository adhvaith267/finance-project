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
    """
    Extract specific financial metrics from an Excel file.
    """
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
            
            # Adjust these column names and logic based on your actual sheet structure
            # Extracting Sales data as an example
            if 'Sales' in df_sheet.columns:
                data['Sales'] = df_sheet['Sales'].iloc[0]  # Assuming first row contains the sales figure
            
            # Extracting other financial metrics
            if 'Research and Development Expenses' in df_sheet.columns:
                data['Research and Development Expenses'] = df_sheet['Research and Development Expenses'].iloc[0]
                
            if 'Profit Before Tax (EBIT)' in df_sheet.columns:
                data['Profit Before Tax (EBIT)'] = df_sheet['Profit Before Tax (EBIT)'].iloc[0]
                
            if 'Corporate Tax (Provision)' in df_sheet.columns:
                data['Corporate Tax (Provision)'] = df_sheet['Corporate Tax (Provision)'].iloc[0]
            
            if 'Total Assets' in df_sheet.columns:
                data['Total Assets'] = df_sheet['Total Assets'].iloc[0]
                
            if 'Plant, Property, and Equipment (PPE)' in df_sheet.columns:
                data['Plant, Property, and Equipment (PPE)'] = df_sheet['Plant, Property, and Equipment (PPE)'].iloc[0]
                
            if 'Intangible Assets' in df_sheet.columns:
                data['Intangible Assets'] = df_sheet['Intangible Assets'].iloc[0]
                
            if 'Goodwill' in df_sheet.columns:
                data['Goodwill'] = df_sheet['Goodwill'].iloc[0]
                
            if 'Inventories' in df_sheet.columns:
                data['Inventories'] = df_sheet['Inventories'].iloc[0]
                
            if 'Officer\'s Compensation' in df_sheet.columns:
                data['Officer\'s Compensation'] = df_sheet['Officer\'s Compensation'].iloc[0]
                
            if 'Tax Haven Subsidiaries' in df_sheet.columns:
                data['Tax Haven Subsidiaries'] = df_sheet['Tax Haven Subsidiaries'].iloc[0]
                
            if 'Auditor Fees' in df_sheet.columns:
                data['Auditor Fees'] = df_sheet['Auditor Fees'].iloc[0]
                
            if 'Foreign Income' in df_sheet.columns:
                data['Foreign Income'] = df_sheet['Foreign Income'].iloc[0]
                
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
