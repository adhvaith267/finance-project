import os
import pandas as pd
import numpy as np
from multiprocessing import Pool, cpu_count

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

# Function to extract financial data from Excel
def extract_financial_data(file_path, company_name):
    data = {metric: np.nan for metric in metrics}
    data["Stock Ticker"] = company_name
    
    try:
        excel_data = pd.ExcelFile(file_path)
        print(f"Processing file: {file_path}")
        
        for sheet in excel_data.sheet_names:
            df_sheet = excel_data.parse(sheet)
            
            if 'Sales' in df_sheet.columns:
                data['Sales'] = df_sheet['Sales'].iloc[0]
                
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
                
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
    
    return data

# Function to process a single company's files
def process_company(company_name):
    company_path = os.path.join(root_path, company_name)
    company_data = []
    
    if os.path.isdir(company_path):
        for file in os.listdir(company_path):
            if file.endswith(".xlsx"):
                file_path = os.path.join(company_path, file)
                year = "Unknown"  # Extract year if available
                data = extract_financial_data(file_path, company_name)
                data["Year"] = year
                company_data.append(data)
    
    return company_data

# Multiprocessing for concurrent processing
def process_all_companies():
    companies = [company for company in os.listdir(root_path) if os.path.isdir(os.path.join(root_path, company))]
    
    # Utilize multiprocessing to process companies in parallel
    with Pool(cpu_count()) as pool:
        all_data = pool.map(process_company, companies)
    
    # Flatten the list of lists
    all_data_flat = [item for sublist in all_data for item in sublist]
    
    # Convert to DataFrame
    df = pd.DataFrame(all_data_flat, columns=metrics)
    
    # Save the extracted data to a CSV file
    df.to_csv("financial_data_optimized.csv", index=False)
    print("Data extraction complete. Results saved to financial_data_optimized.csv.")

# Run the optimized extraction
process_all_companies()
