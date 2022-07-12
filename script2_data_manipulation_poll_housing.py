# -*- coding: utf-8 -*-

# ==============================================
# Code objective: DATA EXTRACTION AND COLLATION
# ==============================================
# a) Housing data
    # housing_downloads_path should have the county housing pdf files for each month between 2016-2021
    # Data source: https://www.marealtor.com/market-data/

# b) Pollutant data
    # pollutant_downloads_path should have the csv files downloaded from EPA website for each year between 2016-2021
    # Data source: https://www.epa.gov/outdoor-air-quality-data/download-daily-data

# c) Collated data
    # collated_data_path is the folder where the collated housing and pollution datasets will be saved
# ==============================================


# -----------------------------------
# Importing libraries
# -----------------------------------
import pandas as pd
import os, os.path
import re
import PyPDF2
import tabula

# -----------------------------------------------------------------
# Function to extract count names from each page of the pdf files
# -----------------------------------------------------------------
def extract_county(pdfFileName):
    pdfObj = open(pdfFileName, 'rb')
    pdfReader = PyPDF2.PdfFileReader(pdfObj)
    numbPages = pdfReader.numPages
    monthly_county_list = []
    monthly_county_df = pd.DataFrame()
    for i in range(0,numbPages):
        pageText = pdfReader.getPage(i).extractText()
        username_pattern = '([a-zA-Z]+) County'
        # reqText = pageText.split("County")[0]
        reqText = re.findall(username_pattern, pageText)[-1]
        dfCountySingle = extract_table(pdfFileName,i)
        countyName = reqText
        dfCountySingle['County'] = countyName
        monthly_county_list.append(countyName)
        monthly_county_df = monthly_county_df.append(dfCountySingle) 
    pdfObj.close()
    return monthly_county_df

# ------------------------------------------------------------------------------
# Function to extract the relevant table having Median Sales price from pdf files
# ------------------------------------------------------------------------------
def extract_table(pdfFileName, numbPage):
    df = tabula.read_pdf(pdfFileName, pages=numbPage+1, stream = True)
    dfSingleFamily = df[0]
    # new_header = dfSingleFamily.iloc[0]
    dfSingleFamily = dfSingleFamily[1:]
    dfSingleFamily.columns = ['Key_Metrics','LY','TY','Pct_Diff','YTD_LY','YTD_TY','YTD_Pct_Diff']
    return dfSingleFamily

# -------------------------------------------------------------------------------------------------------------
# Function to collate pollutant data for all years and adding a YEAR column by extracting it from the file name
# -------------------------------------------------------------------------------------------------------------
def extract_pollutantdata(data_path):
    dirListing = os.listdir(data_path)
    SO2PollutantData = pd.DataFrame()
    PM2_5PollutantData = pd.DataFrame()
    PM10PollutantData = pd.DataFrame()
    NO2PollutantData = pd.DataFrame()
    COPollutantData = pd.DataFrame()
    OzPollutantData  = pd.DataFrame()
    for i in range(0,len(dirListing)):
        df = pd.read_csv(data_path+'\\'+dirListing[i])
        df['Year'] = dirListing[i][-11:-7]
        if 'SO2' in dirListing[i]:
            SO2PollutantData = SO2PollutantData.append(df) 
        elif 'PM2_5' in dirListing[i]:
            PM2_5PollutantData = PM2_5PollutantData.append(df)
        elif 'PM10' in dirListing[i]:
            PM10PollutantData = PM10PollutantData.append(df)
        elif 'NO2' in dirListing[i]:
            NO2PollutantData = NO2PollutantData.append(df)
        elif 'CO' in dirListing[i]:
            COPollutantData = COPollutantData.append(df)
        else:
            OzPollutantData = OzPollutantData.append(df) 
    return SO2PollutantData,PM2_5PollutantData,PM10PollutantData,NO2PollutantData,COPollutantData,OzPollutantData
    

def main():
    # Provide folder path for the downloaded housing and pollutant data files
    housing_downloads_path = r'C:\Users\sneha\Desktop\MISM 6212 Data Mining\Final project\Submission\Downloaded data\Housing data'   
    pollutant_downloads_path = r'C:\Users\sneha\Desktop\MISM 6212 Data Mining\Final project\Submission\Downloaded data\Pollutant data' 
    # housing_downloads_path should have the county housing pdf files
    # pollutant_downloads_path should have the csv files downloaded from EPA website for each year between 2016-2021
    # List of pollutants: CO, Ozone, PM2.5, PM10, NO2, SO2
    
    # Provide folder path for saving the collated datasets
    collated_data_path = r'C:\Users\sneha\Desktop\MISM 6212 Data Mining\Final project\Submission\Collated data'
    
    pdfDirListing = os.listdir(housing_downloads_path)
    SingleFamilyData = pd.DataFrame()
    for i in range(0,len(pdfDirListing)):
        filename = housing_downloads_path+'\\'+pdfDirListing[i]
        df = extract_county(filename)
        df['Year'] = pdfDirListing[i][:7]
        SingleFamilyData = SingleFamilyData.append(df)
    SingleFamilyData.to_csv(collated_data_path + r'\Housing_data.csv', index=False)
    
    SO2Data, PM2_5Data, PM10Data, NO2Data, COData, OzData = extract_pollutantdata(pollutant_downloads_path)
    
    SO2Data.to_csv(collated_data_path   + r'\SO2data.csv', index=False)
    PM2_5Data.to_csv(collated_data_path + r'\PM2_5data.csv', index=False)
    PM10Data.to_csv(collated_data_path  + r'\PM10data.csv', index=False)
    NO2Data.to_csv(collated_data_path   + r'\NO2data.csv', index=False)
    COData.to_csv(collated_data_path    + r'\COdata.csv',index=False)
    OzData.to_csv(collated_data_path    + r'\Ozdata.csv', index=False)
    
    
if __name__ == "__main__":
    main()   
    
# ==================================== X END OF SCRIPT X ====================================
