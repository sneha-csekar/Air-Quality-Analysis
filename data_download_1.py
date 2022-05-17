# -*- coding: utf-8 -*-
"""
Created on Mon Mar 21 18:27:45 2022

@author: sneha
"""

import pandas as pd
import os, os.path
import re
import PyPDF2
import tabula

from time import  sleep
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver import ActionChains


def download_housing_data(web_path, folder_path):
    option_set_1 = Options()
    option_set_1.headless = True
    driver_path = r'C:\Users\sneha\Downloads\chromedriver_win32\chromedriver'
    driver = webdriver.Chrome(executable_path = driver_path,options = option_set_1)
    driver.get(web_path)
    year_link_list = []
    year_text_list = []
    for x in driver.find_elements(By.PARTIAL_LINK_TEXT, 'County Data'):
        if x.text!='':
            year_link_list.append(x.get_attribute('href'))
            year_text_list.append(x.text)
    year_text_list = [year_text_list[i].replace(" ","_") for i in range(len(year_text_list))]
    year_link_dict = {year_text_list[i] : year_link_list[i] for i in range(len(year_link_list )) }
    driver.quit()
    
    options_2 = Options()
    options_2.headless = True
    profile = {"download.default_directory":folder_path,"download.prompt_for_download":False, "download.directory_upgrade":True, "plugins.always_open_pdf_externally":True}
    options_2.add_experimental_option('prefs', profile)
    
    for k, v in year_link_dict.items():
        if '2022' not in k:
            id_val = v.split("#")[1]
            driver = webdriver.Chrome(executable_path = driver_path,options=options_2)
            driver.get(v)
            parent = driver.find_element(By.XPATH, '//div[@id="'+id_val+'"]')
            year_month_elements = parent.find_elements(By.XPATH, '//div[@id="'+id_val+'"]//a[@class="link-with-right-arrow" and contains(@href,"Counties")]')
            sleep(10)
            for month in year_month_elements:
                action = ActionChains(driver)
                action.click(month).perform()
                sleep(10)
            driver.quit()

def download_pollutant_data(web_path, folder_path):
    option_set_3 = Options()
    option_set_3.headless = True
    profile = {"download.default_directory":folder_path,"download.prompt_for_download":False, "download.directory_upgrade":True, "plugins.always_open_pdf_externally":True}
    option_set_3.add_experimental_option('prefs', profile)
    
    driver_path = r'C:\Users\sneha\Downloads\chromedriver_win32\chromedriver'
    driver = webdriver.Chrome(executable_path = driver_path, options = option_set_3)
    driver.get(web_path)
    
    select_poll = Select(driver.find_element(By.NAME,'poll'))
    sleep(5)
    pollutant_options = [opt.text for opt in select_poll.options]
    select_poll.select_by_visible_text(pollutant_options[1])
    
    select_year = Select(driver.find_element(By.NAME,'year'))
    sleep(5)
    year_options = [opt.text for opt in select_year.options]
    select_year.select_by_visible_text(year_options[1])
    
    select_state = Select(driver.find_element(By.NAME,'state'))
    sleep(5)
    state_options = [opt.text for opt in select_state.options]
    
    for poll in pollutant_options:
        if poll not in ['Select ...','Pb', 'CO','NO2','Ozone','PM10']:
            select_poll.select_by_visible_text(poll)
            sleep(5)
            for year in year_options:
                if year in ['2021','2020','2019','2018','2017','2016']:
                    select_year.select_by_visible_text(year)
                    sleep(5)
                    select_state.select_by_visible_text('Massachusetts')
                    sleep(5)
                    submit_button = driver.find_element(By.XPATH,'//div[@id="launch"]//input')
                    submit_button.click()
                    sleep(5)
                    download_link = driver.find_element(By.PARTIAL_LINK_TEXT,'Download CSV')
                    download_link.click()
                    sleep(15)
                    old_name = folder_path+'\\ad_viz_plotval_data.csv'
                    new_name = folder_path+'\\'+poll.replace('.','_')+'_'+year+'_MA.csv'
                    os.rename(old_name, new_name)
    driver.quit()
    
    
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

def extract_county_alt(pdfFileName):
    pdfObj = open(pdfFileName, 'rb')
    pdfReader = PyPDF2.PdfFileReader(pdfObj)
    numbPages = pdfReader.numPages
    monthly_county_list = []
    monthly_county_df = pd.DataFrame()
    for i in range(0,numbPages):
        pageText = pdfReader.getPage(i).extractText()
        # reqText = pageText.split("County")[0]
        reqText = pageText.split("County")[1]
        length = len(reqText.strip())
        newString = ''
        for j in range(length,0,-1):
            if(reqText[j]=='—'):
                countyName = newString[::-1]
                break
            else:
                newString = newString + reqText[j]
                
        dfCountySingle = extract_table(pdfFileName,i)
        dfCountySingle['County'] = countyName
        monthly_county_list.append(countyName)
        monthly_county_df = monthly_county_df.append(dfCountySingle) 
    pdfObj.close()
    return monthly_county_df

def extract_table(pdfFileName, numbPage):
    df = tabula.read_pdf(pdfFileName, pages=numbPage+1, stream = True)
    dfSingleFamily = df[0]
    # new_header = dfSingleFamily.iloc[0]
    dfSingleFamily = dfSingleFamily[1:]
    dfSingleFamily.columns = ['Key_Metrics','LY','TY','Pct_Diff','YTD_LY','YTD_TY','YTD_Pct_Diff']
    return dfSingleFamily

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
    path = os.getcwd()
    print(path)
    
    now = datetime.now()
    print("-----------------------\nStarting housing data download...\nCurrent time:", now.strftime("%H:%M:%S"),'\n-----------------------')
    housing_web_path = r'https://www.marealtor.com/market-data/'
    housing_downloads_path = os.path.join(path+r'\Data\Housing Data Downloads')
    download_housing_data(housing_web_path, housing_downloads_path)

    now = datetime.now()
    print("-----------------------\nStarting pollutant data download...\nCurrent time:", now.strftime("%H:%M:%S"),'\n-----------------------')
    pollutant_web_path =r'https://www.epa.gov/outdoor-air-quality-data/download-daily-data'
    pollutant_downloads_path = os.path.join(path+r'\Data\Pollutant Data Downloads')
    download_pollutant_data(pollutant_web_path, pollutant_downloads_path)
    
    now = datetime.now()
    print("-----------------------\nStarting housing data collation...\nCurrent time:", now.strftime("%H:%M:%S"),'\n-----------------------')
    pdfDirListing = os.listdir(housing_downloads_path)
    SingleFamilyData = pd.DataFrame()
    for i in range(0,len(pdfDirListing)):
        filename = housing_downloads_path+'\\'+pdfDirListing[i]
        df = extract_county(filename)
        df['Year'] = pdfDirListing[i][:7]
        SingleFamilyData = SingleFamilyData.append(df)
    SingleFamilyData.to_csv(os.path.join(path+r'\Data\Collated data\Housing_data.csv'), index=False)
    
    now = datetime.now()
    print("-----------------------\nStarting pollutant data collation...\nCurrent time:", now.strftime("%H:%M:%S"),'\n-----------------------')
    SO2Data, PM2_5Data, PM10Data, NO2Data, COData, OzData = extract_pollutantdata(pollutant_downloads_path)
    pollutant_data_path = (os.path.join(path+r'\Data\Collated data'))
    SO2Data.to_csv(pollutant_data_path   + r'\SO2data.csv', index=False)
    PM2_5Data.to_csv(pollutant_data_path + r'\PM2_5data.csv', index=False)
    PM10Data.to_csv(pollutant_data_path  + r'\PM10data.csv', index=False)
    NO2Data.to_csv(pollutant_data_path   + r'\NO2data.csv', index=False)
    COData.to_csv(pollutant_data_path    + r'\COdata.csv',index=False)
    OzData.to_csv(pollutant_data_path    + r'\Ozdata.csv', index=False)
    now = datetime.now()
    print("-----------------------\nCurrent time:", now.strftime("%H:%M:%S"),'\n-----------------------')
    
if __name__ == "__main__":
    main()   
    
