# -*- coding: utf-8 -*-

# =========================================================
# Code objective: DATA CLEANING AND FINAL DATASET CREATION
# =========================================================
# Combine all collated datasets and create a final master dataset at month-year and county level
# To be used for the visualization and model building
# a) Collated data
   # collated_data_path is the folder where the collated housing pollution, weather, area, population datasets are saved

# b) Final data
   # final_data_path is the folder where the final master dataset will be saved
# ==============================================


# -----------------------------------
# Importing libraries
# -----------------------------------
import pandas as pd 

# -----------------------------------
# Importing data
# -----------------------------------
# Folder path for collated datasets
collated_data_path = r'C:\Users\sneha\Desktop\MISM 6212 Data Mining\Final project\Submission\Collated data'
# Folder path for saving final dataset
final_data_path = r'C:\Users\sneha\Desktop\MISM 6212 Data Mining\Final project\Submission\Final dataset'


# Load the datasets
df_population= pd.read_csv(collated_data_path + r'\Population_data.csv')
df_area= pd.read_csv(collated_data_path + r'\Area_data.csv')
df_weather= pd.read_csv(collated_data_path + r'\Weather_data.csv')
df_housing=pd.read_csv(collated_data_path + r'\Housing_data.csv')

df_CO = pd.read_csv(collated_data_path + r'\COdata.csv')
df_NO2 = pd.read_csv(collated_data_path + r'\NO2data.csv')
df_Oz = pd.read_csv(collated_data_path + r'\Ozdata.csv')
df_PM2 =pd.read_csv(collated_data_path + r'\PM2_5data.csv')
df_PM10= pd.read_csv(collated_data_path + r'\PM10data.csv')
df_SO2= pd.read_csv(collated_data_path + r'\SO2data.csv')


# -------------------------------------
# Steps in Merging pollutant data
# Merge all individual pollutant datasets using outer join method on the following columns:
# "Date","Site ID","Site Name","CBSA_CODE","CBSA_NAME","STATE_CODE","STATE","COUNTY_CODE","COUNTY","SITE_LATITUDE","SITE_LONGITUDE","Year"
# a) Drop duplicate columns:'Source','POC','DAILY_OBS_COUNT','PERCENT_COMPLETE','AQS_PARAMETER_CODE','AQS_PARAMETER_DESC'
col_list=['Source','POC','DAILY_OBS_COUNT','PERCENT_COMPLETE','AQS_PARAMETER_CODE','AQS_PARAMETER_DESC']
# b) Rename the columns
# -------------------------------------

# 1. Merge df_NO2 & df_CO
df1 = df_NO2.merge(df_CO, how="outer", on=["Date","Site ID","Site Name","CBSA_CODE","CBSA_NAME","STATE_CODE","STATE","COUNTY_CODE","COUNTY","SITE_LATITUDE","SITE_LONGITUDE","Year"])
# Drop duplicate columns 
col_list_1 = ['DAILY_OBS_COUNT_x','PERCENT_COMPLETE_x','AQS_PARAMETER_CODE_x','AQS_PARAMETER_DESC_x','DAILY_OBS_COUNT_y','PERCENT_COMPLETE_y','AQS_PARAMETER_CODE_y','AQS_PARAMETER_DESC_y','Source_x','Source_y','POC_x','POC_y']
df1 = df1.drop(columns=col_list_1)
df1 = df1.rename(columns={"DAILY_AQI_VALUE_x": "DAILY_AQI_VALUE_NO2", "DAILY_AQI_VALUE_y": "DAILY_AQI_VALUE_CO","UNITS_x":"UNITS_NO2","UNITS_y":"UNITS_CO"})

# 2. Merge df1 & df_Oz
df2 = df1.merge(df_Oz, how="outer", on=["Date","Site ID","Site Name","CBSA_CODE","CBSA_NAME","STATE_CODE","STATE","COUNTY_CODE","COUNTY","SITE_LATITUDE","SITE_LONGITUDE","Year"])
df2 = df2.drop(columns=col_list)
df2 = df2.rename(columns={"UNITS": "UNITS_Oz", "DAILY_AQI_VALUE": "DAILY_AQI_VALUE_Oz"})

# 3. Merge df2 & df_PM2 using outer join
df3 = df2.merge(df_PM2, how="outer", on=["Date","Site ID","Site Name","CBSA_CODE","CBSA_NAME","STATE_CODE","STATE","COUNTY_CODE","COUNTY","SITE_LATITUDE","SITE_LONGITUDE","Year"])
df3 = df3.drop(columns=col_list)
df3 = df3.rename(columns={"UNITS": "UNITS_PM2", "DAILY_AQI_VALUE": "DAILY_AQI_VALUE_PM2"})

# 4. Merge df3 & df_PM10 using outer join
df4 = df3.merge(df_PM10, how="outer", on=["Date","Site ID","Site Name","CBSA_CODE","CBSA_NAME","STATE_CODE","STATE","COUNTY_CODE","COUNTY","SITE_LATITUDE","SITE_LONGITUDE","Year"])
df4 = df4.drop(columns=col_list)
df4 = df4.rename(columns={"UNITS": "UNITS_PM10", "DAILY_AQI_VALUE": "DAILY_AQI_VALUE_PM10"})

# 5. Merge df4 & df_SO2 using outer join
df5 = df4.merge(df_SO2, how="outer", on=["Date","Site ID","Site Name","CBSA_CODE","CBSA_NAME","STATE_CODE","STATE","COUNTY_CODE","COUNTY","SITE_LATITUDE","SITE_LONGITUDE","Year"])
df5 = df5.drop(columns=col_list)
df5 = df5.rename(columns={"UNITS": "UNITS_SO2", "DAILY_AQI_VALUE": "DAILY_AQI_VALUE_SO2","Year": "YEAR"})

# Arrange the order of columns
df5 = df5[["Date", "Site ID", "Site Name","CBSA_CODE","CBSA_NAME","STATE_CODE","STATE","COUNTY_CODE","COUNTY","SITE_LATITUDE","SITE_LONGITUDE","YEAR","Daily Max 1-hour NO2 Concentration","UNITS_NO2","DAILY_AQI_VALUE_NO2","Daily Max 8-hour CO Concentration","UNITS_CO","DAILY_AQI_VALUE_CO","Daily Max 8-hour Ozone Concentration","UNITS_Oz","DAILY_AQI_VALUE_Oz","Daily Mean PM2.5 Concentration","UNITS_PM2","DAILY_AQI_VALUE_PM2","Daily Mean PM10 Concentration","UNITS_PM10","DAILY_AQI_VALUE_PM10","Daily Max 1-hour SO2 Concentration","UNITS_SO2","DAILY_AQI_VALUE_SO2"]]

# -------------------------------------
# Data type manipulations
# -------------------------------------
# Convert the date column to datetime format
df5['Date'] = df5['Date'].astype('datetime64[ns]')
# Create Month column from Date
df5['MONTH'] = pd.DatetimeIndex(df5['Date']).month_name()
# Create Month_year column from Date
df5['MONTH_YEAR'] = pd.to_datetime(df5['Date']).dt.to_period('M').astype(str)

# -------------------------------------
# Merge the df5 data with weather data
# -------------------------------------
# Convert the date column data type( object) to datetime64[ns]
df_weather['DATE'] = df_weather['DATE'].astype('datetime64[ns]')
df5_weather = df5.merge(df_weather, how="inner", left_on=["Date","COUNTY"],right_on=["DATE","COUNTY"])

# Dropping out PM 10 pollutant concentration column due to poor coverage (945 of the column is nulls due to poor coverage of PM 10 information from EPA website.)
# Aggregate the data using county and month year columns and find mean of each pollutants concentration and weather metrics.
df5_weather_agg = df5_weather.groupby(['MONTH_YEAR','YEAR','COUNTY']).agg({'Daily Max 1-hour NO2 Concentration': 'mean','Daily Max 8-hour CO Concentration':'mean','Daily Max 8-hour Ozone Concentration':'mean','Daily Mean PM2.5 Concentration':'mean','Daily Max 1-hour SO2 Concentration':'mean','TAVG':'mean','AWND':'mean','PRCP':'mean'}).reset_index()

# -------------------------------------
# Merge the df5_weather_agg data with population data
# -------------------------------------
df5_weather_population = df5_weather_agg.merge(df_population, how="inner", on=["YEAR","COUNTY"])

# -------------------------------------
# Merge the df5_weather_population data with area data
# -------------------------------------
df5_weather_population_area = df5_weather_population.merge(df_area, how="inner", on=["COUNTY"])

# -------------------------------------
# Select required rows and columns from df_hopusing and rename to standard format
# -------------------------------------

# Filter the key metrics column and have just 'TY','County','Year' columns in the dataframe
df_housing = df_housing[df_housing['Key_Metrics']=="Median Sales Price*"][['TY','County','Year']]
#rename the columns to standard naming format
df_housing = df_housing.rename(columns={"TY": "Median Price", "Year": "MONTH_YEAR","County":"COUNTY"})

# -------------------------------------
# Merge the df5_weather_population_area data with housing data
# -------------------------------------
df5_weather_population_area_housing = df5_weather_population_area.merge(df_housing, how="inner", on =["MONTH_YEAR","COUNTY"])
df_final = df5_weather_population_area_housing

# -------------------------------------
# Cleaning operations on the final dataset
# -------------------------------------
# Replace the $ in Median Price column
df_final['Median Price'] = df_final['Median Price'].str.replace('$','')
# Replace the comma in Median Price column
df_final['Median Price'] = df_final['Median Price'].str.replace(',','')
# Convert the object data type to int type for MONTH YEAR column
df_final['Median Price']=df_final['Median Price'].astype(int)
df_final['MONTH']= pd.DatetimeIndex(df_final['MONTH_YEAR']).month_name()

df_final = df_final.rename(columns={"Daily Max 1-hour NO2 Concentration": "NO2 Concentration","Daily Max 8-hour CO Concentration": "CO Concentration","Daily Max 8-hour Ozone Concentration": "Ozone Concentration","Daily Mean PM2.5 Concentration": "PM2.5 Concentration","Daily Max 1-hour SO2 Concentration": "SO2 Concentration"})
# Create new column for month from MONTH_YEAR column


# Picking only 5 counties of interest for the analysis
df_final_counties = df_final[df_final['COUNTY'].isin(['Essex','Hampden','Hampshire','Suffolk','Worcester'])].reset_index().drop(columns = 'index')

# ------------------------------------------------------------------
# EXPORTING final dataset to be used for visualization and modeling
# ------------------------------------------------------------------
df_final_counties.to_csv( final_data_path + r'\Final_dataset.csv')

# ==================================== X END OF SCRIPT X ====================================