# -*- coding: utf-8 -*-

# ===============================================
# Code objective: DATA EXTRACTION AND COLLATION
# ===============================================
# a) Area data (Demographic)
    # Data source: https://www.openintro.org/data/?data=county_complete

# b) Population data (Demographic)
    # pop_data_path should have the data files downloaded from Census website for population between 2016-19 and 2020-21
    # Data source(2016-2019): https://www.census.gov/data/tables/time-series/demo/popest/2010s-counties-detail.html
    # Data source(2020-2021): https://www.census.gov/data/datasets/time-series/demo/popest/2020s-counties-total.html

# c) Weather data
    # weather_data_path should have the weather csv files for each county having information from 2016 - 2021
    # Data Source: https://www.ncei.noaa.gov/maps/daily/

# d) Collated data
    # collated_data_path is the folder where the collated housing and pollution datasets will be saved
# ==============================================


# -----------------------------------
# Importing libraries
# -----------------------------------
import pandas as pd
import re
import numpy as np
import os

# Folder path for saving the collated datasets
collated_data_path = r'C:\Users\sneha\Desktop\MISM 6212 Data Mining\Final project\Submission\Collated data'

# -----------------------------------------------------------------
# Area data collation
# -----------------------------------------------------------------

# Read the downloaded data files
df_area= pd.read_csv(r"C:\Users\sneha\Desktop\MISM 6212 Data Mining\Final project\Submission\Downloaded data\Area data\county_complete.csv")
df_area = df_area[df_area['state']=="Massachusetts"][['name','area_2010']].reset_index().drop(columns = 'index')
df_area.columns = ['COUNTY','LAND_AREA']
df_area['COUNTY'] = df_area['COUNTY'].map(lambda x: x.split(' ')[0]) 

# Exporting are data to csv
df_area.to_csv(collated_data_path + r'\Area_data.csv', index=False)
# Units:
# AREA in Sq.mi


# -----------------------------------------------------------------
# Population data collation
# -----------------------------------------------------------------

# Provide the folder path for the downloaded population data files:
pop_data_path = r'C:\Users\sneha\Desktop\MISM 6212 Data Mining\Final project\Submission\Downloaded data\Population data'

# Read the downloaded data files
df_pop_till_2019 = pd.read_csv(pop_data_path + r'\cc-est2019-agesex-25.csv')
df_pop_20_21 = pd.read_excel(pop_data_path + r'\co-est2021-pop-25.xlsx', header = 3, names = ['GEO_AREA','2020_POPESTIMATE_BASE','2020_POPESTIMATE','2021_POPESTIMATE']).drop(columns = '2020_POPESTIMATE_BASE')
df_pop_20_21 = df_pop_20_21.iloc[1:,:].dropna()

# Function to extract county name only from the column using string manipulation
def pop_extract_county(text):
    text = re.sub(r'[^\w\s]','',text)
    return text.split(' ')[0]

# Final columns needed in the population data
cols = ['YEAR','COUNTY','POPESTIMATE']
# Note: Since data format is different for population data (2016-19) and population data (2020-21), using different steps to extract the required information for the data files

# Population data (2016-2019)
df_pop_till_2019 = df_pop_till_2019.loc[df_pop_till_2019['YEAR']>= 9][[ 'CTYNAME','YEAR','POPESTIMATE']]
year_mapping = {9:'2016', 10:'2017', 11:'2018', 12:'2019'}
for i, row in df_pop_till_2019.iterrows():
    if row['YEAR'] in year_mapping.keys():
        df_pop_till_2019.loc[i,'YEAR'] = year_mapping.get(row['YEAR'])
        df_pop_till_2019.loc[i,'COUNTY'] = pop_extract_county(row['CTYNAME'])
df_pop_till_2019 = df_pop_till_2019[cols]

# Population data (2020-2021)
df_pop_20_21['COUNTY'] = df_pop_20_21['GEO_AREA'].map(lambda x: pop_extract_county(x)) 
df_pop_20_21 = df_pop_20_21.drop(columns = 'GEO_AREA').melt(id_vars = 'COUNTY').rename({'value':'POPESTIMATE'}, axis = 'columns')
df_pop_20_21['YEAR']=df_pop_20_21['variable'].map(lambda x: x.split('_')[0])
df_pop_20_21 = df_pop_20_21.drop(columns = 'variable')
df_pop_20_21  = df_pop_20_21[cols]

# Appending all population data from 2016 - 2021 and exporting to csv
df_pop = df_pop_till_2019.append(df_pop_20_21).sort_values(["COUNTY","YEAR"])
# Exporting population data to csv    
df_pop.to_csv(collated_data_path + r'\Population_data.csv', index=False)


# -----------------------------------------------------------------
# Weather data collation
# -----------------------------------------------------------------
# Provide folder path for the downloaded weather data files
weather_data_path = r'C:\Users\sneha\Desktop\MISM 6212 Data Mining\Final project\Submission\Downloaded data\Weather data'

dirListing = os.listdir(weather_data_path)
df_weather = pd.DataFrame()

# Calculation of average temperature, treating missing values and 
# aggregate weather information to daily level by doing a group by with date
for i in range(0,len(dirListing)):
    df = pd.read_csv(weather_data_path+'\\'+dirListing[i])
    df['TAVG'] = (df['TMAX'] + df['TMIN'])/2
    if 'AWND' not in df:
        df['AWND'] = np.nan
    df_grp = df.groupby('DATE')[['AWND','PRCP','TAVG']].mean().reset_index()
    df_grp['COUNTY'] = dirListing[i].split('_')[0] 
    
    col_order = ['COUNTY','DATE','TAVG','AWND','PRCP']
    df_grp = df_grp[col_order]
    df_weather = df_weather.append(df_grp, ignore_index = True)

# Exporting weather data to csv    
df_weather.to_csv(collated_data_path + r'\Weather_data.csv', index=False)

# Units: Standard
# TMAX, TMIN, TAVG in Farenheit
# PRCP in inches
# AWND in miles per hour

# ==================================== X END OF SCRIPT X ====================================
