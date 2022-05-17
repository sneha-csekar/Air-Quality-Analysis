# -*- coding: utf-8 -*-

# ==============================================
# Code objective: DATA TRANSFORMATION AND MODELLING
# ==============================================
# a) Final data
    # The Final data, which integrated All pollution data, housing data, weather data, and demographics data.
    # Data: Pleae use Final.csv file for this coding

# ==============================================

#import data libraries
import pandas as pd
import numpy as np
#import visualization libraries
import matplotlib.pyplot as plt
import seaborn as sns

data = pd.read_csv(r"C:\Users\sneha\Desktop\MISM 6212 Data Mining\Final project\Submission\Final dataset\Final_dataset.csv")

rename_dict = {'Month_year':'MONTH_YEAR','Year': 'YEAR', 'Month':'MONTH', 'County':'COUNTY','Median_Price': 'Median Price', 'Est_Population':'POPESTIMATE', 'Land_Area':'LAND_AREA','NO2_Cnct': 'NO2 Concentration', 'CO_Cnct':'CO Concentration', 'Ozone_Cnct':'Ozone Concentration', 'SO2_Cnct': 'SO2 Concentration', 'PM2.5_Cnct':'PM2.5 Concentration', 'Avg_Air_Temp':'TAVG', 'Avg_Wind_Spd': 'AWND' , 'Avg_Prec':'PRCP'}
df = data.rename(columns = rename_dict)


df.columns
#check head dataframe
df.head()
#check info dataframe
df.info()
#check stats df
df.describe()
#check column names
df.columns
#check shape df
df.shape

#visualize the data before cleansing

#pairplot
sns.pairplot(df)
#distribution plot
sns.distplot(df['Median Price'])
#heatmap with values
sns.heatmap(df.corr(),annot =True)

#check missing values- NO2 Concentration and CO Concentration have missing values
df.isnull().sum()


df.columns
df.dtypes

df['COUNTY'].unique()
#array(['Essex', 'Hampden', 'Hampshire', 'Suffolk', 'Worcester']
df.columns

#visualize the missing data
plt.figure(figsize=(10,10))
sns.heatmap(df.isnull())

#Applied imputation strategy
df['NO2 Concentration'] = df['NO2 Concentration'].fillna(df.groupby('COUNTY')['NO2 Concentration'].transform('mean'))
df['CO Concentration'] = df['CO Concentration'].fillna(df.groupby('COUNTY')['CO Concentration'].transform('mean'))


# Visualize after mean imputation of missing values
plt.figure(figsize=(10,10))
sns.heatmap(df.isnull())

#Verifying the imputation method results
df.isnull().sum()



#import modelling libraries
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error


# Preparing data for modelling
df = df.drop(columns = 'MONTH_YEAR')
df_dummy=pd.get_dummies(df,drop_first=True)

x=df_dummy.drop(columns='Median Price')
y=df_dummy['Median Price']

x_train,x_test,y_train,y_test = train_test_split(x,y,test_size=0.3,random_state=1)


# =============================================================================
# Linear regression model
# =============================================================================

model=LinearRegression()
model.fit(x_train,y_train)


y_pred=model.predict(x_test)
y_pred

min(df_dummy['Median Price'])


rmse_test_LR = mean_squared_error(y_test,y_pred,squared=False)
print("Testing data rmse for Linear Regression is :",round(rmse_test_LR,2))
# Final
# Testing data rmse for Linear Regression is : 27620.61



y_pred_train=model.predict(x_train)
rmse_train_LR = mean_squared_error(y_train,y_pred_train,squared=False)
print("Training data rmse for Linear Regression is:",rmse_train_LR)
# Final:
# Training data rmse for Linear Regression is: 27506.219131401056

# =============================================================================
#Linear regression model with K best feature selection:
# =============================================================================
from sklearn.feature_selection import SelectKBest,f_regression

# -------------------------------------
# Trial with k =20
# -------------------------------------
bestfeatures=SelectKBest(score_func=f_regression, k = 20) ##initialize
bestfeatures.fit(x_train,y_train) ##training

bestfeatures.get_support()


cols = bestfeatures.get_support(indices=True)
cols
x_train_new = x_train.iloc[:,cols]
x_train_new.columns

new_x_train = bestfeatures.transform(x_train)
new_x_test = bestfeatures.transform(x_test)

lm=LinearRegression()
lm.fit(new_x_train,y_train)

y_predictions_k_20 = lm.predict(new_x_test)
rmse_test_skbest_20 = mean_squared_error(y_test, y_predictions_k_20,squared=False)
print("RMSE for testing data after skbest (k=20) is :", round(rmse_test_skbest_20,2))
# Final
# RMSE for testing data after skbest (k=2) is : 28432.51

# -------------------------------------
# Trial with k = 10
# -------------------------------------
bestfeatures=SelectKBest(score_func=f_regression, k = 10) ##initialize
bestfeatures.fit(x_train,y_train) ##training

bestfeatures.get_support()


cols = bestfeatures.get_support(indices=True)
cols
x_train_new = x_train.iloc[:,cols]
x_train_new.columns
# Selected columns
# ['CO Concentration', 'Ozone Concentration', 'AWND', 'POPESTIMATE', 'LAND_AREA', 'YEAR', 'COUNTY_Hampden', 'COUNTY_Hampshire',       'COUNTY_Suffolk', 'COUNTY_Worcester']

new_x_train = bestfeatures.transform(x_train)
new_x_test = bestfeatures.transform(x_test)

lm=LinearRegression()
lm.fit(new_x_train,y_train)

y_predictions_k_10=lm.predict(new_x_test)
rmse_test_skbest_10 = mean_squared_error(y_test, y_predictions_k_10,squared=False)
print("RMSE for testing data after skbest (k=10) is :", round(rmse_test_skbest_10,2))
# Final
# RMSE for testing data after skbest (k=10) is : 30014.13

# -------------------------------------
# Trial with k = 5
# -------------------------------------
bestfeatures=SelectKBest(score_func=f_regression, k = 5) ##initialize
bestfeatures.fit(x_train,y_train) ##training

bestfeatures.get_support()

cols = bestfeatures.get_support(indices=True)
cols
x_train_new = x_train.iloc[:,cols]
x_train_new.columns

# Selected columns
# ['AWND', 'POPESTIMATE', 'LAND_AREA', 'COUNTY_Hampden', 'COUNTY_Suffolk']

new_x_train = bestfeatures.transform(x_train)
new_x_test = bestfeatures.transform(x_test)

lm=LinearRegression()
lm.fit(new_x_train,y_train)


y_predictions_k_5 = lm.predict(new_x_test)
rmse_test_skbest_5 = mean_squared_error(y_test, y_predictions_k_5,squared=False)
print("RMSE for testing data after skbest (k=5) is :", round(rmse_test_skbest_5,2))
# Final
# RMSE for testing data after skbest (k=5) is : 55999.04

# -------------------------------------
# Trial with k = 6
# -------------------------------------
bestfeatures=SelectKBest(score_func=f_regression, k = 6) ##initialize
bestfeatures.fit(x_train,y_train) ##training

bestfeatures.get_support()

cols = bestfeatures.get_support(indices=True)
cols
x_train_new = x_train.iloc[:,cols]
x_train_new.columns
# Selected columns
# ['AWND', 'POPESTIMATE', 'LAND_AREA', 'YEAR', 'COUNTY_Hampden', 'COUNTY_Suffolk']

new_x_train = bestfeatures.transform(x_train)
new_x_test = bestfeatures.transform(x_test)

lm=LinearRegression()
lm.fit(new_x_train,y_train)

y_predictions_k_6 = lm.predict(new_x_test)
rmse_test_skbest_6 = mean_squared_error(y_test, y_predictions_k_6,squared=False)
print("RMSE for testing data after skbest (k=6) is :",rmse_test_skbest_6)
# Final
# RMSE for testing data after skbest (k=6) is : 31091.370125255293

# Insights

# From our problem knowledge, we felt 10 variables were important in explaining median prices. With k = 10,
# we found the following columns to be selected by the K Best features model:
# ['CO Concentration', 'Ozone Concentration', 'AWND', 'POPESTIMATE', 'LAND_AREA', 'YEAR', 'COUNTY_Hampden', 'COUNTY_Hampshire',       'COUNTY_Suffolk', 'COUNTY_Worcester']

# Trying with k = 5, we found that the model did not pick pollutant concentration variables among the top 5.
# Population, land area and type of county (industrial city/suburb) played a bigger role.


# ------------------------------------------
# RMSE and K curve - finding the elbow point
# ------------------------------------------
xplot=[]
yplot=[]

for i in range(1,27):
    bestfeatures=SelectKBest(score_func=f_regression,k=i) ##initialize
    bestfeatures.fit(x_train,y_train) ##training(finding correlation)
    ##bestfeatures.get_support()
    new_x_train=bestfeatures.transform(x_train)
    new_x_test=bestfeatures.transform(x_test)
   
    ##building the model on selected columns
    lm=LinearRegression()
    lm.fit(new_x_train,y_train)
   
    predictions=lm.predict(new_x_test)
   
    rmse_test_after_skbest=mean_squared_error(y_test,predictions,squared=False)
    print("no of features:",i)
    print("rmse_test_after_skbest:",rmse_test_after_skbest)
    xplot.append(i)
    yplot.append(rmse_test_after_skbest)
plt.plot(xplot,yplot)
plt.xlabel('No of features')
plt.ylabel('RMSE')
plt.title('RMSE vs k value in Select K best features')

# At k = 6, RMSE value drops considerably as seen in graph. 
# With further increase in k, only minimal reduction seen in RMSE.

# =============================================================================
# Random forest regression model :
# =============================================================================

from sklearn.metrics import classification_report,accuracy_score
from sklearn.ensemble import RandomForestRegressor

model3 = RandomForestRegressor()
model3.fit(x_train,y_train)

model3.score(x_test,y_test)
#0.9665724424128596

y_pred_RF = model3.predict(x_test)
rmse_test_RF = mean_squared_error(y_test, y_pred_RF,squared=False)
print("RMSE for testing data with Random Forest is :", round(rmse_test_RF,2))

# Final
# RMSE for testing data with Random Forest is : 26230.66


# =============================================================================
# Random forest regression model with hyper-parameter tuning:
# =============================================================================

# Improve Random Forest model by tuning
from sklearn.model_selection import RandomizedSearchCV

n_estimators = [int(x) for x in np.linspace(start = 200, stop = 1000, num = 5)]
max_features = ['auto', 'sqrt']
max_depth = [int(x) for x in np.linspace(10, 110, num = 11)]
max_depth.append(None)
min_samples_split = [2, 5, 10]
min_samples_leaf = [1, 2, 4]
bootstrap = [True, False]

random_grid = {'n_estimators': n_estimators, 'max_features': max_features,
               'max_depth': max_depth,
               'min_samples_split': min_samples_split,
               'min_samples_leaf': min_samples_leaf,
               'bootstrap': bootstrap}

rf = RandomForestRegressor()
rf_random = RandomizedSearchCV(estimator = rf, param_distributions = random_grid, n_iter = 100, cv = 3, verbose=2, random_state=42, n_jobs = -1)
rf_random.fit(x_train,y_train)


rf_random.best_params_
rf_random.score(x_test,y_test)


prediction_rf_tuned = rf_random.predict(x_test)


rmse_test_rf_tuned = mean_squared_error(y_test,prediction_rf_tuned,squared=False)
print("RMSE for testing data with tuned Random Forest is :", round(rmse_test_rf_tuned,2))
# Final
# RMSE for testing data with tuned Random Forest is : 26064.67

# we need more pollutant variables details& more granular data at daily level to improve the performance of RF.
#visualization
sns.distplot(y_test - prediction_rf_tuned)


#Linear regression rmse
rmse_test_LR   = round(rmse_test_LR,2)
rmse_test_LR
#Linear regression with SelectKBest rmse
rmse_test_skbest_6 =  round(rmse_test_skbest_6,2)
rmse_test_skbest_6
#Random Forest rmse
rmse_test_RF       = round(rmse_test_RF,2)
rmse_test_RF
#Random forest with hyperparameter tuning rmse
rmse_test_rf_tuned = round(rmse_test_rf_tuned,2)
rmse_test_rf_tuned
# =============================================================================
# Comparison of all models :
# =============================================================================

final_df = pd.DataFrame([('Linear Regression:',rmse_test_LR),('Linear Regression Tuned:',rmse_test_skbest_6),('Random Forest:',rmse_test_RF),('Random Forest Tuned:',rmse_test_rf_tuned)], columns = ('Model Name', 'RMSE'))
final_df
#When compared to other models, the random forest with hyperparameter adjustment has a low rmse.
