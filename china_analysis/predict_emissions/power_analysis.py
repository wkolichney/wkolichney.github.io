###################
'''
CHINA POWER PLANT ANALYSIS: PREDICTING EMISSIONS
AUTHOR: William K. Olichney
DATE: 08/08/2025




'''
###################
### LIBRARIES ###
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
import os
import sqlalchemy
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, collect_set, concat_ws
from pyspark.sql.functions import split, explode, trim
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score
import matplotlib.ticker as ticker
from sqlalchemy import text
### DATA BASES ###
spark = SparkSession.builder.appName("EmissionsAggregation").getOrCreate()
engine = create_engine('sqlite:///power_plant_data.db')
directory = 'C:/Users/wikku/portfolio/bu_gci/machine_learn_power'  #USED ONLY FOR EIA EXCEL SHEETS ON EMISSIONS
########################################### LOAD DATA ########################################################################################################
df = pd.read_excel('C:/Users/wikku/portfolio/bu_gci/chinese_power_plant.xlsx') #given data

df['Technology'] = df['Technology'].str.lower() #standardize names
#unique id
df['id'] = df.index + 1
carbon = pd.read_excel('C:/Users/wikku/portfolio/bu_gci/co2_all_country.xlsx') #our world in data
nox_sulfur = pd.read_excel('C:/Users/wikku/portfolio/bu_gci/nitrogen_sulfur_all_country.xlsx') #our world in data

#SQL
df.to_sql('power_plant', con=engine, if_exists='replace', index=False)
carbon.to_sql('carbon_accounting', con=engine, if_exists='replace', index=False)
nox_sulfur.to_sql('nox_sulfur', con=engine, if_exists='replace', index=False)


files = sorted([f for f in os.listdir(directory) if f.endswith('.xlsx') and not f.startswith('~$')]) #for reading in multiple excel files from eia.gov

'''   
The following section reads multiple Excel files found from eia.gov containing emissions data for different pollutants,
takes the relevant columns, groups by year

'''
pollutants = {
    'CO2': 'Metric Tonnes of CO2 Emissions',
    'SO2': 'Selected SO2 Emissions (Metric Tonnes)',
    'NOx': 'Selected NOx Emissions (Metric Tonnes)'
}
id_cols = ['Plant Code', 'Aggregated Fuel Group', 'Generation (kWh)', 'Year']
pollutant_dfs = {}

for pollutant, emission_col in pollutants.items():
    all_years = []
    
    for file in files:
        filepath = os.path.join(directory, file)
        year = int(file.replace('emissions', '').replace('.xlsx', ''))

        try:
            df = pd.read_excel(filepath, sheet_name=pollutant, skiprows=1)
            df.columns = [col.strip() for col in df.columns]  # Remove whitespace
            df['Year'] = year

            # Drop non-numeric Plant Codes (footnotes, etc.)
            df = df[pd.to_numeric(df['Plant Code'], errors='coerce').notna()]
            df['Plant Code'] = df['Plant Code'].astype(int)

            # Keep only relevant columns
            df = df[['Plant Code', 'Aggregated Fuel Group', 'Generation (kWh)', 'Year', emission_col]].copy()
            df = df.rename(columns={emission_col: f'{pollutant.lower()}_emissions'})

            #group by plant, year, and fuel group. In the original dataset, the plants were split up by other factors than just their fuel group.
            #those other factors, like "prime mover" wont matter in the Chinese dataset, so we can just group by the plant, observation year, and observation fuel group (some plants had multiple)
            df = df.groupby(['Plant Code', 'Aggregated Fuel Group', 'Year'], as_index=False).sum()

            all_years.append(df)

        except Exception as e:
            print(f"Error reading {pollutant} from {file}: {e}")

    # Combine all years and store
    pollutant_dfs[pollutant] = pd.concat(all_years, ignore_index=True)

#Store in sql
for pollutant, df in pollutant_dfs.items():
    df.to_sql(f'{pollutant.lower()}_emissions', con = engine, if_exists='replace', index=False)

########################################### LOAD DATA DONE ###################################################################################################

########################################### SPARK DATA FRAME #################################################################################################
# SQL to Pandas to CSV to Spark DataFrame to Pandas Dataframe

'''
Why Spark? Why this convoluted process?
1. SQL helped me join the dataframes together to bring the emissions data together.
2. However, further aggregation was needed now that the emissions data comes together.
3. Pandas could not handle the sheer size of the data, but Spark can. It's a great at scaling data processing tasks.
4. Bringing Pandas directly to Spark is difficult, so I used a CSV as a middleman. Spark reads CSVs better, so create csv
5. Finally, I convert the Spark DataFrame back to Pandas for machine learning model training.

This pattern may not be the most efficient. In future, I will consider doing the SQL join directly in Spark or using PySpark for the entire process. However, this method gets the job done.
(it was also nice to have the csv file, that way I could skip beginning steps)

'''

#SQL to join emissions dataframes of CO2, SO2, and NOx
query = """
SELECT 
    co2."Plant Code" AS plant_id,
    co2."Aggregated Fuel Group",
    co2."Generation (kWh)",
    co2."Year",
    co2.CO2_emissions,
    so2.SO2_emissions,
    nox.NOx_emissions
FROM co2_emissions co2
LEFT JOIN so2_emissions so2
    ON co2."Plant Code" = so2."Plant Code" AND co2."Year" = so2."Year"
LEFT JOIN nox_emissions nox
    ON co2."Plant Code" = nox."Plant Code" AND co2."Year" = nox."Year"
"""
#SQL to pandas, just something I'm used to
combined_df = pd.read_sql_query(query, con = engine)

#pandas to csv, so I can read it into spark. Certainly a middleman step, but it works
combined_df.to_csv('all_emissions_combined.csv', index=False) #THIS WILL CREATE A 2+ GB CSV FILE, MAKE SURE YOU HAVE ENOUGH SPACE - sorry
#Spark read csv, so I can do aggregations with a large dataset
spark_df = spark.read.csv('all_emissions_combined.csv', header=True, inferSchema=True)
#aggregations
#why aggregate? If you look at the dataset that joined the emissions data, you will see that the data set is "taking turns" with the emissions data.
#For example, when the data has values for CO2 emissions, it does not have values for SO2 or NOx emissions. When it has values for SO2 emissions, it does not have values for CO2 or NOx emissions. ETC.
#So, I need to aggregate the data by plant and year, so that I can have a single row for each plant and year, with the emissions data on CO2, SO2, and NOx for that plant and year.
collapsed_df = (
    spark_df.groupBy("plant_id", "Year")
    .agg(
        spark_sum("Generation (kWh)").alias("generation_kwh"),
        spark_sum("CO2_emissions").alias("co2_emissions"),
        spark_sum("SO2_emissions").alias("so2_emissions"),
        spark_sum("NOx_emissions").alias("nox_emissions"),
        concat_ws(",", collect_set("Aggregated Fuel Group")).alias("fuel_group")
    )
)
#split by fuel group, so that each fuel group is its own row, which will repeat plants and years
#while not ideal, this says that the gas part of the plant produces the same emissions as the coal part of the plant, which is not true, but it is a good enough approximation for this analysis
exploded_df = (
    collapsed_df
    .withColumn("fuel_group", explode(split("fuel_group", ",")))
    .withColumn("fuel_group", trim("fuel_group"))
)
#back to Pandas -> used for machine learning model
final_df = exploded_df.toPandas()
########################################### SPARK DATA FRAME DONE ############################################################################################
########################################### MACHINE LEARNING MODEL ###########################################################################################
# turn generation_kwh to mwh
final_df['generation_mwh'] = final_df['generation_kwh'] / 1000
#drop kwh
final_df = final_df.drop(columns=['generation_kwh'])
#change names of the fuel groups PET turn to oil, GAS to gas, and COAL to coal
final_df['fuel_group'] = final_df['fuel_group'].replace({
    'PET': 'oil',
    'GAS': 'gas',
    'COAL': 'coal'
})
final_df = final_df[~final_df['fuel_group'].isin(['MSW', 'GEO'])] #only wanted to focus on coal, gas, and oil

final_df = final_df.dropna() #not many nas, not a big deal
#replace zeros if necessary to avoid log(0)
final_df = final_df[
    (final_df['co2_emissions'] > 0) &
    (final_df['so2_emissions'] > 0) &
    (final_df['nox_emissions'] > 0)
]
final_df['log_co2'] = np.log(final_df['co2_emissions']) #will standardize
final_df['log_so2'] = np.log(final_df['so2_emissions'])
final_df['log_nox'] = np.log(final_df['nox_emissions'])

#I belive there was a a "test" plant id 9999 that was a huge outlier, so this code will remove that and other possible outliers
co2_thresh = final_df['co2_emissions'].quantile(0.99)
so2_thresh = final_df['so2_emissions'].quantile(0.99)
nox_thresh = final_df['nox_emissions'].quantile(0.99)

#keep rows below the 99th percentile for all three
final_df = final_df[
    (final_df['co2_emissions'] <= co2_thresh) &
    (final_df['so2_emissions'] <= so2_thresh) &
    (final_df['nox_emissions'] <= nox_thresh)
]
final_df.rename(columns={'Year': 'year'}, inplace=True)  # Rename for consistency
#machine learning models
X = final_df[['generation_mwh', 'year', 'fuel_group']]
X = pd.get_dummies(X, columns=['fuel_group'], drop_first=True)
### Make a model for each emission type as the y ###
y_co2 = final_df['log_co2']
y_so2 = final_df['log_so2']
y_nox = final_df['log_nox']


'''
The following code will autmoatically train a Random Forest model for each pollutant (CO2, SO2, NOx), three different models with each pollutant as the target variable.
'''
targets = {
    'co2': y_co2,
    'so2': y_so2,
    'nox': y_nox
}

# Store results for each
results = {}
models = {} #store the models to apply to chinese power plant data

for label, y in targets.items():
    # Split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Model
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    # Evaluate
    r2 = r2_score(y_test, y_pred)
    rmse = mean_squared_error(y_test, y_pred, squared=False)
    results[label] = (r2, rmse)
    models[label] = model  # Store the model for later use

    # Plot Actual vs Predicted
    plt.figure(figsize=(10, 7))
    plt.scatter(y_test, y_pred, alpha=0.5)
    plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--')
    plt.xlabel(f"Actual {label} Emissions")
    plt.ylabel(f"Predicted {label} Emissions")
    plt.title(f"Random Forest: Actual vs Predicted {label} Emissions")
    plt.grid(True)
    plt.tight_layout()
    plt.show()
    plt.close()

    # Feature importances
    importances = model.feature_importances_
    feature_names = X.columns
    sorted_idx = importances.argsort()

    plt.figure(figsize=(8, 6))
    plt.barh(range(len(importances)), importances[sorted_idx], align='center')
    plt.yticks(range(len(importances)), [feature_names[i] for i in sorted_idx])
    plt.title(f"{label} Model Feature Importance")
    plt.tight_layout()
    plt.show()
    plt.close()

# Print all results
for label, (r2, rmse) in results.items():
    print(f"{label} Model R²: {r2:.3f}")
    print(f"{label} Model RMSE: {rmse:.2f}")
########################################### MACHINE LEARNING MODEL DONE ######################################################################################
########################################### APPLY MACHINE LEARNING MODEL TO CHINESE DATA #####################################################################
query = """ SELECT id, "Capacity (MW)" AS mw, "Year of Commission" as year_commission, Technology AS technology, Country AS country FROM power_plant WHERE year_commission BETWEEN 1995 AND 2032"""
chinese_power_df = pd.read_sql_query(query, con=engine) #read in the power plant data from the SQL database
#turn year_commission to year
chinese_power_df['year_commission'] = pd.to_datetime(chinese_power_df['year_commission'], format='%Y').dt.year
chinese_power_df['mwh'] = chinese_power_df['mw'] * 8760 #convert MW to MWh, assuming running every hour of the year
chinese_power_df = chinese_power_df.drop(columns=['mw']) #drop MW column, not needed anymore
chinese_power_df = chinese_power_df[~chinese_power_df['technology'].isin(['hydropower', 'solar', 'wind', 'biomass', 'nuclear', 'geothermal', 'waste'])]  #i only focuses on coal, gas, and oil
#Define target range -> I want to not only predict emissions for the years in the dataset, but also for future years, but i didn't want to go too far into the past either
future_years = range(1995, 2040)

#Expand the DataFrame
df_expanded = (
    chinese_power_df.assign(key=1)
    .merge(pd.DataFrame({'year': future_years, 'key': 1}), on='key')
    .query('year >= year_commission')  # Only keep rows where the plant exists call it 'Year' to align with model name
    .drop(columns='key')
)
#align with model name
df_expanded.rename(columns={'mwh': 'generation_mwh'}, inplace=True)
df_expanded['fuel_group'] = df_expanded['technology'].str.lower()
df_expanded['fuel_group_gas'] = (df_expanded['fuel_group'] == 'gas').astype(int)
df_expanded['fuel_group_oil'] = (df_expanded['fuel_group'] == 'oil').astype(int)
X_new = df_expanded[['generation_mwh', 'year', 'fuel_group_gas', 'fuel_group_oil']]
df_expanded['Predicted_co2'] = models['co2'].predict(X_new)
df_expanded['Predicted_so2'] = models['so2'].predict(X_new)
df_expanded['Predicted_nox'] = models['nox'].predict(X_new)
df_expanded.drop(columns=['fuel_group_gas', 'fuel_group_oil', 'fuel_group'], inplace=True)  # Clean up the DataFrame
#Convert log predictions back to original scale
df_expanded['pred_co2_emissions'] = np.exp(df_expanded['Predicted_co2'])
df_expanded['pred_so2_emissions'] = np.exp(df_expanded['Predicted_so2'])
df_expanded['pred_nox_emissions'] = np.exp(df_expanded['Predicted_nox'])
df_expanded.to_sql('predicted_emissions', con=engine, if_exists='replace', index=False)# Store the predictions in SQL
########################################### APPLY MACHINE LEARNING MODEL TO CHINESE DATA DONE ################################################################
########################################### CHINESE EMISSIONS AS A PROPORTION OF RECIPIENT COUNTRY'S EMISSIONS ###############################################
query = """
SELECT 
    nox_sulfur.entity, 
    nox_sulfur.year, 
    "nitrogen_oxide" as "nox_emissions", 
    "sulfur_dioxide" as "so2_emissions",
    "annual_co2" as "annual_co2_emissions",
    predicted_emissions.country,
    predicted_emissions.year as pred_year,
    predicted_emissions.pred_co2_emissions,
    predicted_emissions.pred_so2_emissions,
    predicted_emissions.pred_nox_emissions
FROM nox_sulfur
JOIN carbon_accounting 
    ON nox_sulfur.entity = carbon_accounting.entity 
    AND nox_sulfur.year = carbon_accounting.year
JOIN predicted_emissions 
    ON nox_sulfur.entity = predicted_emissions.country 
    AND nox_sulfur.year = predicted_emissions.year
"""
nox_sulfur_carbon_df = pd.read_sql_query(query, con=engine)
nox_sulfur_carbon_df['pred_co2_emissions'] = nox_sulfur_carbon_df['pred_co2_emissions'] / 1e6  # convert to million metric tons
nox_sulfur_carbon_df['pred_so2_emissions'] = nox_sulfur_carbon_df['pred_so2_emissions'] / 1e3  # convert to thousand metric tons
nox_sulfur_carbon_df['pred_nox_emissions'] = nox_sulfur_carbon_df['pred_nox_emissions'] / 1e3 # convert to thousand metric tons
nox_sulfur_carbon_df['nox_emissions'] = nox_sulfur_carbon_df['nox_emissions'] / 1e3  # convert to thousand metric tons
nox_sulfur_carbon_df['so2_emissions'] = nox_sulfur_carbon_df['so2_emissions'] / 1e3 # convert to thousand metric tons
nox_sulfur_carbon_df['annual_co2_emissions'] = nox_sulfur_carbon_df['annual_co2_emissions'] / 1e6 # convert to million metric tons


### PLOTTING ###

## CO2 ##
# Make sure year columns are numeric
nox_sulfur_carbon_df['year'] = nox_sulfur_carbon_df['year'].astype(int)
nox_sulfur_carbon_df['pred_year'] = nox_sulfur_carbon_df['pred_year'].astype(int)

for country in sorted(nox_sulfur_carbon_df['country'].unique()):
    country_df = nox_sulfur_carbon_df[nox_sulfur_carbon_df['country'] == country]

    # years for THIS country only
    years = sorted(country_df['year'].unique())

    # annual actual totals
    actual = (
        country_df.groupby('year')['annual_co2_emissions']
        .sum()
        .reindex(years, fill_value=0)
    )

    # annual Chinese totals (no cumsum!)
    predicted = (
        country_df.groupby('pred_year')['pred_co2_emissions']
        .sum()
        .reindex(years, fill_value=0)
    )

    # optionally ensure the stack never goes negative
    predicted = predicted.clip(upper=actual)

    if predicted.sum() == 0 and actual.sum() == 0:
        continue

    other = actual - predicted

    # --- Plot ---
    plt.figure(figsize=(10, 5))
    plt.bar(years, other, label='Other CO₂', color='gray')
    plt.bar(years, predicted, bottom=other, label='Chinese Plants CO₂', color='red')

    # % labels (light offset based on scale)
    for x, tot, chi in zip(years, actual, predicted):
        if tot > 0:
            pct = 100 * chi / tot
            if pct >= 1:
                plt.text(x, tot * 1.01, f"{pct:.1f}%", ha='center', fontsize=8)

    plt.title(f"CO₂ Emissions in {country} (Actual vs. Chinese Plants)")
    plt.xlabel("Year")
    plt.ylabel("CO₂ Emissions (million metric tons)")  # units match your scaling
    plt.xticks(years, [str(y) for y in years], rotation=45)
    plt.grid(axis='y', linestyle='--', alpha=0.6)
    plt.legend()
    plt.tight_layout()
    plt.show()
    plt.close()

## SO2 ##
for country in sorted(nox_sulfur_carbon_df['country'].unique()):
    country_df = nox_sulfur_carbon_df[nox_sulfur_carbon_df['country'] == country]

    # years for THIS country only
    years = sorted(country_df['year'].unique())

    # annual actual totals
    actual = (
        country_df.groupby('year')['so2_emissions']
        .sum()
        .reindex(years, fill_value=0)
    )

    # annual Chinese totals
    predicted = (
        country_df.groupby('pred_year')['pred_so2_emissions']
        .sum()
        .reindex(years, fill_value=0)
    )

    # optionally ensure the stack never goes negative
    predicted = predicted.clip(upper=actual)

    if predicted.sum() == 0 and actual.sum() == 0:
        continue

    other = actual - predicted

    # --- Plot ---
    plt.figure(figsize=(10, 5))
    plt.bar(years, other, label='Other SO2', color='gray')
    plt.bar(years, predicted, bottom=other, label='Chinese Plants SO2', color='red')

    # % labels (light offset based on scale)
    for x, tot, chi in zip(years, actual, predicted):
        if tot > 0:
            pct = 100 * chi / tot
            if pct >= 1:
                plt.text(x, tot * 1.01, f"{pct:.1f}%", ha='center', fontsize=8)

    plt.title(f"SO2 Emissions in {country} (Actual vs. Chinese Plants)")
    plt.xlabel("Year")
    plt.ylabel("SO2 Emissions (thousand metric tons)")  # units match your scaling
    plt.xticks(years, [str(y) for y in years], rotation=45)
    plt.grid(axis='y', linestyle='--', alpha=0.6)
    plt.legend()
    plt.tight_layout()
    plt.show()
    plt.close()
## NOx ##
for country in sorted(nox_sulfur_carbon_df['country'].unique()):
    country_df = nox_sulfur_carbon_df[nox_sulfur_carbon_df['country'] == country]

    # years for THIS country only
    years = sorted(country_df['year'].unique())

    # annual actual totals
    actual = (
        country_df.groupby('year')['nox_emissions']
        .sum()
        .reindex(years, fill_value=0)
    )

    # annual Chinese totals
    predicted = (
        country_df.groupby('pred_year')['pred_nox_emissions']
        .sum()
        .reindex(years, fill_value=0)
    )

    # optionally ensure the stack never goes negative
    predicted = predicted.clip(upper=actual)

    if predicted.sum() == 0 and actual.sum() == 0:
        continue

    other = actual - predicted

    # --- Plot ---
    plt.figure(figsize=(10, 5))
    plt.bar(years, other, label='Other NOx', color='gray')
    plt.bar(years, predicted, bottom=other, label='Chinese Plants NOx', color='red')

    # % labels (light offset based on scale)
    for x, tot, chi in zip(years, actual, predicted):
        if tot > 0:
            pct = 100 * chi / tot
            if pct >= 1:
                plt.text(x, tot * 1.01, f"{pct:.1f}%", ha='center', fontsize=8)

    plt.title(f"NOx Emissions in {country} (Actual vs. Chinese Plants)")
    plt.xlabel("Year")
    plt.ylabel("NOx Emissions (thousand metric tons)")  # units match your scaling
    plt.xticks(years, [str(y) for y in years], rotation=45)
    plt.grid(axis='y', linestyle='--', alpha=0.6)
    plt.legend()
    plt.tight_layout()
    plt.show()
    plt.close()
