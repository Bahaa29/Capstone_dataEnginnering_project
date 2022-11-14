# CAPSTONE PROJECT THE FINAL UDACITY PROJECT IN THE NANO DEGREE
![c-1](https://user-images.githubusercontent.com/51878421/201762457-44d1dd40-57dd-4aa8-b6f2-1b0273cfd220.jpg)


## Immigration in the USA

### Detials
This projects aims to enrich the US I94 immigration data with further data such as US airport data, US demographics and temperature data to have a wider basis for analysis on the immigration data.

## data source
The following datasets are included in the project workspace. We purposely did not include a lot of detail about the data and instead point you to the sources. This is to help you get experience doing a self-guided project and researching the data yourself. If something about the data is unclear, make an assumption, document it, and move on. Feel free to enrich your project by gathering and including additional data sources.

I94 Immigration Data: This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. This is where the data comes from. There's a sample file so you can take a look at the data in csv format before reading it all in. You do not have to use the entire dataset, just use what you need to accomplish the goal you set at the beginning of the project.
*-https://www.trade.gov/national-travel-and-tourism-office-*

World Temperature Data: This dataset came from Kaggle. You can read more about it here.
*-https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data-*
U.S. City Demographic Data: This data comes from OpenSoft. You can read more about it here.
*-https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/-*
Airport Code Table: This is a simple table of airport codes and corresponding cities. It comes from here.
*-https://datahub.io/core/airport-codes#data-*


## Accessing the Data

Some of the data is already uploaded to the workspace, which you'll see in the navigation pane within Jupyter Lab. The immigration data and the global temperate data is in an attached disk.

Immigration Data
You can access the immigration data in a folder with the following path: ../../data/18-83510-I94-Data-2016/. There's a file for each month of the year. An example file name is i94_apr16_sub.sas7bdat. Each file has a three-letter abbreviation for the month name. So a full file path for June would look like this: ../../data/18-83510-I94-Data-2016/i94_jun16_sub.sas7bdat. Below is what it would look like to import this file into pandas. Note: these files are large, so you'll have to think about how to process and aggregate them efficiently.

```{fname = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat' df = pd.read_sas(fname, 'sas7bdat', encoding="ISO-8859-1") ```}.

## Temperature Data
You can access the temperature data in a folder with the following path: ../../data2/. There's just one file in that folder, called GlobalLandTemperaturesByCity.csv. Below is how you would read the file into a pandas dataframe.
```{fname = '../../data2/GlobalLandTemperaturesByCity.csv' df = pd.read_csv(fname)}```
## tables 
| table name | columns | description | type |
| ------- | ---------- | ----------- | ---- |
| airports | iata_code - name - type - local_code - coordinates - city | stores information related to airports | dimension table |
| Cities | city - state- male_population - female_population - total_population - num_veterans - foreign_born - state_code | stores demographics data for cities | dimension table |
| immr_fact | cic_id, year,month,city_code,state_code ,arriave_date ,departure_date, mode,visa,country | stores all i94 immigrations data | fact table |
| temp_fact | timestamp - average_temperature - average_temperatur_uncertainty - city - country - latitude - longitude | stores temperature information | dimension table |
|vistior_dim|cic_id,citizen_country,residence_country,birth_year,gender|store visitor data|dimension table|
|airline_dim|cic_id, airline,admin_num,flight_number|store airline data|dimension table|

## Conceptual Data Model
<img src="./data_model.jpg" alt="Alt text" title="data_model">

## QUESTIONS TO ANSWER 
*The data was increased by 100x.
Use Spark to process the data efficiently in a distributed way e.g. with EMR. In case we recognize that we need a write-heavy operation, I would suggest using a Cassandra database instead of PostgreSQL.

*The data populates a dashboard that must be updated on a daily basis by 7am every day.
Use Airflow and create a DAG that performs the logic of the described pipeline. If executing the DAG fails, I recommend to automatically send emails to the engineering team using Airflow's builtin feature, so they can fix potential issues soon.

*The database needed to be accessed by 100+ people.
Use RedShift to have the data stored in a way that it can efficiently be accessed by many people. Also, we can use a database such as PostgreSQL in a more cost-efficient setting that will, however, have slightly lower performance due to its nature.

## TOOLS THAT I USED
1] Pandas is used to ease data preprocessing and visualisation. It is helpful to efficiently load and manipulate data. At a later stage, instead of pandas dataframes

2] I recommend integrating the ETL pipeline into an Airflow DAG.

3] I used a Jupyter Notebook to show the data structure and the need for data cleaning. Python is an often used programming language and was used because it is the language I am the most comfortable with.

4]I recommend using Spark dataframes to allow distributed processing using for example Amazon REDSHIFT. Also, to perform automated updates

## data dict
![data_dictionary](https://user-images.githubusercontent.com/51878421/201352630-2eb32811-2fe4-485d-a079-cffad5639944.png)

## imporved ETL I CAN DO 

Source_S3_Bucket/temperature/GlobalLandTemperaturesByCity.csv
Source_S3_Bucket/demography/us-cities-demographics.csv
Source_S3_Bucket/IMMR/18-83510-I94-Data-2016/*.sas7bdat
Source_S3_Bucket/I94_SAS_Labels_Descriptions.SAS
Source_S3_Bucket/AIRPORT/AIRPORT.csv
Transform immigration data to 1 fact table and 2 dimension tables, fact table will be partitioned by state
Parsing label description file to get auxiliary tables
Transform temperature data to dimension table
Store these tables back to target S3 bucket
