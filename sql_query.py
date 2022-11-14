drop_immigrations = "DROP TABLE IF EXISTS immr_fact;"
drop_temp_dim = "DROP TABLE IF EXISTS temp_dim;"
drop_vistior_dim = "DROP TABLE IF EXISTS vistior_dim ;"
drop_cities_dim = "DROP TABLE IF EXISTS cities_dim;"
drop_airline_dim = "DROP TABLE IF EXISTS airline_dim;"
drop_airports_dim = "DROP TABLE IF EXISTS airports_dim;"

create_immr_fact="""
CREATE TABLE IF NOT EXISTS public.immr_fact (
cic_id float, 
year float, 
month float , 
city_code varchar, 
state_code varchar , 
arrive_date Timestamp, 
departure_date Timestamp, 
mode float , 
visa float,
country varchar 
);
"""
create_temp_dim="""
CREATE TABLE IF NOT EXISTS public.temp_dim(
dt  DATE,
average_temperature  FLOAT,
average_temperature_uncertainty FLOAT,
city    VARCHAR,
country VARCHAR,
year int,
month int 
);
"""

create_cities_dim="""
create table if not exists public.cities_dim(
city varchar ,
state varchar ,
male_population int,
female_population int,
total_population int,
num_veterans int,
foreign_born int,
state_code varchar
);
"""
create_vistior_dim="""
create table if not exists public.vistior_dim(
cic_id float,
citizen_country float, 
residence_country float, 
birth_year float, 
gender varchar
);
"""

create_airline_dim="""
create table if not exists public.airline_dim(
cic_id float,
airline varchar, 
admin_num float, 
flight_number float
);
"""

create_airports_dim = """
CREATE TABLE IF NOT EXISTS public.airports_dim (
iata_code    VARCHAR PRIMARY KEY,
name         VARCHAR,
type         VARCHAR,
local_code   VARCHAR,
coordinates  VARCHAR,
cities_code  VARCHAR,
elevation_ft FLOAT,
continent    VARCHAR,
iso_country  VARCHAR,
iso_region   VARCHAR,
municipality VARCHAR,
gps_code     VARCHAR
);
"""

airport_insert = """
INSERT INTO airports (iata_code, name, type, local_code, coordinates, city, elevation_ft, continent, \
    iso_country, iso_region, municipality, gps_code) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

demographic_insert = """
INSERT INTO cities_dim (city, state, media_age, male_population, female_population, total_population, \
num_veterans, foreign_born, average_household_size, state_code, race, count) VALUES (%s, %s, %s, %s, \
%s, %s, %s, %s, %s, %s, %s, %s)"""


temperature_insert = ("""
INSERT INTO tem_dim (dt, average_temperature, average_temperature_uncertainty, city, country, \
latitude, longitude) VALUES (%s, %s, %s, %s, %s, %s, %s)""")

immigration_insert = ("""
INSERT INTO immr_fact(cic_id, year, month,city_code,state_code,arrive_date,departure_date,mode,visa,country) \
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
""")

vistior_insert =("""
INSERT INTO vistior_dim (cic_id,citizen_country,residence_country,birth_year,gender)\
VALUES(%s, %s, %s, %s, %s)
""")

airline_insert =("""
INSERT INTO airline_dim(cic_id,airline,admin_num,flight_number,visa_type)\
VALUES(%s, %s, %s, %s, %s)
""")
create_tables=[create_immr_fact,create_temp_dim,create_cities_dim,create_airports_dim,create_vistior_dim,create_airline_dim]
drop_tables=[drop_immigrations,drop_temp_dim,drop_vistior_dim,drop_cities_dim,drop_airline_dim,drop_airports_dim]
