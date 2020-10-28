# This script was created to help merge two APC reports.
# The program reads in weekday, Saturday and Sunday files
# Then creates a unique field to join on
# Calculates a weighted average for the values
# Cleans up the data
# And writes it to a csv

import pandas as pd
import numpy as np

# pd.options.display.max_columns = 999 # if using in a jupyter notebook to check outputs

# read in data
wkdy1 = pd.read_excel('stop_summary_wkdy_20200201_20200324.xlsx')
sat1 = pd.read_excel('stop_summary_sat_20200201_20200324.xlsx')
sun1 = pd.read_excel('stop_summary_sun_20200201_20200324.xlsx')

wkdy2 = pd.read_excel('stop_summary_wkdy_20200325_20200430.xlsx')
sat2 = pd.read_excel('stop_summary_sat_20200325_20200430.xlsx')
sun2 = pd.read_excel('stop_summary_sun_20200325_20200430.xlsx')

# combine the wkdy, sat and sun for each session
spring1 = pd.concat([wkdy1, sat1, sun1])
spring2 = pd.concat([wkdy2, sat2, sun2])

# create unique id field to join the dataset on
spring1['join_field1'] = spring1['DAY_OF_WEEK'] + spring1['ROUTE'].astype('str') + spring1['DIR'] + spring1['TRIP_TIME'].astype('str') + spring1['UNIQUE_STOP_NO'].astype('str') + spring1['SEQUENTIAL_STOP_NO'].astype('str')
spring2['join_field2'] = spring2['DAY_OF_WEEK'] + spring2['ROUTE'].astype('str') + spring2['DIR'] + spring2['TRIP_TIME'].astype('str') + spring2['UNIQUE_STOP_NO'].astype('str') + spring2['SEQUENTIAL_STOP_NO'].astype('str')

# rename columns for 'ON', 'OFF' and 'LOAD' for each session
spring1.rename({
        'ON': 'ON_spring1',
        'OFF': 'OFF_spring1',
        'LOAD': 'LOAD_spring1',
    }, axis=1, inplace=True)
spring2.rename({
        'ON': 'ON_spring2',
        'OFF': 'OFF_spring2',
        'LOAD': 'LOAD_spring2',
    }, axis=1, inplace=True)
    
# join the two dataframes together
spring = spring1.merge(spring2,
                       how='outer',
                       left_on='join_field1',
                       right_on='join_field2')
                       
# set variables for number of days in each session
session1_wkdys = 37
session1_sats = 8
session1_suns = 8

session2_wkdys = 26
session2_sats = 5
session2_suns = 5

total_wkdys = session1_wkdys + session2_wkdys
total_sats = session1_sats + session2_sats
total_suns = session1_suns + session2_suns

weighting_conditions = [
        (spring['DAY_OF_WEEK_x'].eq('WKDY') | spring['DAY_OF_WEEK_x'].eq('SAT') | spring['DAY_OF_WEEK_x'].eq('SUN')) & (spring['DAY_OF_WEEK_y'].isnull()),
        (spring['DAY_OF_WEEK_y'].eq('WKDY') | spring['DAY_OF_WEEK_y'].eq('SAT') | spring['DAY_OF_WEEK_y'].eq('SUN')) & (spring['DAY_OF_WEEK_x'].isnull()),
        (spring['DAY_OF_WEEK_x'].eq('WKDY')) & (spring['DAY_OF_WEEK_y'].eq('WKDY')),
        (spring['DAY_OF_WEEK_x'].eq('SAT')) & (spring['DAY_OF_WEEK_y'].eq('SAT')),
        (spring['DAY_OF_WEEK_x'].eq('SUN')) & (spring['DAY_OF_WEEK_y'].eq('SUN'))
]

ON_choices = [
    round(spring['ON_spring1'], 2),
    round(spring['ON_spring2'], 2),
    round((spring['ON_spring1'] * session1_wkdys + spring['ON_spring2'] * session2_wkdys) / total_wkdys, 2),
    round((spring['ON_spring1'] * session1_sats + spring['ON_spring2'] * session2_sats) / total_sats, 2),
    round((spring['ON_spring1'] * session1_suns + spring['ON_spring2'] * session2_suns) / total_suns, 2)
]

OFF_choices = [
    round(spring['OFF_spring1'], 2),
    round(spring['OFF_spring2'], 2),
    round((spring['OFF_spring1'] * session1_wkdys + spring['OFF_spring2'] * session2_wkdys) / total_wkdys, 2),
    round((spring['OFF_spring1'] * session1_sats + spring['OFF_spring2'] * session2_sats) / total_sats, 2),
    round((spring['OFF_spring1'] * session1_suns + spring['OFF_spring2'] * session2_suns) / total_suns, 2)
]

LOAD_choices = [
    round(spring['LOAD_spring1'], 2),
    round(spring['LOAD_spring2'], 2),
    round((spring['LOAD_spring1'] * session1_wkdys + spring['LOAD_spring2'] * session2_wkdys) / total_wkdys, 2),
    round((spring['LOAD_spring1'] * session1_sats + spring['LOAD_spring2'] * session2_sats) / total_sats, 2),
    round((spring['LOAD_spring1'] * session1_suns + spring['LOAD_spring2'] * session2_suns) / total_suns, 2)
]

spring['ON_final'] = np.select(weighting_conditions, ON_choices)
spring['OFF_final'] = np.select(weighting_conditions, OFF_choices)
spring['LOAD_final'] = np.select(weighting_conditions, LOAD_choices)

# replace missing values
spring['DAY_OF_WEEK_x'].fillna(spring['DAY_OF_WEEK_y'], inplace=True)
spring['ROUTE_x'].fillna(spring['ROUTE_y'], inplace=True)
spring['DIR_x'].fillna(spring['DIR_y'], inplace=True)
spring['TRIP_TIME_x'].fillna(spring['TRIP_TIME_y'], inplace=True)
spring['PATTERN_x'].fillna(spring['PATTERN_y'], inplace=True)
spring['UNIQUE_STOP_NO_x'].fillna(spring['UNIQUE_STOP_NO_y'], inplace=True)
spring['SEQUENTIAL_STOP_NO_x'].fillna(spring['SEQUENTIAL_STOP_NO_y'], inplace=True)
spring['STOPNAME_x'].fillna(spring['STOPNAME_y'], inplace=True)
spring['LAT_x'].fillna(spring['LAT_y'], inplace=True)
spring['LONG_x'].fillna(spring['LONG_y'], inplace=True)
spring['SAMPLES_x'].fillna(spring['SAMPLES_y'], inplace=True)

# drop redundant columns
spring.drop([
    'ON_spring1',
    'OFF_spring1',
    'LOAD_spring1',
    'join_field1',
    'DAY_OF_WEEK_y',
    'ROUTE_y',
    'DIR_y',
    'TRIP_TIME_y',
    'PATTERN_y',
    'UNIQUE_STOP_NO_y',
    'SEQUENTIAL_STOP_NO_y',
    'STOPNAME_y',
    'STOPNAME_x',
    'LAT_x',
    'LONG_x',
    'ON_spring2',
    'OFF_spring2',
    'LOAD_spring2',
    'LAT_y',
    'LONG_y',
    'SAMPLES_y',
    'join_field2'
], axis=1, inplace=True)

# rename columns of interest
spring.rename({
    'DAY_OF_WEEK_x': 'DAY_OF_WEEK',
    'ROUTE_x': 'ROUTE',
    'DIR_x': 'DIR',
    'TRIP_TIME_x': 'TRIP_TIME',
    'PATTERN_x': 'PATTERN',
    'UNIQUE_STOP_NO_x': 'STOP_CODE',
    'SEQUENTIAL_STOP_NO_x': 'STOP_SEQUENCE',
    'SAMPLES_x': 'SAMPLES',
    'ON_final': 'ON',
    'OFF_final': 'OFF',
    'LOAD_final': 'LOAD'
}, axis=1, inplace=True)

# bring in stop code master to update stop names in dataframe
stop_code_master = pd.read_excel('W:\COMMUTER OPS - OPERATIONS & FACILITIES\PART Express Operations\Routes Schedules & Changes\Stop Codes\Stop Code Master.xlsx',
                                 skiprows=1, usecols=[0,1,2,3,4])
stop_code_master = stop_code_master[stop_code_master['stop_name'].notna()]

# join stop code master to dataframe
spring = spring.merge(stop_code_master,
                      how='inner',
                      left_on='STOP_CODE',
                      right_on='stop_code')
                      
# drop redundant columns
spring.drop(['PATTERN', 'STOP_CODE', 'stop_desc'], axis=1, inplace=True)

# add route long name to dataframe
route_long_name = {
    1:  '1 - Winston-Salem Express',
    2:  '2 - Greensboro Express',
    3:  '3 - High Point Express',
    4:  '4 - Alamance-Burlington Express',
    5:  '5 - NC Amtrak Connector',
    6:  '6 - Surry County Express',
    9:  '9 - Davidson Business 85 Express',
    10: '10 - Randolph Express',
    17: '17 - Kernersville Express',
    19: '19 - Palladium Circulator',
    20: '20 - NW Pleasant Ridge',
    21: '21 - NE Chimney Rock',
    22: '22 - SW Sandy Ridge',
    23: '23 - SE Piedmont Parkway',
    27: '27 - Airport Area',
    28: '28 - West Forsyth Express'
}
spring['route_long_name'] = spring['ROUTE'].map(route_long_name)

# calculate service period
service_conditions = [
    spring['TRIP_TIME'] >= 1800,
    spring['TRIP_TIME'] >= 1500,
    spring['TRIP_TIME'] >= 900
]
service_choices = ['Evening', 'PM Peak', 'Off Peak']
spring['SERVICE_PERIOD'] = np.select(service_conditions, service_choices, default='AM Peak')

# create and format a new trip time attribute
format_time_conditions = [
    spring['TRIP_TIME'].astype(str).str.len() == 5,
    spring['TRIP_TIME'].astype(str).str.len() == 6
]

format_time_choices = [
    '0' + spring['TRIP_TIME'].astype(str).str.slice(start=0, stop=1) + ':' + spring['TRIP_TIME'].astype(str).str.slice(start=1, stop=3),
    spring['TRIP_TIME'].astype(str).str.slice(start=0, stop=2) + ':' + spring['TRIP_TIME'].astype(str).str.slice(start=2, stop=4)
]

spring['USE_TIME'] = np.select(format_time_conditions, format_time_choices)

# write data to csv
spring.to_csv('stop_summary_spring2020.csv', index=False)
