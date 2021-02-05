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
wkdy1 = pd.read_excel('stop_summary_wkdy_20201102_20210103.xlsx', engine='openpyxl')
sat1 = pd.read_excel('stop_summary_sat_20201102_20210103.xlsx', engine='openpyxl')
# sun1 = pd.read_excel('stop_summary_sun_20200201_20200324.xlsx')

wkdy2 = pd.read_excel('stop_summary_wkdy_20210104_20210130.xlsx', engine='openpyxl')
sat2 = pd.read_excel('stop_summary_sat_20210104_20210130.xlsx', engine='openpyxl')
# sun2 = pd.read_excel('stop_summary_sun_20200325_20200430.xlsx')

# combine the wkdy, sat and sun for each session
session1 = pd.concat([wkdy1, sat1])
session2 = pd.concat([wkdy2, sat2])

# create unique id field to join the dataset on
session1['join_field1'] = session1['DAY_OF_WEEK'] + session1['ROUTE'].astype('str') + session1['DIR'] + session1[
    'TRIP_TIME'].astype('str') + session1['UNIQUE_STOP_NO'].astype('str') + session1['SEQUENTIAL_STOP_NO'].astype('str')
session2['join_field2'] = session2['DAY_OF_WEEK'] + session2['ROUTE'].astype('str') + session2['DIR'] + session2[
    'TRIP_TIME'].astype('str') + session2['UNIQUE_STOP_NO'].astype('str') + session2['SEQUENTIAL_STOP_NO'].astype('str')

# rename columns for 'ON', 'OFF' and 'LOAD' for each session
session1.rename({
    'ON': 'ON_session1',
    'OFF': 'OFF_session1',
    'LOAD': 'LOAD_session1',
}, axis=1, inplace=True)
session2.rename({
    'ON': 'ON_session2',
    'OFF': 'OFF_session2',
    'LOAD': 'LOAD_session2',
}, axis=1, inplace=True)

# join the two dataframes together
session = session1.merge(session2,
                         how='outer',
                         left_on='join_field1',
                         right_on='join_field2')

# set variables for number of days in each session
session1_wkdys = 42
session1_sats = 9
# session1_suns = 8

session2_wkdys = 20
session2_sats = 4
# session2_suns = 5

total_wkdys = session1_wkdys + session2_wkdys
total_sats = session1_sats + session2_sats
# total_suns = session1_suns + session2_suns

weighting_conditions = [
    (session['DAY_OF_WEEK_x'].eq('WKDY') | session['DAY_OF_WEEK_x'].eq('SAT') | session['DAY_OF_WEEK_x'].eq('SUN')) & (
        session['DAY_OF_WEEK_y'].isnull()),
    (session['DAY_OF_WEEK_y'].eq('WKDY') | session['DAY_OF_WEEK_y'].eq('SAT') | session['DAY_OF_WEEK_y'].eq('SUN')) & (
        session['DAY_OF_WEEK_x'].isnull()),
    (session['DAY_OF_WEEK_x'].eq('WKDY')) & (session['DAY_OF_WEEK_y'].eq('WKDY')),
    (session['DAY_OF_WEEK_x'].eq('SAT')) & (session['DAY_OF_WEEK_y'].eq('SAT'))
    #(session['DAY_OF_WEEK_x'].eq('SUN')) & (session['DAY_OF_WEEK_y'].eq('SUN'))
]

ON_choices = [
    round(session['ON_session1'], 2),
    round(session['ON_session2'], 2),
    round((session['ON_session1'] * session1_wkdys + session['ON_session2'] * session2_wkdys) / total_wkdys, 2),
    round((session['ON_session1'] * session1_sats + session['ON_session2'] * session2_sats) / total_sats, 2)
    #round((session['ON_spring1'] * session1_suns + session['ON_spring2'] * session2_suns) / total_suns, 2)
]

OFF_choices = [
    round(session['OFF_session1'], 2),
    round(session['OFF_session2'], 2),
    round((session['OFF_session1'] * session1_wkdys + session['OFF_session2'] * session2_wkdys) / total_wkdys, 2),
    round((session['OFF_session1'] * session1_sats + session['OFF_session2'] * session2_sats) / total_sats, 2)
    #round((session['OFF_spring1'] * session1_suns + session['OFF_spring2'] * session2_suns) / total_suns, 2)
]

LOAD_choices = [
    round(session['LOAD_session1'], 2),
    round(session['LOAD_session2'], 2),
    round((session['LOAD_session1'] * session1_wkdys + session['LOAD_session2'] * session2_wkdys) / total_wkdys, 2),
    round((session['LOAD_session1'] * session1_sats + session['LOAD_session2'] * session2_sats) / total_sats, 2)
    #round((session['LOAD_spring1'] * session1_suns + session['LOAD_spring2'] * session2_suns) / total_suns, 2)
]

session['ON_final'] = np.select(weighting_conditions, ON_choices)
session['OFF_final'] = np.select(weighting_conditions, OFF_choices)
session['LOAD_final'] = np.select(weighting_conditions, LOAD_choices)

# replace missing values
session['DAY_OF_WEEK_x'].fillna(session['DAY_OF_WEEK_y'], inplace=True)
session['ROUTE_x'].fillna(session['ROUTE_y'], inplace=True)
session['DIR_x'].fillna(session['DIR_y'], inplace=True)
session['TRIP_TIME_x'].fillna(session['TRIP_TIME_y'], inplace=True)
session['PATTERN_x'].fillna(session['PATTERN_y'], inplace=True)
session['UNIQUE_STOP_NO_x'].fillna(session['UNIQUE_STOP_NO_y'], inplace=True)
session['SEQUENTIAL_STOP_NO_x'].fillna(session['SEQUENTIAL_STOP_NO_y'], inplace=True)
session['STOPNAME_x'].fillna(session['STOPNAME_y'], inplace=True)
session['LAT_x'].fillna(session['LAT_y'], inplace=True)
session['LONG_x'].fillna(session['LONG_y'], inplace=True)
session['SAMPLES_x'].fillna(session['SAMPLES_y'], inplace=True)

# drop redundant columns
session.drop([
    'ON_session1',
    'OFF_session1',
    'LOAD_session1',
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
    'ON_session2',
    'OFF_session2',
    'LOAD_session2',
    'LAT_y',
    'LONG_y',
    'SAMPLES_y',
    'join_field2'
], axis=1, inplace=True)

# rename columns of interest
session.rename({
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
stop_code_master = pd.read_excel(
    'W:\COMMUTER OPS - OPERATIONS & FACILITIES\PART Express Operations\Routes Schedules & Changes\Stop Codes\Stop Code Master.xlsx',
    skiprows=1, usecols=[0, 1, 2, 3, 4], engine='openpyxl')
stop_code_master = stop_code_master[stop_code_master['stop_name'].notna()]

# join stop code master to dataframe
session = session.merge(stop_code_master,
                        how='inner',
                        left_on='STOP_CODE',
                        right_on='stop_code')

# drop redundant columns
session.drop(['PATTERN', 'STOP_CODE', 'stop_desc'], axis=1, inplace=True)

# add route long name to dataframe
route_long_name = {
    1: '1 - Winston-Salem Express',
    2: '2 - Greensboro Express',
    3: '3 - High Point Express',
    4: '4 - Alamance-Burlington Express',
    5: '5 - NC Amtrak Connector',
    6: '6 - Surry County Express',
    9: '9 - Davidson Business 85 Express',
    10: '10 - Randolph Express',
    17: '17 - Kernersville Express',
    19: '19 - Palladium Circulator',
    20: '20 - NW Pleasant Ridge',
    21: '21 - NE Chimney Rock',
    22: '22 - SW Sandy Ridge',
    23: '23 - SE Piedmont Parkway',
    24: '24 - Burgess/Regional Rd',
    27: '27 - Airport Area',
    28: '28 - West Forsyth Express'
}
session['route_long_name'] = session['ROUTE'].map(route_long_name)

# calculate service period
service_conditions = [
    session['TRIP_TIME'] >= 1800,
    session['TRIP_TIME'] >= 1500,
    session['TRIP_TIME'] >= 900
]
service_choices = ['Evening', 'PM Peak', 'Off Peak']
session['SERVICE_PERIOD'] = np.select(service_conditions, service_choices, default='AM Peak')

# create and format a new trip time attribute
format_time_conditions = [
    session['TRIP_TIME'].astype(str).str.len() == 5,
    session['TRIP_TIME'].astype(str).str.len() == 6
]

format_time_choices = [
    '0' + session['TRIP_TIME'].astype(str).str.slice(start=0, stop=1) + ':' + session['TRIP_TIME'].astype(str).str.slice(
        start=1, stop=3),
    session['TRIP_TIME'].astype(str).str.slice(start=0, stop=2) + ':' + session['TRIP_TIME'].astype(str).str.slice(
        start=2, stop=4)
]

session['USE_TIME'] = np.select(format_time_conditions, format_time_choices)

# write data to csv
session.to_csv('stop_summary_holidays2020-21.csv', index=False)
