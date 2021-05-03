# Miguel Fernandez
# 10/28/2020

# This script will merge three APC stop summary reports
# You will need to provide the path to the files
# As well as the number of service days in each session

import pandas as pd
import numpy as np

# pd.options.display.max_columns = 999 #for displaying results in Jupyter Notebook

path = r'D:\\APC\\Miguel_APC\\Spring 2021\\'

# read in data
wkdy1 = pd.read_excel(path + 'stop_summary_wkdy_20210201_20210228.xlsx')
sat1 = pd.read_excel(path + 'stop_summary_sat_20210201_20210228.xlsx')

wkdy2 = pd.read_excel(path + 'stop_summary_wkdy_20210301_20210404.xlsx')
sat2 = pd.read_excel(path + 'stop_summary_sat_20210301_20210404.xlsx')

wkdy3 = pd.read_excel(path + 'stop_summary_wkdy_20210405_20210430.xlsx')
sat3 = pd.read_excel(path + 'stop_summary_sat_20210405_20210430.xlsx')

# combine the wkdy and sat for each session
session1 = pd.concat([wkdy1, sat1])
session2 = pd.concat([wkdy2, sat2])
session3 = pd.concat([wkdy3, sat3])

# create unique id field to join the dataset on
session1['join_field1'] = session1['DAY_OF_WEEK'] + session1['ROUTE'].astype('str') + session1['DIR'] + session1[
    'TRIP_TIME'].astype('str') + session1['UNIQUE_STOP_NO'].astype('str') + session1['SEQUENTIAL_STOP_NO'].astype('str')
session2['join_field2'] = session2['DAY_OF_WEEK'] + session2['ROUTE'].astype('str') + session2['DIR'] + session2[
    'TRIP_TIME'].astype('str') + session2['UNIQUE_STOP_NO'].astype('str') + session2['SEQUENTIAL_STOP_NO'].astype('str')
session3['join_field3'] = session3['DAY_OF_WEEK'] + session3['ROUTE'].astype('str') + session3['DIR'] + session3[
    'TRIP_TIME'].astype('str') + session3['UNIQUE_STOP_NO'].astype('str') + session3['SEQUENTIAL_STOP_NO'].astype('str')

# rename columns for 'ON', 'OFF' and 'LOAD' for each session
session1.rename({
    'DAY_OF_WEEK': 'DAY_OF_WEEK_session1',
    'ON': 'ON_session1',
    'OFF': 'OFF_session1',
    'LOAD': 'LOAD_session1',
}, axis=1, inplace=True)

session2.rename({
    'DAY_OF_WEEK': 'DAY_OF_WEEK_session2',
    'ROUTE': 'ROUTE_session2',
    'DIR': 'DIR_session2',
    'TRIP_TIME': 'TRIP_TIME_session2',
    'PATTERN': 'PATTERN_session2',
    'UNIQUE_STOP_NO': 'UNIQUE_STOP_NO_session2',
    'SEQUENTIAL_STOP_NO': 'SEQUENTIAL_STOP_NO_session2',
    'STOPNAME': 'STOPNAME_session2',
    'LAT': 'LAT_session2',
    'LONG': 'LONG_session2',
    'SAMPLES': 'SAMPLES_session2',
    'ON': 'ON_session2',
    'OFF': 'OFF_session2',
    'LOAD': 'LOAD_session2',
}, axis=1, inplace=True)

session3.rename({
    'DAY_OF_WEEK': 'DAY_OF_WEEK_session3',
    'ROUTE': 'ROUTE_session3',
    'DIR': 'DIR_session3',
    'TRIP_TIME': 'TRIP_TIME_session3',
    'PATTERN': 'PATTERN_session3',
    'UNIQUE_STOP_NO': 'UNIQUE_STOP_NO_session3',
    'SEQUENTIAL_STOP_NO': 'SEQUENTIAL_STOP_NO_session3',
    'STOPNAME': 'STOPNAME_session3',
    'LAT': 'LAT_session3',
    'LONG': 'LONG_session3',
    'SAMPLES': 'SAMPLES_session3',
    'ON': 'ON_session3',
    'OFF': 'OFF_session3',
    'LOAD': 'LOAD_session3',
}, axis=1, inplace=True)

# setup dataframes to be merged
session1 = session1.set_index('join_field1')
session2 = session2.set_index('join_field2')
session3 = session3.set_index('join_field3')

# join three dataframes
session = pd.concat([session1, session2, session3], axis=1, sort=False)

# set variables for number of days in each session
session1_wkdys = 20
session1_sats = 4

session2_wkdys = 24
session2_sats = 5

session3_wkdys = 20
session3_sats = 3

total_session1session2_wkdys = session1_wkdys + session2_wkdys
total_session1session2_sats = session1_sats + session2_sats

total_session1session3_wkdys = session1_wkdys + session3_wkdys
total_session1session3_sats = session1_sats + session3_sats

total_session2session3_wkdys = session2_wkdys + session3_wkdys
total_session2session3_sats = session2_sats + session3_sats

total_wkdys = session1_wkdys + session2_wkdys + session3_wkdys
total_sats = session1_sats + session2_sats + session3_sats

weighting_conditions = [
    session['DAY_OF_WEEK_session1'].isnull() & session['DAY_OF_WEEK_session2'].isnull() & session[
        'DAY_OF_WEEK_session3'].notnull(),
    session['DAY_OF_WEEK_session1'].isnull() & session['DAY_OF_WEEK_session2'].notnull() & session[
        'DAY_OF_WEEK_session3'].isnull(),
    session['DAY_OF_WEEK_session1'].notnull() & session['DAY_OF_WEEK_session2'].isnull() & session[
        'DAY_OF_WEEK_session3'].isnull(),
    session['DAY_OF_WEEK_session1'].isnull() & session['DAY_OF_WEEK_session2'].eq('WKDY') & session[
        'DAY_OF_WEEK_session3'].eq('WKDY'),
    session['DAY_OF_WEEK_session1'].isnull() & session['DAY_OF_WEEK_session2'].eq('SAT') & session[
        'DAY_OF_WEEK_session3'].eq('SAT'),
    session['DAY_OF_WEEK_session1'].eq('WKDY') & session['DAY_OF_WEEK_session2'].isnull() & session[
        'DAY_OF_WEEK_session3'].eq('WKDY'),
    session['DAY_OF_WEEK_session1'].eq('SAT') & session['DAY_OF_WEEK_session2'].isnull() & session[
        'DAY_OF_WEEK_session3'].eq('SAT'),
    session['DAY_OF_WEEK_session1'].eq('WKDY') & session['DAY_OF_WEEK_session2'].eq('WKDY') & session[
        'DAY_OF_WEEK_session3'].isnull(),
    session['DAY_OF_WEEK_session1'].eq('SAT') & session['DAY_OF_WEEK_session2'].eq('SAT') & session[
        'DAY_OF_WEEK_session3'].isnull(),
    session['DAY_OF_WEEK_session1'].eq('WKDY') & session['DAY_OF_WEEK_session2'].eq('WKDY') & session[
        'DAY_OF_WEEK_session3'].eq('WKDY'),
    session['DAY_OF_WEEK_session1'].eq('SAT') & session['DAY_OF_WEEK_session2'].eq('SAT') & session[
        'DAY_OF_WEEK_session3'].eq('SAT')
]

ON_choices = [
    round(session['ON_session3'], 2),
    round(session['ON_session2'], 2),
    round(session['ON_session1'], 2),
    round(
        (session['ON_session2'] * session2_wkdys + session['ON_session3'] * session3_wkdys) / total_session2session3_wkdys,
        2),
    round((session['ON_session2'] * session2_sats + session['ON_session3'] * session3_sats) / total_session2session3_sats,
          2),
    round(
        (session['ON_session1'] * session1_wkdys + session['ON_session3'] * session3_wkdys) / total_session1session3_wkdys,
        2),
    round((session['ON_session1'] * session1_sats + session['ON_session3'] * session3_sats) / total_session1session3_sats,
          2),
    round(
        (session['ON_session1'] * session1_wkdys + session['ON_session2'] * session2_wkdys) / total_session1session2_wkdys,
        2),
    round((session['ON_session1'] * session1_sats + session['ON_session2'] * session2_sats) / total_session1session2_sats,
          2),
    round((session['ON_session1'] * session1_wkdys + session['ON_session2'] * session2_wkdys + session[
        'ON_session3'] * session3_wkdys) / total_wkdys, 2),
    round((session['ON_session1'] * session1_sats + session['ON_session2'] * session2_sats + session[
        'ON_session3'] * session3_sats) / total_sats, 2)
]

OFF_choices = [
    round(session['OFF_session3'], 2),
    round(session['OFF_session2'], 2),
    round(session['OFF_session1'], 2),
    round((session['OFF_session2'] * session2_wkdys + session[
        'OFF_session3'] * session3_wkdys) / total_session2session3_wkdys, 2),
    round((session['OFF_session2'] * session2_sats + session['OFF_session3'] * session3_sats) / total_session2session3_sats,
          2),
    round((session['OFF_session1'] * session1_wkdys + session[
        'OFF_session3'] * session3_wkdys) / total_session1session3_wkdys, 2),
    round((session['OFF_session1'] * session1_sats + session['OFF_session3'] * session3_sats) / total_session1session3_sats,
          2),
    round((session['OFF_session1'] * session1_wkdys + session[
        'OFF_session2'] * session2_wkdys) / total_session1session2_wkdys, 2),
    round((session['OFF_session1'] * session1_sats + session['OFF_session2'] * session2_sats) / total_session1session2_sats,
          2),
    round((session['OFF_session1'] * session1_wkdys + session['OFF_session2'] * session2_wkdys + session[
        'OFF_session3'] * session3_wkdys) / total_wkdys, 2),
    round((session['OFF_session1'] * session1_sats + session['OFF_session2'] * session2_sats + session[
        'OFF_session3'] * session3_sats) / total_sats, 2)
]

LOAD_choices = [
    round(session['LOAD_session3'], 2),
    round(session['LOAD_session2'], 2),
    round(session['LOAD_session1'], 2),
    round((session['LOAD_session2'] * session2_wkdys + session[
        'LOAD_session3'] * session3_wkdys) / total_session2session3_wkdys, 2),
    round(
        (session['LOAD_session2'] * session2_sats + session['LOAD_session3'] * session3_sats) / total_session2session3_sats,
        2),
    round((session['LOAD_session1'] * session1_wkdys + session[
        'LOAD_session3'] * session3_wkdys) / total_session1session3_wkdys, 2),
    round(
        (session['LOAD_session1'] * session1_sats + session['LOAD_session3'] * session3_sats) / total_session1session3_sats,
        2),
    round((session['LOAD_session1'] * session1_wkdys + session[
        'LOAD_session2'] * session2_wkdys) / total_session1session2_wkdys, 2),
    round(
        (session['LOAD_session1'] * session1_sats + session['LOAD_session2'] * session2_sats) / total_session1session2_sats,
        2),
    round((session['LOAD_session1'] * session1_wkdys + session['LOAD_session2'] * session2_wkdys + session[
        'LOAD_session3'] * session3_wkdys) / total_wkdys, 2),
    round((session['LOAD_session1'] * session1_sats + session['LOAD_session2'] * session2_sats + session[
        'LOAD_session3'] * session3_sats) / total_sats, 2)
]

session['ON_final'] = np.select(weighting_conditions, ON_choices)
session['OFF_final'] = np.select(weighting_conditions, OFF_choices)
session['LOAD_final'] = np.select(weighting_conditions, LOAD_choices)

# fill in missing values
# not the prettiest way
session['DAY_OF_WEEK_session1'] = np.where(session['DAY_OF_WEEK_session1'].isnull(), session['DAY_OF_WEEK_session2'],
                                           session['DAY_OF_WEEK_session1'])
session['DAY_OF_WEEK_session1'] = np.where(session['DAY_OF_WEEK_session1'].isnull(), session['DAY_OF_WEEK_session3'],
                                           session['DAY_OF_WEEK_session1'])

session['ROUTE'] = np.where(session['ROUTE'].isnull(), session['ROUTE_session2'], session['ROUTE'])
session['ROUTE'] = np.where(session['ROUTE'].isnull(), session['ROUTE_session3'], session['ROUTE'])

session['DIR'] = np.where(session['DIR'].isnull(), session['DIR_session2'], session['DIR'])
session['DIR'] = np.where(session['DIR'].isnull(), session['DIR_session3'], session['DIR'])

session['TRIP_TIME'] = np.where(session['TRIP_TIME'].isnull(), session['TRIP_TIME_session2'], session['TRIP_TIME'])
session['TRIP_TIME'] = np.where(session['TRIP_TIME'].isnull(), session['TRIP_TIME_session3'], session['TRIP_TIME'])

session['PATTERN'] = np.where(session['PATTERN'].isnull(), session['PATTERN_session2'], session['PATTERN'])
session['PATTERN'] = np.where(session['PATTERN'].isnull(), session['PATTERN_session3'], session['PATTERN'])

session['UNIQUE_STOP_NO'] = np.where(session['UNIQUE_STOP_NO'].isnull(), session['UNIQUE_STOP_NO_session2'],
                                     session['UNIQUE_STOP_NO'])
session['UNIQUE_STOP_NO'] = np.where(session['UNIQUE_STOP_NO'].isnull(), session['UNIQUE_STOP_NO_session3'],
                                     session['UNIQUE_STOP_NO'])

session['SEQUENTIAL_STOP_NO'] = np.where(session['SEQUENTIAL_STOP_NO'].isnull(), session['SEQUENTIAL_STOP_NO_session2'],
                                         session['SEQUENTIAL_STOP_NO'])
session['SEQUENTIAL_STOP_NO'] = np.where(session['SEQUENTIAL_STOP_NO'].isnull(), session['SEQUENTIAL_STOP_NO_session3'],
                                         session['SEQUENTIAL_STOP_NO'])

session['SAMPLES'] = np.where(session['SAMPLES'].isnull(), session['SAMPLES_session2'], session['SAMPLES'])
session['SAMPLES'] = np.where(session['SAMPLES'].isnull(), session['SAMPLES_session3'], session['SAMPLES'])

# keep columns of interest
keep_cols = ['DAY_OF_WEEK_session1', 'ROUTE', 'DIR', 'TRIP_TIME', 'PATTERN', 'UNIQUE_STOP_NO',
             'SEQUENTIAL_STOP_NO', 'SAMPLES', 'ON_final', 'OFF_final', 'LOAD_final']

session = session[session.columns[session.columns.isin(keep_cols)]]

# rename columns
session.rename({
    'DAY_OF_WEEK_session1': 'DAY_OF_WEEK',
    'UNIQUE_STOP_NO': 'STOP_CODE',
    'SEQUENTIAL_STOP_NO': 'STOP_SEQUENCE',
    'ON_final': 'ON',
    'OFF_final': 'OFF',
    'LOAD_final': 'LOAD'
}, axis=1, inplace=True)

# bring in stop code master to update stop names in dataframe
stop_code_master = pd.read_excel(
    'W:\COMMUTER OPS - OPERATIONS & FACILITIES\PART Express Operations\Routes Schedules & Changes\Stop Codes\Stop Code Master.xlsx',
    skiprows=1, usecols=[0, 1, 2, 3, 4])
stop_code_master = stop_code_master[stop_code_master['stop_name'].notna()]

# join stop code master to dataframe
session = session.merge(
    stop_code_master,
    how='inner',
    left_on='STOP_CODE',
    right_on='stop_code'
)

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

session.to_csv(path + 'stop_summary_spring2021.csv', index=False)

