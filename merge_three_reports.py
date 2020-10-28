# Miguel Fernandez
# 10/28/2020

# This script will merge three APC stop summary reports
# You will need to provide the path to the files
# As well as the number of service days in each session

import pandas as pd
import numpy as np

#pd.options.display.max_columns = 999 #for displaying results in Jupyter Notebook


# read in data
wkdy1 = pd.read_excel('stop_summary_wkdy_20200501_20200614.xlsx')
sat1 = pd.read_excel('stop_summary_sat_20200501_20200614.xlsx')

wkdy2 = pd.read_excel('stop_summary_wkdy_20200615_20200705.xlsx')
sat2 = pd.read_excel('stop_summary_sat_20200615_20200705.xlsx')

wkdy3 = pd.read_excel('stop_summary_wkdy_20200706_20200731.xlsx')
sat3 = pd.read_excel('stop_summary_sat_20200706_20200731.xlsx')


# combine the wkdy and sat for each session
summer1 = pd.concat([wkdy1, sat1])
summer2 = pd.concat([wkdy2, sat2])
summer3 = pd.concat([wkdy3, sat3])


# create unique id field to join the dataset on
summer1['join_field1'] = summer1['DAY_OF_WEEK'] + summer1['ROUTE'].astype('str') + summer1['DIR'] + summer1['TRIP_TIME'].astype('str') + summer1['UNIQUE_STOP_NO'].astype('str') + summer1['SEQUENTIAL_STOP_NO'].astype('str')
summer2['join_field2'] = summer2['DAY_OF_WEEK'] + summer2['ROUTE'].astype('str') + summer2['DIR'] + summer2['TRIP_TIME'].astype('str') + summer2['UNIQUE_STOP_NO'].astype('str') + summer2['SEQUENTIAL_STOP_NO'].astype('str')
summer3['join_field3'] = summer3['DAY_OF_WEEK'] + summer3['ROUTE'].astype('str') + summer3['DIR'] + summer3['TRIP_TIME'].astype('str') + summer3['UNIQUE_STOP_NO'].astype('str') + summer3['SEQUENTIAL_STOP_NO'].astype('str')


# rename columns for 'ON', 'OFF' and 'LOAD' for each session
summer1.rename({
        'DAY_OF_WEEK': 'DAY_OF_WEEK_summer1',
        'ON': 'ON_summer1',
        'OFF': 'OFF_summer1',
        'LOAD': 'LOAD_summer1',
    }, axis=1, inplace=True)

summer2.rename({
        'DAY_OF_WEEK': 'DAY_OF_WEEK_summer2',
        'ROUTE': 'ROUTE_summer2',
        'DIR': 'DIR_summer2',
        'TRIP_TIME': 'TRIP_TIME_summer2',
        'PATTERN': 'PATTERN_summer2',
        'UNIQUE_STOP_NO': 'UNIQUE_STOP_NO_summer2',
        'SEQUENTIAL_STOP_NO': 'SEQUENTIAL_STOP_NO_summer2',
        'STOPNAME': 'STOPNAME_summer2',
        'LAT': 'LAT_summer2',
        'LONG': 'LONG_summer2',
        'SAMPLES': 'SAMPLES_summer2',
        'ON': 'ON_summer2',
        'OFF': 'OFF_summer2',
        'LOAD': 'LOAD_summer2',
    }, axis=1, inplace=True)

summer3.rename({
        'DAY_OF_WEEK': 'DAY_OF_WEEK_summer3',
        'ROUTE': 'ROUTE_summer3',
        'DIR': 'DIR_summer3',
        'TRIP_TIME': 'TRIP_TIME_summer3',
        'PATTERN': 'PATTERN_summer3',
        'UNIQUE_STOP_NO': 'UNIQUE_STOP_NO_summer3',
        'SEQUENTIAL_STOP_NO': 'SEQUENTIAL_STOP_NO_summer3',
        'STOPNAME': 'STOPNAME_summer3',
        'LAT': 'LAT_summer3',
        'LONG': 'LONG_summer3',
        'SAMPLES': 'SAMPLES_summer3',
        'ON': 'ON_summer3',
        'OFF': 'OFF_summer3',
        'LOAD': 'LOAD_summer3',
    }, axis=1, inplace=True)
    
    
# setup dataframes to be merged
summer1 = summer1.set_index('join_field1')
summer2 = summer2.set_index('join_field2')
summer3 = summer3.set_index('join_field3')

# join three dataframes
summer = pd.concat([summer1, summer2, summer3], axis=1, sort=False)


# set variables for number of days in each session
session1_wkdys = 30
session1_sats = 7

session2_wkdys = 15
session2_sats = 2

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
        summer['DAY_OF_WEEK_summer1'].isnull() & summer['DAY_OF_WEEK_summer2'].isnull() & summer['DAY_OF_WEEK_summer3'].notnull(),
        summer['DAY_OF_WEEK_summer1'].isnull() & summer['DAY_OF_WEEK_summer2'].notnull() & summer['DAY_OF_WEEK_summer3'].isnull(),
        summer['DAY_OF_WEEK_summer1'].notnull() & summer['DAY_OF_WEEK_summer2'].isnull() & summer['DAY_OF_WEEK_summer3'].isnull(),
        summer['DAY_OF_WEEK_summer1'].isnull() & summer['DAY_OF_WEEK_summer2'].eq('WKDY') & summer['DAY_OF_WEEK_summer3'].eq('WKDY'),
        summer['DAY_OF_WEEK_summer1'].isnull() & summer['DAY_OF_WEEK_summer2'].eq('SAT') & summer['DAY_OF_WEEK_summer3'].eq('SAT'),
        summer['DAY_OF_WEEK_summer1'].eq('WKDY') & summer['DAY_OF_WEEK_summer2'].isnull() & summer['DAY_OF_WEEK_summer3'].eq('WKDY'),
        summer['DAY_OF_WEEK_summer1'].eq('SAT') & summer['DAY_OF_WEEK_summer2'].isnull() & summer['DAY_OF_WEEK_summer3'].eq('SAT'),
        summer['DAY_OF_WEEK_summer1'].eq('WKDY') & summer['DAY_OF_WEEK_summer2'].eq('WKDY') & summer['DAY_OF_WEEK_summer3'].isnull(),
        summer['DAY_OF_WEEK_summer1'].eq('SAT') & summer['DAY_OF_WEEK_summer2'].eq('SAT') & summer['DAY_OF_WEEK_summer3'].isnull(),
        summer['DAY_OF_WEEK_summer1'].eq('WKDY') & summer['DAY_OF_WEEK_summer2'].eq('WKDY') & summer['DAY_OF_WEEK_summer3'].eq('WKDY'),
        summer['DAY_OF_WEEK_summer1'].eq('SAT') & summer['DAY_OF_WEEK_summer2'].eq('SAT') & summer['DAY_OF_WEEK_summer3'].eq('SAT')
]


ON_choices = [
        round(summer['ON_summer3'], 2),
        round(summer['ON_summer2'], 2),
        round(summer['ON_summer1'], 2),
        round((summer['ON_summer2'] * session2_wkdys + summer['ON_summer3'] * session3_wkdys) / total_session2session3_wkdys, 2),
        round((summer['ON_summer2'] * session2_sats + summer['ON_summer3'] * session3_sats) / total_session2session3_sats, 2),
        round((summer['ON_summer1'] * session1_wkdys + summer['ON_summer3'] * session3_wkdys) / total_session1session3_wkdys, 2),
        round((summer['ON_summer1'] * session1_sats + summer['ON_summer3'] * session3_sats) / total_session1session3_sats, 2),
        round((summer['ON_summer1'] * session1_wkdys + summer['ON_summer2'] * session2_wkdys) / total_session1session2_wkdys, 2),
        round((summer['ON_summer1'] * session1_sats + summer['ON_summer2'] * session2_sats) / total_session1session2_sats, 2),
        round((summer['ON_summer1'] * session1_wkdys + summer['ON_summer2'] * session2_wkdys + summer['ON_summer3'] * session3_wkdys) / total_wkdys, 2),
        round((summer['ON_summer1'] * session1_sats + summer['ON_summer2'] * session2_sats + summer['ON_summer3'] * session3_sats) / total_sats, 2)
]

OFF_choices = [
        round(summer['OFF_summer3'], 2),
        round(summer['OFF_summer2'], 2),
        round(summer['OFF_summer1'], 2),
        round((summer['OFF_summer2'] * session2_wkdys + summer['OFF_summer3'] * session3_wkdys) / total_session2session3_wkdys, 2),
        round((summer['OFF_summer2'] * session2_sats + summer['OFF_summer3'] * session3_sats) / total_session2session3_sats, 2),
        round((summer['OFF_summer1'] * session1_wkdys + summer['OFF_summer3'] * session3_wkdys) / total_session1session3_wkdys, 2),
        round((summer['OFF_summer1'] * session1_sats + summer['OFF_summer3'] * session3_sats) / total_session1session3_sats, 2),
        round((summer['OFF_summer1'] * session1_wkdys + summer['OFF_summer2'] * session2_wkdys) / total_session1session2_wkdys, 2),
        round((summer['OFF_summer1'] * session1_sats + summer['OFF_summer2'] * session2_sats) / total_session1session2_sats, 2),
        round((summer['OFF_summer1'] * session1_wkdys + summer['OFF_summer2'] * session2_wkdys + summer['OFF_summer3'] * session3_wkdys) / total_wkdys, 2),
        round((summer['OFF_summer1'] * session1_sats + summer['OFF_summer2'] * session2_sats + summer['OFF_summer3'] * session3_sats) / total_sats, 2)
]

LOAD_choices = [
        round(summer['LOAD_summer3'], 2),
        round(summer['LOAD_summer2'], 2),
        round(summer['LOAD_summer1'], 2),
        round((summer['LOAD_summer2'] * session2_wkdys + summer['LOAD_summer3'] * session3_wkdys) / total_session2session3_wkdys, 2),
        round((summer['LOAD_summer2'] * session2_sats + summer['LOAD_summer3'] * session3_sats) / total_session2session3_sats, 2),
        round((summer['LOAD_summer1'] * session1_wkdys + summer['LOAD_summer3'] * session3_wkdys) / total_session1session3_wkdys, 2),
        round((summer['LOAD_summer1'] * session1_sats + summer['LOAD_summer3'] * session3_sats) / total_session1session3_sats, 2),
        round((summer['LOAD_summer1'] * session1_wkdys + summer['LOAD_summer2'] * session2_wkdys) / total_session1session2_wkdys, 2),
        round((summer['LOAD_summer1'] * session1_sats + summer['LOAD_summer2'] * session2_sats) / total_session1session2_sats, 2),
        round((summer['LOAD_summer1'] * session1_wkdys + summer['LOAD_summer2'] * session2_wkdys + summer['LOAD_summer3'] * session3_wkdys) / total_wkdys, 2),
        round((summer['LOAD_summer1'] * session1_sats + summer['LOAD_summer2'] * session2_sats + summer['LOAD_summer3'] * session3_sats) / total_sats, 2)
]

summer['ON_final'] = np.select(weighting_conditions, ON_choices)
summer['OFF_final'] = np.select(weighting_conditions, OFF_choices)
summer['LOAD_final'] = np.select(weighting_conditions, LOAD_choices)


# fill in missing values
# not the prettiest way
summer['DAY_OF_WEEK_summer1'] = np.where(summer['DAY_OF_WEEK_summer1'].isnull(), summer['DAY_OF_WEEK_summer2'], summer['DAY_OF_WEEK_summer1'])
summer['DAY_OF_WEEK_summer1'] = np.where(summer['DAY_OF_WEEK_summer1'].isnull(), summer['DAY_OF_WEEK_summer3'], summer['DAY_OF_WEEK_summer1'])

summer['ROUTE'] = np.where(summer['ROUTE'].isnull(), summer['ROUTE_summer2'], summer['ROUTE'])
summer['ROUTE'] = np.where(summer['ROUTE'].isnull(), summer['ROUTE_summer3'], summer['ROUTE'])

summer['DIR'] = np.where(summer['DIR'].isnull(), summer['DIR_summer2'], summer['DIR'])
summer['DIR'] = np.where(summer['DIR'].isnull(), summer['DIR_summer3'], summer['DIR'])

summer['TRIP_TIME'] = np.where(summer['TRIP_TIME'].isnull(), summer['TRIP_TIME_summer2'], summer['TRIP_TIME'])
summer['TRIP_TIME'] = np.where(summer['TRIP_TIME'].isnull(), summer['TRIP_TIME_summer3'], summer['TRIP_TIME'])

summer['PATTERN'] = np.where(summer['PATTERN'].isnull(), summer['PATTERN_summer2'], summer['PATTERN'])
summer['PATTERN'] = np.where(summer['PATTERN'].isnull(), summer['PATTERN_summer3'], summer['PATTERN'])

summer['UNIQUE_STOP_NO'] = np.where(summer['UNIQUE_STOP_NO'].isnull(), summer['UNIQUE_STOP_NO_summer2'], summer['UNIQUE_STOP_NO'])
summer['UNIQUE_STOP_NO'] = np.where(summer['UNIQUE_STOP_NO'].isnull(), summer['UNIQUE_STOP_NO_summer3'], summer['UNIQUE_STOP_NO'])

summer['SEQUENTIAL_STOP_NO'] = np.where(summer['SEQUENTIAL_STOP_NO'].isnull(), summer['SEQUENTIAL_STOP_NO_summer2'], summer['SEQUENTIAL_STOP_NO'])
summer['SEQUENTIAL_STOP_NO'] = np.where(summer['SEQUENTIAL_STOP_NO'].isnull(), summer['SEQUENTIAL_STOP_NO_summer3'], summer['SEQUENTIAL_STOP_NO'])

summer['SAMPLES'] = np.where(summer['SAMPLES'].isnull(), summer['SAMPLES_summer2'], summer['SAMPLES'])
summer['SAMPLES'] = np.where(summer['SAMPLES'].isnull(), summer['SAMPLES_summer3'], summer['SAMPLES'])


# keep columns of interest
keep_cols = ['DAY_OF_WEEK_summer1', 'ROUTE', 'DIR', 'TRIP_TIME', 'PATTERN', 'UNIQUE_STOP_NO',
             'SEQUENTIAL_STOP_NO', 'SAMPLES', 'ON_final', 'OFF_final', 'LOAD_final']

summer = summer[summer.columns[summer.columns.isin(keep_cols)]]

# rename columns
summer.rename({
    'DAY_OF_WEEK_summer1': 'DAY_OF_WEEK',
    'UNIQUE_STOP_NO': 'STOP_CODE',
    'SEQUENTIAL_STOP_NO': 'STOP_SEQUENCE',
    'ON_final': 'ON',
    'OFF_final': 'OFF',
    'LOAD_final': 'LOAD'
}, axis=1, inplace=True)


# bring in stop code master to update stop names in dataframe
stop_code_master = pd.read_excel('W:\COMMUTER OPS - OPERATIONS & FACILITIES\PART Express Operations\Routes Schedules & Changes\Stop Codes\Stop Code Master.xlsx',
                                 skiprows=1, usecols=[0,1,2,3,4])
stop_code_master = stop_code_master[stop_code_master['stop_name'].notna()]


# join stop code master to dataframe
summer = summer.merge(stop_code_master,
                      how='inner',
                      left_on='STOP_CODE',
                      right_on='stop_code')
                      
                      
# drop redundant columns
summer.drop(['PATTERN', 'STOP_CODE', 'stop_desc'], axis=1, inplace=True)


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
    23: '23 - SW Piedmont Parkway',
    27: '27 - Airport Area',
    28: '28 - West Forsyth Express'
}

summer['route_long_name'] = summer['ROUTE'].map(route_long_name)


# calculate service period
service_conditions = [
    summer['TRIP_TIME'] >= 1800,
    summer['TRIP_TIME'] >= 1500,
    summer['TRIP_TIME'] >= 900
]

service_choices = ['Evening', 'PM Peak', 'Off Peak']

summer['SERVICE_PERIOD'] = np.select(service_conditions, service_choices, default='AM Peak')


# create and format a new trip time attribute
format_time_conditions = [
    summer['TRIP_TIME'].astype(str).str.len() == 5,
    summer['TRIP_TIME'].astype(str).str.len() == 6
]

format_time_choices = [
    '0' + summer['TRIP_TIME'].astype(str).str.slice(start=0, stop=1) + ':' + summer['TRIP_TIME'].astype(str).str.slice(start=1, stop=3),
    summer['TRIP_TIME'].astype(str).str.slice(start=0, stop=2) + ':' + summer['TRIP_TIME'].astype(str).str.slice(start=2, stop=4)
]

summer['USE_TIME'] = np.select(format_time_conditions, format_time_choices)


summer.to_csv('stop_summary_summer2020.csv', index=False)
