
from googleapiclient.discovery import build
import pandas as pd
import requests
import json
import utils

SPREADSHEET_ID = '1bZX-JssNcHKmcika-X9U5HV32eRiZHUeYCCc_IfORvQ'
creds = utils.get_creds()

# election_date = '20201103'
# election_date = '20181106'
election_date = '20161108'


service = build('sheets', 'v4', credentials=creds)


dfs = []
requests_payload = []

sheet_id = 0
file_name = 'results_pct'
snake_date = election_date[0:4] + '_' + \
    election_date[4:6] + '_' + election_date[6:]
url = f'https://dl.ncsbe.gov/ENRS/{snake_date}/{file_name}_{election_date}.zip'
print(url)
data = utils.get_zipfile(url, f'{file_name}_{election_date}.txt')
df = pd.read_csv(data, delimiter='\t')
df = df.sort_values(['Contest Name', 'Choice'])
df = df[(df['Contest Name'].str.contains('NC STATE SENATE'))
        | (df['Contest Name'].str.contains('NC HOUSE'))]


grid_coordinate = utils.grid_for_sheet(sheet_id)
request_payload = utils.payload_for_file(df, grid_coordinate)
requests_payload.append(request_payload)

grouped = df.groupby(['Contest Name', 'Precinct']).sum().reset_index()
precinct_counts = grouped.groupby('Contest Name').count()
precinct_reported_counts = grouped[grouped['Total Votes'] >
                                   0].groupby('Contest Name').count()
precinct_df = precinct_counts.join(
    precinct_reported_counts, rsuffix='reported')
precinct_df = pd.DataFrame(
    {'precincts_reported_perc': precinct_df['Precinctreported'] / precinct_df['Precinct'] * 100}, index=precinct_df.index)

sheet_id = 1496596366
url = f'https://er.ncsbe.gov/enr/{election_date}/data/results_0.txt'
print(url)
resp = requests.get(url)
df = pd.read_json(resp.content)
df = df.drop(['cid', 'vfr', 'gid', 'lid', 'dtx', 'prt',
              'ptl', 'col', 'ogl', 'ref'], axis=1)
df = df.rename(columns={'cnm': 'Race', 'bnm': 'Candidate',
                        'pty': 'Party', 'vct': 'Total Votes', 'pct': 'Percent of Vote',
                        'evc': 'Election Day Vote Count', 'avc': 'Absentee Vote Count', 'ovc': 'One-Stop Vote Count', 'pvc': 'Provisional Vote Count'})
df = df.sort_values(['Race', 'Candidate'])
filtered_df = df[(df['Race'].str.contains('NC STATE SENATE'))
                 | (df['Race'].str.contains('NC HOUSE'))]
filtered_df['Race'] = filtered_df['Race'].str.replace(
    ' (VOTE FOR 1)', '', regex=False)
grid_coordinate = utils.grid_for_sheet(sheet_id)
request_payload = utils.payload_for_file(filtered_df, grid_coordinate)
requests_payload.append(request_payload)

breakpoint()

joined = utils.build_joined_df(filtered_df, precinct_df)


sheet_id = 2103006474
grid_coordinate = utils.grid_for_sheet(sheet_id)
request_payload = utils.payload_for_file(joined, grid_coordinate)
requests_payload.append(request_payload)

update_payload = {'requests': requests_payload}

sheet = service.spreadsheets()
request = service.spreadsheets().batchUpdate(
    spreadsheetId=SPREADSHEET_ID, body=update_payload)
response = request.execute()
