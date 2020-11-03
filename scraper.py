import json
import sys
import time

import pandas as pd
import requests
from googleapiclient.discovery import build

import utils

SPREADSHEET_ID = '1bZX-JssNcHKmcika-X9U5HV32eRiZHUeYCCc_IfORvQ'
creds = utils.get_creds()

# election_date = '20181106'
# election_date = '20201103'
# election_date = '20161108'

election_date = sys.argv[1]
wait_time = int(sys.argv[2])


def process_election(election_date):

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
    raw_results_df = pd.read_csv(data, delimiter='\t')
    raw_results_df = utils.filter_df(raw_results_df)
    raw_results_df = raw_results_df.sort_values(['Contest Name', 'Choice'])

    grid_coordinate = utils.grid_for_sheet(sheet_id)
    request_payload = utils.payload_for_file(raw_results_df, grid_coordinate)
    # requests_payload.append(request_payload)

    filtered_precinct_df = raw_results_df.copy()
    precinct_blacklist = ['TRANS', 'ONE', 'OS', 'CURB', 'PROVI', 'ABSEN']
    for item in precinct_blacklist:
        filtered_precinct_df = filtered_precinct_df[~filtered_precinct_df['Precinct'].str.contains(
            item)]
    grouped = filtered_precinct_df.groupby(
        ['Contest Name', 'Precinct']).sum().reset_index()
    grouped = grouped.dropna()
    precinct_counts = grouped.groupby('Contest Name').count()
    precinct_reported_counts = grouped[grouped['Total Votes'] >
                                       0].groupby('Contest Name').count()

    precinct_df = precinct_counts.join(
        precinct_reported_counts, rsuffix='reported')
    precinct_df = pd.DataFrame(
        {'precincts_reported_perc': precinct_df['Precinctreported'] / precinct_df['Precinct']}, index=precinct_df.index)

    sheet_id = 1496596366
    url = f'https://er.ncsbe.gov/enr/{election_date}/data/results_0.txt'
    print(url)
    resp = requests.get(url)
    candidate_df = pd.read_json(resp.content)
    candidate_df = candidate_df.drop(['cid', 'vfr', 'gid', 'lid', 'dtx', 'prt',
                                      'ptl', 'col', 'ogl', 'ref'], axis=1)
    candidate_df = candidate_df.rename(columns={'cnm': 'Race', 'bnm': 'Candidate',
                                                'pty': 'Party', 'vct': 'Total Votes', 'pct': 'Percent of Vote',
                                                'evc': 'Election Day Vote Count', 'avc': 'Absentee Vote Count', 'ovc': 'One-Stop Vote Count', 'pvc': 'Provisional Vote Count'})
    filtered_df = utils.filter_df(candidate_df)
    filtered_df = filtered_df.sort_values(['Race', 'Candidate'])
    grid_coordinate = utils.grid_for_sheet(sheet_id)
    request_payload = utils.payload_for_file(filtered_df, grid_coordinate)
    # requests_payload.append(request_payload)

    filtered_df = filtered_df.loc[filtered_df.groupby(
        ['Race', 'Candidate'])['Total Votes'].idxmax().dropna()]
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


while True:
    process_election(election_date)
    print(f'Sleeping for {wait_time}')
    time.sleep(wait_time)
