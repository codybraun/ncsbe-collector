import os.path
import pickle
import zipfile
from io import BytesIO

import requests
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']


def get_creds():
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return creds


def get_zipfile(url, name):
    resp = requests.get(url)
    external_zipfile = zipfile.ZipFile(BytesIO(resp.content))
    txt_file = external_zipfile.open(name)
    return txt_file


def grid_for_sheet(sheet_id):
    return {
        "sheetId": sheet_id,
        "columnIndex": 0,
        "rowIndex": 0,
    }


def payload_for_file(df, grid_coordinate):
    return {'pasteData': {
        "coordinate": grid_coordinate,
        "data": df.to_csv(index=False),
        "delimiter": ','}}


def build_joined_df(filtered_df, precinct_df):
    dem_df = filtered_df[filtered_df['Party'] == 'DEM']
    dem_df = dem_df.set_index('Race')
    rep_df = filtered_df[filtered_df['Party'] == 'REP']
    rep_df = rep_df.set_index('Race')

    tp_df = filtered_df[(filtered_df['Party'] != 'DEM') & (
        filtered_df['Party'] != 'REP')].groupby('Race').sum()

    joined = dem_df.join(rep_df, rsuffix='_rep', how='outer').join(
        precinct_df, how='left').join(tp_df, how='left', rsuffix='_third')

    joined = joined.fillna(0)
    joined['dem_margin_perc'] = joined['Percent of Vote'] - \
        joined['Percent of Vote_rep']
    joined['dem_margin_perc'] = joined['dem_margin_perc']

    joined['dem_margin_raw'] = joined['Total Votes'] - \
        joined['Total Votes_rep']
    joined['dr_votes_counted'] = joined['Total Votes'] + \
        joined['Total Votes_rep']

    joined = joined.reset_index(
    )
    if 'index' in joined.columns:
        joined['Race'] = joined['index']
    # joined = joined[['Race', 'dem_margin_perc', 'dem_margin_raw',
    #                  'dr_votes_counted', 'precincts_reported_perc']]
    joined['Absolute Value of Spread'] = joined['dem_margin_perc'].abs()
    joined['Vote Count'] = joined['Total Votes_third'] + \
        joined['Total Votes'] + joined['Total Votes_rep']
    joined = joined[['Race', 'dem_margin_perc', 'Candidate',
                     'Candidate_rep', 'Percent of Vote', 'Percent of Vote_rep', 'Percent of Vote_third', 'precincts_reported_perc', 'Absolute Value of Spread', 'Vote Count']]
    joined = joined.rename(columns={'Race': 'District', 'dem_margin_perc': 'Dem Margin %', 'Candidate': 'Democrat', 'Candidate_rep': 'Republican',	'Percent of Vote': 'Dem %', 'Percent of Vote_rep': 'Rep %', 'Percent of Vote_third': 'Other %',
                                    'precincts_reported_perc': 'Precincts Reporting'})

    return joined
