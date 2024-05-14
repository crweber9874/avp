import pandas as pd
import numpy as np
import os
import glob
import re
import datetime
from datetime import date

path = r"/Users/Chris/Dropbox/masterData"
os.chdir(path)
csv_files = glob.glob(os.path.join(path, "*.csv"))

regInfo = ['Registrant ID', 'Registration Date',
           'Effective Date of Change',
           'Year of Birth',
           'LastName',
           'FirstName',
           'MiddleName', 'Suffix', 'Status', 'Status Reason', 'County',
           'HouseNumber', 'StreetPrefix', 'StreetName', 'StreetType',
           'StreetSuffix', 'UnitType', 'UnitNumber', 'Residence Address',
           'Residence City', 'Residence State', 'Residence Zip',
           'MailingAddress',
           'MailingAddress2', 'MailingCity', 'MailingState', 'MailingZip',
           'MailingCountry', 'Party', 'PEVL', 'Phone', 'Occupation',
           'PrecinctPart', 'Congressional', 'Board of Supervisors', 'Legislative',
           'Municipal', 'School', 'Fire', 'Precinct']

electionList = ['03/22/2016-2016 PRES PREF', '08/30/2016-2016 PRIMARY ELECTION',
                '11/08/2016-2016 GENERAL ELECTION', '08/28/2018-2018 PRIMARY ELECTION',
                '11/06/2018-2018 GENERAL ELECTION', '03/17/2020-2020 Presidential Preference', '08/04/2020-PRIMARY 2020',
                '11/03/2020-GENERAL 2020', '08/02/2022-2022 PRIMARY ELECTION',
                '08/02/2022-PRIMARY 2022', '11/08/2022-GENERAL 2022']



def check_header(df, electionList, regInfo):
    combined = electionList + regInfo
    columnList = regInfo + electionList
    columns_present = list(set(df.columns).intersection(columnList))
    df = df[columns_present]
    for election in combined:
        if election not in df.columns:
            df[election] = np.nan
        else:
            df = df
    valid_columns = []
    for column in df.columns:
        column = re.sub(r'[^a-zA-Z0-9_]', '_', column)
        column = re.sub(r'^\d+', '_', column)
        valid_columns.append(column)
    df.columns = valid_columns
    sorted_columns = sorted(df.columns)
    return df[sorted_columns]

for f in csv_files:
   path_name = os.path.split(f)[-1]
   match = re.search(r'(\d{4}-\d{2}-\d{2})', path_name)
   date = datetime.datetime.strptime(match.group(), '%Y-%m-%d').date()
   df = pd.read_csv(f, low_memory=False)
   df = check_header(df, electionList, regInfo)
   df["build_date"] = date
   df.to_csv("/Users/Chris/Dropbox/masterData/fix_headers/c" + path_name)

