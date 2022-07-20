# Python-Script-for-LFD-testing
# 


import pandas as pd
import numpy as np
from datetime import date
from datetime import datetime
from time import time
import argparse
#import dask.dataframe as dd

# In[83]:

parser = argparse.ArgumentParser()
parser.add_argument()
parser.add_argument()
parser.add_argument()
parser.add_argument()

args = parser.parse_args()

#args = parser.parse_args()

raw_sc = args.sc
raw_pr = args.prim
raw_lpr = args.legacy

# In[83]:
raw_sc = pd.read_csv(raw_sc)

# Exception handle if primary file is empty
try:
    raw_pr = pd.read_csv(raw_pr)
except IOError as e:
    raw_pr = pd.DataFrame(columns=raw_sc.columns)

raw_pr_legacy = pd.read_csv(raw_lpr)

# In[84]:

# pulling together publication metrics
print(datetime.now().strftime("%H:%M:%S"), "Process starting...")
sc_df = raw_sc
pr_df = raw_pr
output_dir = args.output
print(datetime.now().strftime("%H:%M:%S"), f"Loaded LFD dataset: {len(sc_df)} rows...")
print(datetime.now().strftime("%H:%M:%S"), f"Loaded PCR dataset: {len(pr_df)} rows...")
print(datetime.now().strftime("%H:%M:%S"), f"Loaded Legacy PCR dataset: {len(raw_pr_legacy)} rows...")

# In[85]:

#Change data type of week_commencing column in legacy set
print(datetime.now().strftime("%H:%M:%S"), f"Creating weekly bracket column...")
raw_pr_legacy['week_commencing'] = pd.PeriodIndex(raw_pr_legacy.week_commencing, freq='W-WED')

# creating the reporting week df (dates and corresponding week commencing date)
df_dates = pd.DataFrame({'Date': pd.date_range('2020-01-01', '2025-12-31')})
df_dates['week_commencing'] = df_dates['Date'].dt.to_period('W-WED')

# reformat datetime for specimen_processed_date
df_list = [sc_df, pr_df]
#if primary set is not empty then reformat dates for both datasets, otherwise only secondary
if len(pr_df) != 0:
    for x in df_list:
        x["specimen_processed_date"] = pd.to_datetime(x["specimen_processed_date"], errors='coerce')
        x["specimen_processed_date"] = x["specimen_processed_date"].dt.date
        x["specimen_processed_date"] = pd.to_datetime(x["specimen_processed_date"], errors='coerce')
    # joining the data to the reporting week cycle
    sc_df = pd.merge(sc_df, df_dates, how="left", left_on="specimen_processed_date", right_on="Date")
    pr_df = pd.merge(pr_df, df_dates, how="left", left_on="specimen_processed_date", right_on="Date")
else:
    sc_df["specimen_processed_date"] = pd.to_datetime(sc_df["specimen_processed_date"], errors='coerce')
    sc_df["specimen_processed_date"] = sc_df["specimen_processed_date"].dt.date
    sc_df["specimen_processed_date"] = pd.to_datetime(sc_df["specimen_processed_date"], errors='coerce')
    sc_df = pd.merge(sc_df, df_dates, how="left", left_on="specimen_processed_date", right_on="Date")

print(datetime.now().strftime("%H:%M:%S"), f"Processing secondary orphan data...")

# Secondary Orphan reformatting
orphans_sec = pd.pivot_table(sc_df, index='week_commencing', columns='npex_results',
                             values='orphans', aggfunc='sum')
orphans_sec = orphans_sec[['POSITIVE', 'NEGATIVE', 'VOID']].transpose()
orphans_sec.index = orphans_sec.index.set_names([
    'Total number of LFD Orphans tests taken in primary schools, school-based nurseries and maintained nursery schools'])

# Primary Orphan reformatting
print(datetime.now().strftime("%H:%M:%S"), f"Processing primary orphan data...")

    #if the primary file is not empty, reformat
if len(pr_df) != 0:
    orphans_prim = pd.pivot_table(pr_df, index='week_commencing', columns='npex_results',
                                  values='orphans', aggfunc='sum')
    orphans_prim = orphans_prim[['POSITIVE', 'NEGATIVE', 'VOID']].transpose()
    orphans_prim.index = orphans_prim.index.set_names([
        'Total number of LFD Orphans tests taken in primary schools, school-based nurseries and maintained nursery schools'])
else:
    print(datetime.now().strftime("%H:%M:%S"), "Using legacy PCR dataset...")
    orphans_prim = raw_pr_legacy[['week_commencing', 'POSITIVE', 'NEGATIVE', 'VOID']].transpose()
    #Move first row (week_commencing) to column headers
    orphans_prim.columns = orphans_prim.iloc[0]
    #Remove first rows (week_commencing)
    orphans_prim = orphans_prim.iloc[1:, :]
    orphans_prim.index = orphans_prim.index.set_names([
        'Total number of LFD Orphans tests taken in primary schools, school-based nurseries and maintained nursery schools'])

#Concat the two tables for high level table
high_level_df = pd.concat([orphans_prim, orphans_sec],
                          ignore_index=False, sort=True, keys=[
        'Total number of LFD Orphans tests taken in primary schools, school-based nurseries and maintained nursery schools',
        'Total number of LFD Orphans tests taken in Secondary Schools & Colleges'],
                          names=['Phase of Education', 'Test Result'])

high_level_df.fillna(0, inplace=True)
print(datetime.now().strftime("%H:%M:%S"), "High Level table created")

# Download file
current_date = date.today()
current_date = current_date.strftime("%d%m")
high_level_df.to_csv((fr'{output_dir}Orphans_{current_date}.csv'))
print(datetime.now().strftime("%H:%M:%S"), f"Education Orphans file saved to {output_dir}")
