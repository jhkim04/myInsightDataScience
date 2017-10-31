# -*- coding: utf-8 -*-
"""
By Jinho Kim

This is a python script for Coding Challenge of Insight Data Engineering
"""

import numpy as np
import pandas as pd
import sys

col_names = ['CMTE_ID',
      'AMNDT_IND',
      'RPT_TP',
      'TRANSACTION_PGI',
      'IMAGE_NUM',
      'TRANSACTION_TP',
      'ENTITY_TP',
      'NAME',
      'CITY',
      'STATE',
      'ZIP_CODE',
      'EMPLOYER',
      'OCCUPATION',
      'TRANSACTION_DT',
      'TRANSACTION_AMT',
      'OTHER_ID',
      'TRAN_ID',
      'FILE_NUM',
      'MEMO_CD',
      'MEMO_TEXT',
      'SUB_ID'] 
    
colums_selected = ["CMTE_ID", "ZIP_CODE","TRANSACTION_DT","TRANSACTION_AMT"]
    
def main(infile, outfile_by_zip, outfile_by_date):
    #import csv
    #with open('./input/indiv_header_file.csv', 'r') as f:
    #  reader = csv.reader(f)
    #  col_names = list(reader)

    df = pd.read_table(infile,sep="|", header=None, names = col_names, dtype=str)
    
    # Filtering data where OTHER_ID is null and
    # non-empty cells in the CMTE_ID and TRANSACTION_AMT fields
    df = df.loc[df['OTHER_ID'].isnull() & df['CMTE_ID'].notnull() & df['TRANSACTION_AMT'].notnull()]
    
    # transfrom TRANSACTION_AMT to integer values
    df['TRANSACTION_AMT'] = pd.to_numeric(df['TRANSACTION_AMT'], errors='coerce')
    # extract 5 digit zipcode and transfrom invalid zipcode (i.e., empty, fewer than five digits) as NaN
    df['ZIP_CODE'] = df['ZIP_CODE'].str.extract('^(\d{5})', expand = False)
    # select only CMTE_ID, ZIP_CODE, TRANSACTION_DT, TRANSACTION_AMT
    df = df[colums_selected]
# =============================================================================
# group by zip code
# =============================================================================
    # Group data by CMTE_ID and ZIP_CODE
    df_zip = df.loc[df['ZIP_CODE'].notnull()]
    group_by_zip = df_zip.groupby(['CMTE_ID','ZIP_CODE'])
    
    # calcualte number of transactions received by recipient from the contributor's zip code streamed in so far
    df_zip.loc[:,'RUNNING_CNT'] = group_by_zip['TRANSACTION_AMT'].cumcount()+1
   # calcualte amount of transactions received by recipient from the contributor's zip code streamed in so far
    df_zip.loc[:,'RUNNING_TTL'] = group_by_zip['TRANSACTION_AMT'].cumsum()
    df_zip.loc[:,'RUNNING_MID'] = 0
    #df['RUNNING_MID'] = 
    
    # calculate running median of contributions received by recipient from the contributor's zip code streamed in so far
    for cmte_id, zip_code in group_by_zip.groups.keys():
        by_id_zip = df_zip[(df_zip['CMTE_ID']==cmte_id) & (df_zip['ZIP_CODE'] == zip_code)]
        trns_amt = by_id_zip['TRANSACTION_AMT']   
        df_zip.loc[(df_zip['CMTE_ID']==cmte_id) & (df_zip['ZIP_CODE'] == zip_code), 'RUNNING_MID'] = [np.round(np.median(trns_amt[:i])) for i in range(1,len(trns_amt)+1)]
    
    save_by_zip = df_zip[['CMTE_ID', 'ZIP_CODE', 'RUNNING_MID', 'RUNNING_CNT', 'RUNNING_TTL']]
    
    ### save to file, medianvals_by_zip.txt
    save_by_zip.to_csv(outfile_by_zip, header=None, index=None, sep='|', mode='w')
    
    
# =============================================================================
# group by date
# =============================================================================
    # Group data by CMTE_ID and RANSACTION_DT
    group_by_date = df.groupby(['CMTE_ID','TRANSACTION_DT'], as_index=False)

    # caculate total number, total amount, and median of contributions received by recipient on that date
    df_by_date = group_by_date['TRANSACTION_AMT'].agg({'COUNT':'count', 'TOTAL':'sum', 
                                  'MEDIAN': lambda x:np.round(np.median(x))
                              })
    
    # Sorted alphabetical by recipient and then chronologically by date
    df_by_date['DATE'] = pd.to_datetime(df_by_date['TRANSACTION_DT'], format='%m%d%Y', errors='coerce')
    df_by_date = df_by_date.loc[df_by_date['DATE'].notnull()]
    df_date_sorted = df_by_date.sort_values(by=['CMTE_ID','DATE'], ascending=[True, True])
    
    save_by_date = df_date_sorted[['CMTE_ID', 'TRANSACTION_DT', 'MEDIAN', 'COUNT', 'TOTAL']]
    ## save to file, medianvals_by_date.txt
    save_by_date.to_csv(outfile_by_date, header=None, index=None, sep='|', mode='w')

# =============================================================================
# main
# =============================================================================


if __name__ == "__main__":
    
    _infile = "./input/itcont.txt"
    _outfile_by_zip = "./output/medianvals_by_zip.txt"
    _outfile_by_date = "./output/medianvals_by_date.txt"

    if (len(sys.argv) == 1):
        pass
    elif (len(sys.argv) == 2):
        _infile = sys.argv[1]
    elif (len(sys.argv) == 3):
        _infile = sys.argv[1]
        _outfile_by_zip = sys.argv[2] 
    elif (len(sys.argv) >= 4):    
        _infile = sys.argv[1]
        _outfile_by_zip = sys.argv[2]
        _outfile_by_date = sys.argv[3] 
    else:
        warning = 'Usage: python ./src/find_political_donors.py ./input/itcont.txt ./output/medianvals_by_zip.txt ./output/medianvals_by_date.txt'
        print(warning)
        
    main(_infile, _outfile_by_zip, _outfile_by_date)