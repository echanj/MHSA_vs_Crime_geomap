import numpy as np
import pandas as pd
import json
from scipy import stats
import sqlite3

from pandas.io import sql
sqlite_db = 'miniproject_db.sqlite'
conn = sqlite3.connect(sqlite_db) 
c = conn.cursor()


def get_mhsa_ids_for_state(focus_state):


 query = """
SELECT MHSA_idx, count(1) as number_of_agencies
FROM agency_hitlist  
WHERE state_abbr = '%s' 
GROUP BY MHSA_idx;
"""
 grouped_MHSA_ids = sql.read_sql(query%(focus_state), con=conn)
 my_mhsa_ids=list(grouped_MHSA_ids['MHSA_idx'].values)
 return  my_mhsa_ids,grouped_MHSA_ids['number_of_agencies'].sum()



def get_agency_data(my_mhsa_ids):
 
 '''
 go through all the saveed .json munge and pool the crime data 
 save as list of dictionaries. each dict corresponds to an MHSA row from our intital shortlist
 also calculate p-Value from t-test 
 '''
 
 # my_mhsa_ids=[18]
 agency_grouped_crime_data=[]
 # for idx in range(len(mh)):
 # for idx in range(10):
 for idx in my_mhsa_ids :
     mhsa_id = idx
     
     # print("working on MHSA ID %i"%mhsa_id)
     query = """
     SELECT * FROM agency_data_extraction_sup_info
      WHERE mhsa_id = %i;
         """
     agency_sup_info = sql.read_sql(query%(mhsa_id), con=conn)
     
    # ref_from_year=agency_sup_info.data_from_year.max()
    # ref_to_year=agency_sup_info.data_to_year.min()
    # withdrawn_year=agency_sup_info.withdrawn_year.min()
    # print(ref_from_year,ref_to_year,withdrawn_year)
 
     agencies=[]
     crime_tots=pd.DataFrame()  # create a fresh data frame 
     for agency in agency_sup_info.iterrows():
         ref_from_year=agency[1].data_from_year
         ref_to_year=agency[1].data_to_year
         withdrawn_year=agency[1].withdrawn_year
         filename=agency[1].filename
         agency_name=agency[1].agency_name
         agencies.append(agency_name)
         if filename != 'none':
             
             with open('./data/%s'%filename, 'r') as file:
                 agency_crime = json.load(file)
             agency_crime_df=pd.DataFrame(agency_crime['data'])
             agency_crime_df['tot_crime'] = agency_crime_df.drop(['data_year'],axis=1).astype(int).sum(axis=1)
             df_slice=agency_crime_df[(agency_crime_df['data_year']>=ref_from_year)  & (agency_crime_df['data_year']<=ref_to_year)]
             crime_tots=pd.concat([crime_tots,df_slice[['data_year','tot_crime']]],axis=0)
             data_years=df_slice.data_year.values
             #. we are going to assume empty when data for the year is empty the crime count was 0. needs revision later      
             
             
             
       #  else:
            # print ( "%s has no data avaliable" %agency_name)
  
     if len(crime_tots) != 0 :
         grouped_crime_tots = crime_tots.groupby('data_year').sum()  #. this is the collective info we want - this gets around the problem of empty data entries 
 
         if withdrawn_year <= grouped_crime_tots.index.max():
         
             group_a=grouped_crime_tots[grouped_crime_tots.index.values<withdrawn_year].tot_crime.values
             group_b=grouped_crime_tots[grouped_crime_tots.index.values>=withdrawn_year].tot_crime.values

             if group_a.size > 0: 
                ave_counts_group_a=int(np.mean(group_a))
             else:
                ave_counts_group_a=0

             if group_b.size >0: 
                ave_counts_group_b=int(np.mean(group_b))
             else:
                ave_counts_group_b=0
         
    
             t_statistic, p_value = stats.ttest_ind(group_a, group_b)
         
         mydict= {'mhsa_id': mhsa_id,
               'agencies':agencies,
              'withdrawn_year': withdrawn_year,
             'ave_counts_group_a': ave_counts_group_a,
             'ave_counts_group_b': ave_counts_group_b,
                  'p-value': p_value,
             'data_year' : list(grouped_crime_tots.index.values) , 
             'tot_crime' : list(grouped_crime_tots.tot_crime.values)}
     
     else:
         mydict= {'mhsa_id': mhsa_id,
               'agencies':agencies,
              'withdrawn_year': withdrawn_year,
                   'ave_counts_group_a': None,
             'ave_counts_group_b': None,
                  'p-value': None, 
             'data_year' : list([]) , 
             'tot_crime' : list([])}
     
 
 
     
 
     agency_grouped_crime_data.append(mydict)
     agcd= pd.DataFrame(agency_grouped_crime_data)
 
 return agcd
 
