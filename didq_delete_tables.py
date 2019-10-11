'''python didq_delete_tables.py --project_id 'skyuk-uk-decis-models-01-dev' --schema_name 'RSC30' --reqd_table_names 'didq_discrete_table_SPORTS,didq_discrete_table_BBD'
'''


from datetime import *
from google.cloud import bigquery
import pandas as pd
import re
import sys 
from io import StringIO
import argparse
import warnings
warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")
import os 
import numpy 
import time 
import multiprocessing 
import parmap 



def delete_table(table_name, project_id):
    client = bigquery.Client(project = project_id)
    table_ref = client.dataset(schema_name).table(table_name)
    client.delete_table(table_ref) 

def return_tables(project_id, schema_name): 
  query = 'SELECT table_id FROM'+ '`'+ project_id+'.'+schema_name+'.__TABLES_SUMMARY__`'
  df  = pd.read_gbq(query, project_id,dialect = 'standard')
  #print(list(df['table_id']))
  return list(df['table_id'])


 
def delete_operation(project_id, mode):
	table_names = return_tables(project_id, schema_name)
	if mode=='prefix':
		deletion_table_names = list(filter(lambda k: pattern in k, table_names))
		print(deletion_table_names)
		results = parmap.map(delete_table, deletion_table_names, project_id)
		#results = p.map(delete_table, deletion_table_names)
		print('tables_deleted')
	elif mode == 'array':
		results = parmap.map(delete_table, reqd_table_names, project_id)
		#results = p.map(delete_table, reqd_table_names)
		

if __name__=='__main__':
  import argparse
  parser = argparse.ArgumentParser(description = 'test')
  parser.add_argument('--project_id',help = 'schema for which didq  is need to be run')
  parser.add_argument('--schema_name',help = 'table for which didq  is need to be run')
#  parser.add_argument('--mode',help = 'The corresponding project_id')
 # parser.add_argument('--pattern',help = 'delete tables based on pattern')
  parser.add_argument('--reqd_table_names', help='Pass this as a  list input for deleting tables')
  p = multiprocessing.Pool()
  args = parser.parse_args()
  project_id = args.project_id
  schema_name = args.schema_name
  #mode = args.mode 
  #pattern = args.pattern
  reqd_table_names = args.reqd_table_names
  if (len(reqd_table_names.split(',')) > 1):
  	mode = 'array'
  	reqd_table_names = reqd_table_names.split(',')
  	print(reqd_table_names)
  	delete_operation(project_id, mode)	
  else:
  	mode ='prefix'
  	pattern = reqd_table_names.split(',')[0]
  	print(pattern)
	delete_operation(project_id,mode)