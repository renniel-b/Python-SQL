#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from IPython.display import display, Markdown
import time
import os
import pathlib
import re

import sqlalchemy
from sqlalchemy.engine import *
from sqlalchemy import text
from sqlalchemy import types
from sqlalchemy import event
from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker

import numpy as np
import pandas as pd

from pyexcelerate import Workbook
from openpyxl import load_workbook


current_directory = 'C:\\Users\\'+os.environ.get('USERNAME')
os.chdir(current_directory)
folder_path = current_directory+'\\Documents\\Python-Oracle\\Excel'
if os.path.exists(folder_path) == True:
    print(f'Folder path {folder_path} already exists.')
else:
    print(f'Folder path {folder_path} does not exists. Creating new directory.')
    # Create the new folder
    os.makedirs(folder_path, exist_ok=True)

def fname_check(raw_fname,table_name=None):
    from pathlib import Path
    curr_folder_path = r'C:/Users/'+os.environ.get('USERNAME')+'/Documents/Python-Oracle/Excel'
    special_characters = "!@#$%^&*()[]{};:,./<>?\|`~-=+"
    
    if 'C:/Users/' in raw_fname and '/Documents/Python-Oracle/Excel' in raw_fname:
        fname = raw_fname
        if is_nan(table_name) == True:
            filename = fname.split('/')
            filename = filename[-1].split('.')
            table_name = filename[0].upper()
        else:
            table_name = table_name.upper()
    elif '/' in raw_fname:
        filename = raw_fname.split('/')
        fname = filename[-1]
        if is_nan(table_name) == True:
            filename = fname.split('/')
            filename = filename[-1].split('.')
            table_name = filename[0].upper()
        else:
            table_name = table_name.upper()
    elif '/' not in raw_fname and raw_fname.endswith('.xlsx'):
        fname = raw_fname
        if is_nan(table_name) == True:
            filename = fname.split('/')
            filename = filename[-1].split('.')
            table_name = filename[0].upper()
        else:
            table_name = table_name.upper()
    else:
        filename = raw_fname.split('.')
        filename = filename[-2:]
        fname = '.'.join(filename)
        if is_nan(table_name) == True:
            table_name = filename[0].upper()
        else:
            table_name = table_name.upper()
    
    filename = fname.split('/')
    fname_clean = filename[-1]
    filename_clean = fname_clean.split('.')
    if any(item in special_characters for item in filename_clean[0]):
        fname_clean = clean_char(filename_clean[0],"_") # replace special characters with "_" and remove ending "_" character.
        fname_clean = filename[:-1]+fname_clean+'.'+filename_clean[-1]
        fname = fname_clean
        
    fullpath = None
    is_fullpath = False
    is_file = False
    folder_path = curr_folder_path
    
    if os.path.exists(curr_folder_path+'/'+fname) == True:
        fullpath = str(Path(curr_folder_path+'/'+fname).resolve())
        is_fullpath = os.path.exists(curr_folder_path+'/'+fname)
        is_file = os.path.isfile(fullpath)
        folder_path = fullpath.split('\\')
        folder_path = '/'.join(folder_path[:-1])
        fname = fullpath.replace('\\','/')
    else:
        fname = raw_fname
        if '/' in fname:
            filename = fname.split('/')
            filename = filename[-1].split('.')
            # Convert to uppercase for case sensitivity
            fname = filename[0].upper()
            fname_clean = clean_char(filename[0],"_") # replace special characters with "_" and remove ending "_" character.
        else:
            filename = fname.split('.')
            # Convert to uppercase for case sensitivity
            fname = filename[0].upper()
            fname_clean = clean_char(filename[0],"_") # replace special characters with "_" and remove ending "_" character.

        for i in range(1,len(filename)):
            fname += '.' + filename[i]
            fname_clean += '.' + filename[i]

        dataset_dir = pathlib.Path(curr_folder_path)
        dataset_dir_list = [item for item in dataset_dir.rglob("*") if item.is_dir()]
        dataset_file_list = [item for item in dataset_dir.rglob("*") if item.is_file()]
        dataset_dir_array_shape = np.array(dataset_dir_list).shape
        dataset_file_array_shape = np.array(dataset_file_list).shape
        for j in range(dataset_file_array_shape[0]):
            pathlib_fullpath = str(dataset_file_list[j])
            # If fname contains special characters, use fname_clean
            if any(item in special_characters for item in fname):
                os_fullpath = os.path.join(folder_path,fname_clean)
                if os_fullpath == pathlib_fullpath:
                    fullpath = os.path.join(folder_path,fname)
                    is_fullpath = os.path.exists(fullpath)
                    is_file = os.path.isfile(fullpath)
                    fname = folder_path.replace("\\","/")+'/'+fname_clean
            else:
                os_fullpath = os.path.join(folder_path,fname)
                if os_fullpath == pathlib_fullpath:
                    fullpath = os.path.join(folder_path,fname)
                    is_fullpath = os.path.exists(fullpath)
                    is_file = os.path.isfile(fullpath)
                    fname = folder_path.replace("\\","/")+'/'+fname
        
                        
    _dict_ = {
        'is_fullpath': is_fullpath,
        'fullpath': fullpath,
        'is_file': is_file,
        'folder_path': folder_path,
        'fname': fname,
        'table_name': table_name
    }
    return _dict_

def get_credentials(filename,schema=None,database='oracle',db_file=None):
    current_directory = 'C:\\Users\\'+os.environ.get('USERNAME')
    filename = current_directory+'\\'+filename
    credential = open(filename,'r')
    USERNAME = credential.readline().rstrip()
    PASSWORD = credential.readline().rstrip()
    OS_USER = credential.readline().rstrip()
    HOST = credential.readline().rstrip()
    PORT = credential.readline().rstrip()
    SERVICE = credential.readline()
    if database.lower() == 'oracle':
        if schema != None:
            USERNAME = USERNAME+f'[{schema.upper()}]'
        else:
            USERNAME = USERNAME
        ENGINE_PATH_WIN_AUTH = f'oracle+cx_oracle://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/?service_name={SERVICE}'
    elif database.lower() == 'mysql':
        if db_file != None:
            ENGINE_PATH_WIN_AUTH = f'mysql+pymysql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{db_file}'
        else:
            ENGINE_PATH_WIN_AUTH = f'mysql+pymysql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}'
    elif database.lower() == 'sqlite':
        if db_file != None:
            db_fullpath = fname_check(db_file)
        else:
            db_fullpath = input('Missing db_file argument. Insert database file path.')
        ENGINE_PATH_WIN_AUTH = f'sqlite://{db_fullpath}'
    elif database.lower() == 'postgresql':
        if db_file != None:
            ENGINE_PATH_WIN_AUTH = f'postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{db_file}'
        else:
            ENGINE_PATH_WIN_AUTH = f'postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}'
    credential.close()
    
    _dict = {
        'DB_USERNAME': USERNAME,
        'DB_PASSWORD': PASSWORD,
        'HOST': HOST,
        'PORT': PORT,
        'SERVICE': SERVICE,
        'OS_USERNAME': OS_USER,
        'ENGINE_PATH_WIN_AUTH': ENGINE_PATH_WIN_AUTH,
        'DATABASE': database
    }
    return _dict

def connect_sqlalchemy(credentials):
    from sqlalchemy import create_engine
    ENGINE = create_engine(credentials['ENGINE_PATH_WIN_AUTH'], pool_pre_ping=True)
    return ENGINE

#--- Ths is the workaround for fast_executemany to speed-up INSERT.
# @event.listens_for(engine, 'before_cursor_execute')
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    if executemany:
        cursor.fast_executemany = True

def get_time_elapsed(start_time):
    import time
    print(f'Time elapsed: {time.time() - start_time:2f}')
    new_start_time = time.time()
    return new_start_time
        
def replace_special_char(string,character):
    special_characters = "!@#$%^&*()[]{};:,./<>?\|`~-=+"
    if any(item in special_characters for item in string):
        string = string.translate ({ord(c): character for c in special_characters}) # excluding defined character
    return string

def remove_digits(string):
    string = ''.join([i for i in string if not i.isdigit()])
    return string

def remove_end_char(string,character):
    if string.endswith(character):
        string = string[:-1]
    return string

def clean_char(string,character):
    string = replace_special_char(string,character)
    string = remove_end_char(string,character)
    return string

def insert_space_at_line_breaks(text,num_indent):
    lines = text.split('\n')
    lines_with_space = [' '*num_indent + line for line in lines]
    return '\n'.join(lines_with_space)

def days_between(d1,d2,temporal='days'):
    import datetime
    from dateutil.relativedelta import relativedelta
    from datetime import date
    if "DATE '" in d1 and '-' in d1:
        d1_result = re.search(" '"+"(.*)"+"'",d1)
        d1_year = d1_result.group(1)[0:4]
        d1_month = d1_result.group(1)[5:7]
        d1_day = d1_result.group(1)[8:10]
        d1 = date(int(d1_year), int(d1_month), int(d1_day))

        d2_result = re.search(" '"+"(.*)"+"'",d2)
        d2_year = d2_result.group(1)[0:4]
        d2_month = d2_result.group(1)[5:7]
        d2_day = d2_result.group(1)[8:10]
        d2 = date(int(d2_year), int(d2_month), int(d2_day))
    else:
        d1 = datetime.strptime(d1, "%Y-%m-%d")
        d2 = datetime.strptime(d2, "%Y-%m-%d")
        
    if temporal.upper() == 'DAYS' or temporal.upper() == 'DAY' or temporal.upper() == 'DAILY' or temporal.upper() == 'DD':
        diff = abs(relativedelta(d2, d1).days)
    elif temporal.upper() == 'WEEKS' or temporal.upper() == 'WEEK' or temporal.upper() == 'WEEKLY' or temporal.upper() == 'WW' or temporal.upper() == 'IW':
        diff = abs(relativedelta(d2, d1).days // 7) + 1
    elif temporal.upper() == 'MONTHS' or temporal.upper() == 'MONTH' or temporal.upper() == 'MONTHLY' or temporal.upper() == 'MM':
        diff = abs(relativedelta(d2, d1).months) + 1
    elif temporal.upper() == 'YEARS' or temporal.upper() == 'YEAR' or temporal.upper() == 'YEARLY' or temporal.upper() == 'YY'  or temporal.upper() == 'YYYY':
        diff = abs(relativedelta(d2, d1).years) + 1
        
    return diff

def ora_date_to_pydate(var_date):
    from datetime import date
    var_date_result = re.search(" '"+"(.*)"+"'",var_date)
    var_date_year = var_date_result.group(1)[0:4]
    var_date_month = var_date_result.group(1)[5:7]
    var_date_day = var_date_result.group(1)[8:10]
    pydate = date(int(var_date_year), int(var_date_month), int(var_date_day))
    return pydate

def ora_date_inc(var_date,temporal,increment):
    from dateutil.relativedelta import relativedelta
    var_date = ora_date_to_pydate(var_date)
    if temporal.upper() == 'DAYS' or temporal.upper() == 'DAY' or temporal.upper() == 'DAILY' or temporal.upper() == 'DD':
        var_date += relativedelta(days=increment)
    elif temporal.upper() == 'WEEKS' or temporal.upper() == 'WEEK' or temporal.upper() == 'WEEKLY' or temporal.upper() == 'WW' or temporal.upper() == 'IW':
        var_date += relativedelta(weeks=increment)
    elif temporal.upper() == 'MONTHS' or temporal.upper() == 'MONTH' or temporal.upper() == 'MONTHLY' or temporal.upper() == 'MM':
        var_date += relativedelta(months=increment)
    elif temporal.upper() == 'YEARS' or temporal.upper() == 'YEAR' or temporal.upper() == 'YEARLY' or temporal.upper() == 'YY'  or temporal.upper() == 'YYYY':
        var_date += relativedelta(years=increment)
    date_string = 'DATE ' + "'" + str(var_date) + "'"
    return date_string

def df_fix_dtype(df,schema,dtyp=None):
    import datetime
    from datetime import date
    import decimal
    from decimal import Decimal
    
    df_sample = df.loc[:10000,:]
    col_w_nan = df_sample.columns[df_sample.isna().any()].tolist()
    col_n_nan = df_sample.columns[df_sample.notnull().all()]
    
    df_col_w_nan_new = pd.DataFrame()
    df_col_w_nan_new[col_w_nan] = np.nan
    
    for cols,rows in df[col_w_nan].items():
        row_sample = rows.loc[:10000]
        is_int_type = row_sample[row_sample.notnull()].apply(lambda x: isinstance(x,int)).any()
        is_float_type = row_sample[row_sample.notnull()].apply(lambda x: isinstance(x,float)).any()
        is_date_type = check_dtime(row_sample)['is_date']
        is_datetime_type = check_dtime(row_sample)['is_timestamp']
        is_str_type = row_sample[row_sample.notnull()].apply(lambda x: isinstance(x,str)).any()
        
        if is_float_type == True:
            df_col_w_nan_new[cols] = pd.to_numeric(rows,errors='coerce')
        elif is_int_type == True:
            df_col_w_nan_new[cols] = pd.to_numeric(rows,errors='coerce').astype(pd.Int64Dtype())
        elif is_date_type == True or is_datetime_type == True:
            df_col_w_nan_new[cols] = pd.to_datetime(rows,errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S').astype(str)
        elif is_str_type == True:
            df_col_w_nan_new[cols] = rows.astype(str)
        
    for cols in df_sample[col_n_nan].columns:
        if df_sample[cols].apply(lambda x: isinstance(x,datetime.date)).any() == True:
            df[cols] = pd.to_datetime(df[cols], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S').astype(str)
    
    my_credentials = get_credentials('my_credentials.txt',schema)
    try:
        engine = connect_sqlalchemy(my_credentials)
        if dtyp == None:
            df_ora_dtyp = get_sqlalchemy_dtyp_dict(df,engine)
        else:
            df_ora_dtyp = dtyp
        engine.dispose()
    except Exception as err:
        print('Error: ' + str(err))
        engine.dispose()
    else:
        col_nan_all = df_sample[col_w_nan].loc[:,~df_sample[col_w_nan].columns.isin(df_col_w_nan_new)].columns
        keys, values = zip(*df_ora_dtyp.items())
        list_key = list(keys)
        list_val = list(values)
        for cols in col_nan_all:
            list_key_idx = list_key.index(cols)
            try:
                sql2py = list_val[list_key_idx].python_type
            except:
                sql2py = str
            if sql2py == datetime.datetime:
                df_col_w_nan_new[cols] = pd.to_datetime(df[cols],errors='coerce')
            elif sql2py == str:
                df_col_w_nan_new[cols] = df[cols].astype(str)
            elif sql2py == int:
                df_col_w_nan_new[cols] = pd.to_numeric(df[cols],errors='coerce').astype(pd.Int64Dtype())
            elif sql2py == float:
                df_col_w_nan_new[cols] = pd.to_numeric(df[cols],errors='coerce')
            elif sql2py == decimal.Decimal:
                df_col_w_nan_new[cols] = pd.to_numeric(df[cols],errors='coerce')
            else:
                df_col_w_nan_new[cols] = df[cols]

        df[col_w_nan] = df_col_w_nan_new
        del df_col_w_nan_new
    return df

def query_to_df(query,schema,chunk_size=1000000):
    # Open
    my_credentials = get_credentials('my_credentials.txt',schema)
    engine = connect_sqlalchemy(my_credentials)
    try:
        start_time = time.time()
        if chunk_size != None:
            # Create empty list
            dfl = []
            # Create empty dataframe
            dfs = pd.DataFrame()
            # Start Chunking SQL
            chunk_idx = 0
            start_time_file = time.time()
            start_time_chunk = time.time()
            for chunk in pd.read_sql_query(query,engine,chunksize=chunk_size):
                # Start Appending Data Chunks from SQL Result set into List
                dfl.append(chunk)
                chunk_idx += 1
                end_time_chunk = time.time()
                print('CHUNK {} appended to list. Time elapsed: {:2f}'.format(chunk_idx,end_time_chunk - start_time_chunk))
                start_time_chunk = time.time()
            end_time_file = time.time()
            print('Chunking done. Time elapsed: {:2f}'.format(end_time_file - start_time_file),'s')
            # Append data from list to dataframe
            start_time_df = time.time()
            dfs = pd.concat(dfl, ignore_index=True)
            dfs.columns = dfs.columns.str.upper()
            dfs.where(pd.notnull(dfs),np.nan,inplace=True)
            dfs = df_fix_dtype(dfs,schema)
            del dfl # delete variable to clear memory cache
            print('Chunk list deleted.')
            end_time_df = time.time()
            print('Dataframe created. Time elapsed: {:2f}'.format(end_time_df - start_time_df),'s')
        else:
            start_time_df = time.time()
            dfs = pd.read_sql_query(query,engine)
            dfs.columns = dfs.columns.str.upper()
            dfs.where(pd.notnull(dfs),np.nan,inplace=True)
            dfs = df_fix_dtype(dfs,schema)
            end_time_df = time.time()
            print('Dataframe created. Time elapsed: {:2f}'.format(end_time_df - start_time_df),'s')

        end_time = time.time()
        print('Execution time: {:2f}'.format(end_time - start_time),'s')
        engine.dispose()
    except Exception as err:
        print('Error: ' + str(err))
        engine.dispose()
    return dfs

def query_to_df_whole_timeline(query,schema,date_start,date_end,chunk_size=1000000):
    start_time = time.time()
    my_credentials = get_credentials('my_credentials.txt',schema)
    engine = connect_sqlalchemy(my_credentials)
    try:
        if chunk_size != None:
            # Create empty list
            dfl = []
            # Create empty dataframe
            dfs = pd.DataFrame()
            # Start Chunking SQL
            chunk_idx = 0
            start_time_file = time.time()
            start_time_chunk = time.time()
            for chunk in pd.read_sql_query(query,engine,chunksize=chunk_size):
                # Start Appending Data Chunks from SQL Result set into List
                dfl.append(chunk)
                chunk_idx += 1
                end_time_chunk = time.time()
                exec_time = end_time_chunk - start_time_chunk
                print('CHUNK {} appended to list. Time elapsed: {:2f}'.format(chunk_idx,exec_time))
                start_time_chunk = time.time()

            end_time_file = time.time()
            print('Chunking done. Time elapsed: {:2f}'.format(end_time_file - start_time_file),'s')
            # Append data from list to dataframe
            start_time_df = time.time()
            dfs = pd.concat(dfl, ignore_index=True)
            dfs.columns = dfs.columns.str.upper()
            dfs.where(pd.notnull(dfs),np.nan,inplace=True)
            dfs = df_fix_dtype(dfs,schema)
            is_df_created = 'YES'
            del dfl # delete variable to clear memory cache
            print('Chunk list deleted.')
            end_time_df = time.time()
            print('Dataframe created. Time elapsed: {:2f}'.format(end_time_df - start_time_df),'s')
        else:
            start_time_df = time.time()
            dfs = pd.read_sql_query(query,engine)
            dfs.columns = dfs.columns.str.upper()
            dfs.where(pd.notnull(dfs),np.nan,inplace=True)
            dfs = df_fix_dtype(dfs,schema)
            end_time_df = time.time()
            print('Dataframe created. Time elapsed: {:2f}'.format(end_time_df - start_time_df),'s')

        end_time = time.time()
        exec_time = end_time - start_time
        print('Execution time: {:2f}'.format(exec_time),'s')
        engine.dispose()
    except Exception as err:
        print('Error:' + str(err))
        engine.dispose()
        
    return dfs

def replace_word_containing_substring(text, substring, replacement):
    # Define the regex pattern to match whole words containing the substring
    pattern = r'\b\w*' + re.escape(substring.lower()) + r'\w*\b'
    # Use re.sub to replace the matched words with the replacement
    result = re.sub(pattern, replacement, text)
    return result

def var_query_fstring(text,c_date_start,c_date_end):
    query = replace_word_containing_substring(text, 'date_start', c_date_start)
    query = replace_word_containing_substring(query, 'date_end', c_date_end)
    return f'''{query}'''

def var_query_to_df(query_c_date,schema,date_start,date_end,time_out=60,temporal='days',increment=1,chunk_size = 1000000):
    # Open
    my_credentials = get_credentials('my_credentials.txt',schema)
    c_date_start = date_start
    c_date_end = date_end
    query = var_query_fstring(query_c_date,c_date_start,c_date_end)
    date_diff = days_between(c_date_start,c_date_end,temporal)
    engine = connect_sqlalchemy(my_credentials)
    try:
        print('Timeline: {} to {}'.format(c_date_start,c_date_end))
        start_time = time.time()
        # Create empty list
        dfl = []
        # Create empty dataframe
        dfs = pd.DataFrame()
        # Start Chunking SQL
        chunk_idx = 0
        start_time_file = time.time()
        start_time_chunk = time.time()
        
        from func_timeout import func_timeout, FunctionTimedOut
        try:
            dfs = func_timeout(time_out, query_to_df_whole_timeline, args=(query,schema,date_start,date_end))
        except FunctionTimedOut:
            if temporal.upper() == 'DAYS' or temporal.upper() == 'DAY' or temporal.upper() == 'DAILY' or temporal.upper() == 'DD':
                if increment == 1:
                    print(f'Process takes too long. Partitioning timeline by day and populating individually.')
                else:
                    print(f'Process takes too long. Partitioning timeline by {increment} days and populating individually.')
            elif temporal.upper() == 'WEEKS' or temporal.upper() == 'WEEK' or temporal.upper() == 'WEEKLY' or temporal.upper() == 'WW' or temporal.upper() == 'IW':
                if increment == 1:
                    print(f'Process takes too long. Partitioning timeline by week and populating individually.')
                else:
                    print(f'Process takes too long. Partitioning timeline by {increment} weeks and populating individually.')
            elif temporal.upper() == 'MONTHS' or temporal.upper() == 'MONTH' or temporal.upper() == 'MONTHLY' or temporal.upper() == 'MM':
                if increment == 1:
                    print(f'Process takes too long. Partitioning timeline by month and populating individually.')
                else:
                    print(f'Process takes too long. Partitioning timeline by {increment} months and populating individually.')
            elif temporal.upper() == 'YEARS' or temporal.upper() == 'YEAR' or temporal.upper() == 'YEARLY' or temporal.upper() == 'YY'  or temporal.upper() == 'YYYY':
                if increment == 1:
                    print(f'Process takes too long. Partitioning timeline by year and populating individually.')
                else:
                    print(f'Process takes too long. Partitioning timeline by {increment} years and populating individually.')
        except Exception as err:
            print('Error: ' + str(err))
            engine.dispose()
                
        if dfs.empty == True:
            var_start_time_df = time.time()
            var_dfs = []
            c_date_end = ora_date_inc(c_date_start,temporal,increment)
            for i in range(date_diff):
                print('Timeline: {} to {}'.format(c_date_start,c_date_end))
                query = var_query_fstring(query_c_date,c_date_start,c_date_end)
                if chunk_size != None:
                    # Create empty list
                    dfl = []
                    # Create empty dataframe
                    dfs = pd.DataFrame()
                    # Start Chunking SQL
                    chunk_idx = 0
                    start_time_file = time.time()
                    start_time_chunk = time.time()
                    for chunk in pd.read_sql_query(query,engine,chunksize=chunk_size):
                        # Start Appending Data Chunks from SQL Result set into List
                        dfl.append(chunk)
                        chunk_idx += 1
                        end_time_chunk = time.time()
                        print('CHUNK {} appended to list. Time elapsed: {:2f}'.format(chunk_idx,end_time_chunk - start_time_chunk))
                        start_time_chunk = time.time()
                    end_time_file = time.time()
                    print('Chunking done. Time elapsed: {:2f}'.format(end_time_file - start_time_file),'s')
                    # Append data from list to dataframe
                    start_time_df = time.time()
                    dfs = pd.concat(dfl, ignore_index=True)
                    dfs.columns = dfs.columns.str.upper()
                    dfs.where(pd.notnull(dfs),np.nan,inplace=True)
                    dfs = df_fix_dtype(dfs,schema)
                    del dfl # delete variable to clear memory cache
                    print('Chunk list deleted.')
                    end_time_df = time.time()
                    print('Partial Dataframe {} created. Time elapsed: {:2f}'.format(i+1,end_time_df - start_time_df),'s')
                else:
                    start_time_df = time.time()
                    dfs = pd.concat(dfl, ignore_index=True)
                    dfs.columns = dfs.columns.str.upper()
                    dfs.where(pd.notnull(dfs),np.nan,inplace=True)
                    dfs = df_fix_dtype(dfs,schema)
                    del dfl # delete variable to clear memory cache
                    print('Chunk list deleted.')
                    end_time_df = time.time()
                    print('Partial Dataframe {} created. Time elapsed: {:2f}'.format(i+1,end_time_df - start_time_df),'s')
                # Start Appending Data Chunks from SQL Result set into List
                var_dfs.append(dfs)
                del dfs # delete variable to clear memory cache
                end_time = time.time()
                print('Appended to main dataframe. Time elapsed: {:2f}'.format(end_time - start_time),'s')
                c_date_start = ora_date_inc(c_date_start,temporal,increment)
                c_date_end = ora_date_inc(c_date_end,temporal,increment)
                if ora_date_to_pydate(c_date_end) > ora_date_to_pydate(date_end):
                    c_date_end = date_end
                    if c_date_end == c_date_start:
                        c_date_end = ora_date_inc(c_date_end,'days',1)
            var_end_time_df = time.time()
            print('Dataframe created. Time elapsed: {:2f}'.format(var_end_time_df - var_start_time_df),'s')
            dfs = pd.concat(var_dfs, ignore_index=True)
    except Exception as err:
        print('Error: ' + str(err))
        engine.dispose()
    return dfs

def query_create_table(df,table_name,schema,engine,source_owner='TEST_OWNER',source_table='TEST_TABLE'):
    df_col = df.columns
    df_col_typ = df.dtypes
    df_col_list = [df_col[i] for i in range(df.shape[1])]
    df_col_str = str(df_col_list)
    df_col_str = df_col_str.replace("[","")
    df_col_str = df_col_str.replace("]","")
    df_col_str = df_col_str.replace(", ",", \n")
    col_dtyp_list = []
    df_col_oracle_typ = get_oracle_data_type(df_col_str,engine,source_owner=source_owner,source_table=source_table)
    for i in range(len(df_col)):
        if 'date' in str(df_col_typ[i]).lower() or 'dtime' in str(df_col[i]).lower():
            col_dtyp_list.append(df_col[i] + ' ' + 'DATE')
        elif df_col[i] in df_col_oracle_typ['COLUMN_NAME'].values:
            get_index = df_col_oracle_typ.index[df_col_oracle_typ['COLUMN_NAME']==df_col[i]]
            oracle_dtyp = str(df_col_oracle_typ['DATA_TYPE'][get_index].values)
            oracle_dtyp = oracle_dtyp.replace("[","")
            oracle_dtyp = oracle_dtyp.replace("]","")
            col_dtyp_list.append(df_col[i] + ' ' + oracle_dtyp)
        elif 'int' in str(df_col_typ[i]) or 'float' in str(df_col_typ[i]) or 'num' in str(df_col_typ[i]):
            col_dtyp_list.append(df_col[i] + ' ' + 'NUMBER')
        else:
            col_dtyp_list.append(df_col[i] + ' ' + 'VARCHAR2(80 CHAR)')

    col_dtyp = str(col_dtyp_list).replace("'","")
    col_dtyp = col_dtyp.replace('"',"")
    col_dtyp = col_dtyp.replace("[","")
    col_dtyp = col_dtyp.replace("]","")
    col_dtyp = col_dtyp.replace(", ",", \n")
    query = f'''
CREATE TABLE {schema.upper()}.{table_name.upper()}
(
{col_dtyp}
)
COMPRESS FOR ALL OPERATIONS
'''
    is_partition = input('Do you want to create a partition by range? [YES/NO]  |  ')
    if is_partition.upper() == 'YES' or is_partition.upper() == 'Y':
        date_reference = input('Input column for date_reference.  |  ')
        query = f'''
{query}
PARTITION BY RANGE ({date_reference.upper()})
INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))
(
PARTITION P0 VALUES LESS THAN (TO_DATE(' 2015-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS'))
)
'''
    else:
        query
    return f'''{query};'''

def query_create_gtt(df,table_name,schema,engine,source_owner='TEST_OWNER',source_table='TEST_TABLE'):
    df_col = df.columns
    df_col_typ = df.dtypes
    df_col_list = [df_col[i] for i in range(df.shape[1])]
    df_col_str = str(df_col_list)
    df_col_str = df_col_str.replace("[","")
    df_col_str = df_col_str.replace("]","")
    df_col_str = df_col_str.replace(", ",", \n")
    col_dtyp_list = []
    df_col_oracle_typ = get_oracle_data_type(df_col_str,engine,source_owner=source_owner,source_table=source_table)
    for i in range(len(df_col)):
        if 'date' in str(df_col_typ[i]).lower() or 'dtime' in str(df_col[i]).lower():
            col_dtyp_list.append(df_col[i] + ' ' + 'DATE')
        elif df_col[i] in df_col_oracle_typ['COLUMN_NAME'].values:
            get_index = df_col_oracle_typ.index[df_col_oracle_typ['COLUMN_NAME']==df_col[i]]
            oracle_dtyp = str(df_col_oracle_typ['DATA_TYPE'][get_index].values)
            oracle_dtyp = oracle_dtyp.replace("[","")
            oracle_dtyp = oracle_dtyp.replace("]","")
            col_dtyp_list.append(df_col[i] + ' ' + oracle_dtyp)
        elif 'int' in str(df_col_typ[i]) or 'float' in str(df_col_typ[i]) or 'num' in str(df_col_typ[i]):
            col_dtyp_list.append(df_col[i] + ' ' + 'NUMBER')
        else:
            col_dtyp_list.append(df_col[i] + ' ' + 'VARCHAR2(80 CHAR)')

    col_dtyp = str(col_dtyp_list).replace("'","")
    col_dtyp = col_dtyp.replace('"',"")
    col_dtyp = col_dtyp.replace("[","")
    col_dtyp = col_dtyp.replace("]","")
    col_dtyp = col_dtyp.replace(", ",", \n")
    query = f'''
CREATE TABLE {schema.upper()}.{table_name.upper()}
(
{col_dtyp}
)
ON COMMIT PRESERVE ROWS
;
'''
    return query

def insert_space_at_line_breaks_mrg(text,num_indent):
    text_split = text.split('\n')
    if len(text_split) > 1:
        text = insert_space_at_line_breaks(text,num_indent)
    else:
        text = text
    return text

def query_merge(df,table_name,schema,query,connection_key):
    def ora_conn_key(connection_key):
        if type(connection_key) == list and len(connection_key) > 1:
            new_connection_key = None
            for idx,conn_key in enumerate(connection_key):
                connection_key[idx] = conn_key.upper()
                if idx == 0:
                    new_connection_key = '\n' + 'DM1.'+conn_key.upper() + ' = DM2.'+conn_key.upper()
                elif conn_key.upper() == connection_key[-1]:
                    new_connection_key = new_connection_key + ' and \n' + 'DM1.'+conn_key.upper() + ' = DM2.'+conn_key.upper() + '\n'
                else:
                    new_connection_key = new_connection_key + ' and \n' + 'DM1.'+conn_key.upper() + ' = DM2.'+conn_key.upper()
        elif type(connection_key) == list and len(connection_key) == 1:
            connection_key = connection_key[0].upper()
            new_connection_key = 'DM1.'+connection_key + ' = DM2.'+connection_key
        else:
            connection_key = connection_key.upper()
            new_connection_key = 'DM1.'+connection_key + ' = DM2.'+connection_key
        return connection_key,new_connection_key
    
    df_col = df.columns.str.upper()
    conn_key = ora_conn_key(connection_key)
    
    # WHEN MATCHED THEN UPDATE SET
    df_col_list_mrg = [f"DM1.{df_col[i]} = DM2.{df_col[i]}" for i in range(df.shape[1]) if df_col[i] not in conn_key[0]]
    df_col_mrg = str(df_col_list_mrg).replace("'","")
    df_col_mrg = df_col_mrg.replace("[","")
    df_col_mrg = df_col_mrg.replace("]","")
    df_col_mrg = df_col_mrg.replace(", ",", \n")
    
    #WHEN NOT MATCHED THEN
    ##INSERT
    df_col_list_ins = ['DM1.' + df_col[i] for i in range(df.shape[1])]
    df_col_ins = str(df_col_list_ins).replace("'","")
    df_col_ins = df_col_ins.replace("[","")
    df_col_ins = df_col_ins.replace("]","")
    df_col_ins = df_col_ins.replace(", ",", \n")
    ##VALUES
    df_col_list_val = ['DM2.' + df_col[i] for i in range(df.shape[1])]
    df_col_val = str(df_col_list_val).replace("'","")
    df_col_val = df_col_val.replace("[","")
    df_col_val = df_col_val.replace("]","")
    df_col_val = df_col_val.replace(", ",", \n")
    query = f'''
MERGE INTO {schema.upper()}.{table_name.upper()} DM1
USING
(
{query}
) DM2 ON ({insert_space_at_line_breaks_mrg(conn_key[1],9)})

WHEN MATCHED THEN
UPDATE SET
{insert_space_at_line_breaks(df_col_mrg,6)}

WHEN NOT MATCHED THEN
INSERT
{' '*6}(
{insert_space_at_line_breaks(df_col_ins,6)}
{' '*6})
VALUES
{' '*6}(
{insert_space_at_line_breaks(df_col_val,6)}
{' '*6})
{' '*6};
'''
    return query

def query_create_package(df,table_name,schema,query,connection_key):
    from datetime import date
    merge_statement = query_merge(df,table_name,schema,query,connection_key)
    
    
    query_head = f'''
CREATE OR REPLACE PACKAGE PCKG_{table_name.upper()} AS

    /****************************************************************************************************************************
    
        NAME:      PCKG_{table_name.upper()}
        PURPOSE:   Package to update {table_name.upper()}
        REVISIONS: 
        Ver        Date        Author                     Description
        ---------  ----------  ---------------            -----------------------------------------------------------------------
        1.0        {str(date.today())}  {schema.upper()}       1. Created this package.
              
    ****************************************************************************************************************************/

    procedure proc_main (p_date_start date default trunc(sysdate-1), p_date_end date default trunc(sysdate));

END PCKG_{table_name.upper()};
'''
    
    
    query_body = f'''
CREATE OR REPLACE PACKAGE BODY PCKG_{table_name.upper()} AS
    
/*----------------
------------------
----- proc00 -----
------------------
----------------*/
             
            
    procedure pstats( actable varchar2, anperc number default 0.01)
    is
    begin
        ap_public.core_log_pkg.pstart( 'Stat:'||actable );
        dbms_stats.gather_table_stats ( ownname => user, tabname => actable, estimate_percent => anperc );
        ap_public.core_log_pkg.pend;
    end pstats
    ;
    
/*----------------
------------------
----- proc01 -----
------------------
----------------*/
             
            
    procedure ptruncate( actable varchar2, anperc number default 0.01)
    is
    begin
        ap_public.core_log_pkg.pstart( 'trunc:'||actable );
        execute immediate 'truncate table '||actable ; 
        ap_public.core_log_pkg.pend ;
    end ptruncate
    ;
    
/*----------------
------------------
----- proc02 -----
------------------
----------------*/

    procedure proc_main
    (p_date_start date default trunc(sysdate-1), p_date_end date default trunc(sysdate))
    as
    --=== parametrization ===--
    
    c_date_start constant date := trunc(p_date_start);
    c_date_end constant date := trunc(p_date_end);
    
    
    begin
        ap_public.core_log_pkg.pinit( user, $$plsql_unit ) ;
    
        /*--------------------------------------------------------------------------------------------------------------------------------------------
            Procedure in updating POS EC DATAMART TREE main table
                VERSION         DATE                        USER                            NOTE
                  1         {str(date.today())}              {schema.upper()}            Created this procedure.
                  
        --------------------------------------------------------------------------------------------------------------------------------------------*/
        --ptruncate('{schema.upper()}.{table_name.upper()}');
        
        ap_public.core_log_pkg.pstart('merge: {schema.upper()}.{table_name.upper()} ' || c_date_start || ' - ' || c_date_end);
{insert_space_at_line_breaks(merge_statement,8)}
        pstats('{table_name.upper()}');
        ap_public.core_log_pkg.pfinish;
        commit;
              
        exception when others then
        ap_public.core_log_pkg.perror; 
        raise;

    end proc_main;
  
END PCKG_{table_name.upper()};
'''
    return query_head,query_body

def query_merge_when_matched_and_or_not_matched(df,table_name,schema):
    df_col = df.columns
    
    # WHEN MATCHED THEN UPDATE SET
    df_col_list_mrg = ['DM1.' + df_col[i] + ' ' + '=' + ' ' + 'DM2.' + df_col[i] for i in range(df.shape[1])]
    df_col_mrg = str(df_col_list_mrg).replace("'","")
    df_col_mrg = df_col_mrg.replace("[","")
    df_col_mrg = df_col_mrg.replace("]","")
    df_col_mrg = df_col_mrg.replace(", ",", \n")
    
    #WHEN NOT MATCHED THEN
    ##INSERT
    df_col_list_ins = ['DM1.' + df_col[i] for i in range(df.shape[1])]
    df_col_ins = str(df_col_list_ins).replace("'","")
    df_col_ins = df_col_ins.replace("[","")
    df_col_ins = df_col_ins.replace("]","")
    df_col_ins = df_col_ins.replace(", ",", \n")
    ##VALUES
    df_col_list_val = ['DM2.' + df_col[i] for i in range(df.shape[1])]
    df_col_val = str(df_col_list_val).replace("'","")
    df_col_val = df_col_val.replace("[","")
    df_col_val = df_col_val.replace("]","")
    df_col_val = df_col_val.replace(", ",", \n")
    query = f'''
WHEN MATCHED THEN
UPDATE SET
{df_col_mrg}

WHEN NOT MATCHED THEN
INSERT
(
{df_col_ins}
)
VALUES
(
{df_col_val}
)
;
COMMIT;
'''
    return query

def query_insert_when_not_matched(df,table_name,schema):
    df_col = df.columns
    df_col_list_ins = ['DM1.'+df_col[i] for i in range(df.shape[1])]
    df_col_ins = str(df_col_list_ins).replace("'","")
    df_col_ins = df_col_ins.replace("[","")
    df_col_ins = df_col_ins.replace("]","")
    df_col_ins = df_col_ins.replace(", ",", \n")
    df_col_list_val = ['DM2.'+df_col[i] for i in range(df.shape[1])]
    df_col_val = str(df_col_list_val).replace("'","")
    df_col_val = df_col_val.replace("[","")
    df_col_val = df_col_val.replace("]","")
    df_col_val = df_col_val.replace(", ",", \n")
    query = f'''
WHEN NOT MATCHED THEN
INSERT
(
{df_col_ins}
)
VALUES
(
{df_col_val}
)
;
'''
    return query

def get_oracle_data_type(df_col_str,engine,source_owner='TEST_OWNER',source_table='TEST_TABLE'):
    query = f'''
with tab_base as
(
select
       column_name
      ,data_type
      ,max(char_length) char_length
      ,count(*) antidup
from all_tab_columns
where 1=1
      and owner = '{source_owner.upper()}'
      and table_name = '{source_table.upper()}'
      and column_name in ({df_col_str})
group by
       column_name
      ,data_type
)
select
       column_name
      ,case
            when data_type like '%CHAR%' then data_type ||'('||char_length||' CHAR)'
            else data_type
       end                                                                      data_type
from tab_base
'''
    df = pd.read_sql_query(query,engine)
    df.columns = df.columns.str.upper()
    return df

def apply_ora_hint(query):
    # Oracle only applies hint after the first select statement
    word = 'select'
    word_len = len(word)
    word_idx = query.find(word)

    hint_start = '/*+'
    hint_start_len = len(hint_start)
    hint_start_idx = query.find(hint_start)

    hint_end = '*/'
    hint_end_re = re.escape(hint_end) # workaround when keyword starts with special character
    hint_end_len = len(hint_end)
    hint_end_idxs = [i.start() for i in re.finditer(hint_end_re, query) if i.start() > hint_start_idx]
    for i in range(len(hint_end_idxs)):
        if hint_end_idxs[i] > hint_start_idx:
            idx = (np.abs(hint_end_idxs[i] - hint_start_idx)).argmin()
            hint_end_idx = hint_end_idxs[idx]

    hint = '/*+ materialize */'
    hint_len = len(hint)
    hint_idx = query.find(hint)

    if hint_start in query:
        query = query.replace(query[hint_start_idx:hint_end_idx+hint_end_len],hint)
        if query[word_len+word_idx+1:hint_len+hint_idx] == hint:
            query = query
        else:
            query = query[:word_idx] + word + ' ' + hint + query[word_len+word_idx:]
    else:
        query = query[:word_idx] + word + ' ' + hint + query[word_len+word_idx:]
    return query

def fix_ora_string(string,substr="'",new_substr="''"):
    if substr in string:
        if string[0] == substr and string[-1] == substr:
            string_list = list(string[1:-1])
        else:
            string_list = list(string)
        substr_idxs = [i.start() for i in re.finditer(substr, string) if i.start() > 0]
        for idx in substr_idxs:
            string_list[idx] = new_substr
        string = ''.join(string_list)
    return string

def fix_ora_col_length(df1,df2,engine,cursor,schema,table_name,source_owner,source_table):
    df1_col = df1.columns
    df1_col_list = [df1_col[i] for i in range(df1.shape[1])]
    df1_col_str = str(df1_col_list)
    df1_col_str = df1_col_str.replace("[","")
    df1_col_str = df1_col_str.replace("]","")
    df1_col_str = df1_col_str.replace(", ",", \n")
    df1_dtyp = get_oracle_data_type(df1_col_str,engine,source_owner=schema,source_table=table_name)

    df2_col = df2.columns
    df2_col_list = [df2_col[i] for i in range(df2.shape[1])]
    df2_col_str = str(df2_col_list)
    df2_col_str = df2_col_str.replace("[","")
    df2_col_str = df2_col_str.replace("]","")
    df2_col_str = df2_col_str.replace(", ",", \n")
    df2_dtyp = get_oracle_data_type(df2_col_str,engine,source_owner=source_owner,source_table=source_table)

    for col in df1_col:
        idx1 = df1_dtyp[df1_dtyp['COLUMN_NAME'] == col].index[0]
        idx2 = df2_dtyp[df2_dtyp['COLUMN_NAME'] == col].index[0]
        if 'VARCHAR2' in df1_dtyp.loc[idx1,'DATA_TYPE'] and df1_dtyp.loc[idx1,'CHAR_LENGTH'] < df2_dtyp.loc[idx2,'CHAR_LENGTH']:
            print(f'''Modifying {schema}.{table_name}.{df1_dtyp.loc[idx1,'COLUMN_NAME']} data type from VARCHAR2({df1_dtyp.loc[idx1,'CHAR_LENGTH']} CHAR) to VARCHAR2({df2_dtyp.loc[idx2,'CHAR_LENGTH']} CHAR).''')
            cursor.execute(text(f'''ALTER TABLE {schema}.{table_name} MODIFY {df1_dtyp.loc[idx1,'COLUMN_NAME']} VARCHAR2({df2_dtyp.loc[idx2,'CHAR_LENGTH']} CHAR)'''))

def sql_where_condition(condition):
    if condition == None:
        return 'where 1=1'
    elif isinstance(condition,list) == True:
        return 'where ' + ' and '.join(condition)
    else:
        return 'where ' + condition
            
def insert_row_to_sql(df,df_base,schema,table_name,source_owner,source_table):
    import time
    time_elapsed = time.time()
    my_credentials = get_credentials('my_credentials.txt',schema)
    engine = connect_sqlalchemy(my_credentials)
    cursor = engine.connect()
    col_intersect = df.columns.intersection(df_base.columns)
    df = df_fix_dtype(df[col_intersect],schema)
    for cols in df.columns:
        if ((check_dtime(df[cols])['is_date'] == True or check_dtime(df[cols])['is_timestamp'] == True) and
            (df[df[cols].notnull()][cols].apply(lambda x: isinstance(x,int)).all() == False)
           ):
            if check_dtime(df[cols])['is_date'] == True and check_dtime(df[cols])['is_timestamp'] == False:
                dt = df[cols].apply(lambda x: f"TO_DATE('{str(x).split(' ')[0]}', 'YYYY-MM-DD')" if is_nan(x) == False else x)
                df[cols] = dt
            elif check_dtime(df[cols])['is_date'] == True and check_dtime(df[cols])['is_timestamp'] == True:
                dt = df[cols].apply(lambda x: f"TO_DATE('{str(x)}', 'YYYY-MM-DD HH24:MI:SS')" if is_nan(x) == False else x)
                df[cols] = dt
    
    # Fix Oracle data types of new and current table before inserting rows.
    fix_ora_col_length(df_base,df,engine,cursor,schema,table_name,source_owner,source_table)
    for i in range(df.shape[0]):
        val = df.iloc[i]
        col = df.columns

        _dict = {}
        for idx in range(df.shape[1]):
            if is_nan(val[idx]) == False:
                if isinstance(val[idx],str) == True and 'TO_DATE' not in val[idx]:
                    val[idx] = fix_ora_string(val[idx])
                    val[idx] = fix_ora_string(val[idx],'"','"temp_quotation"')
                _dict.update({col[idx]: val[idx]})
        col = str(list(_dict.keys()))
        col = col.replace("[","")
        col = col.replace("]","")
        col = col.replace("'","")
        val = str(list(_dict.values()))
        val = val.replace("[","")
        val = val.replace("]","")
        val = val.replace('"',"'")
        val = val.replace("'temp_quotation'",'"')
        query = f'INSERT INTO {schema}.{table_name} \n(\n{col}\n)\nVALUES\n(\n{val}\n)'
        query = query.replace('"TO_DATE','TO_DATE')
        query = query.replace("'TO_DATE",'TO_DATE')
        query = query.replace(')",','),')
        query = query.replace("')',","'),")
        try:
            cursor.execute(text(query))
        except Exception as err:
            print(f'Row {i} failed.')
            cursor.rollback()
            print('Rollback.')
            time_elapsed = get_time_elapsed(time_elapsed)
            cursor.close()
            engine.dispose()
            raise
    cursor.commit()
    print('Commit.')
    cursor.close()
    engine.dispose()
    time_elapsed = get_time_elapsed(time_elapsed)

def merge_to_sql(df,table_name,schema,query,connection_key):
    import time
    time_elapsed = time.time()
    my_credentials = get_credentials('my_credentials.txt',schema)
    engine = connect_sqlalchemy(my_credentials)
    cursor = engine.connect()
    merge_statement = query_merge(df,table_name,schema,query,connection_key)
    try:
        query = f'''BEGIN
        {merge_statement}
        COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;'''
        cursor.execute(text(query))
    except Exception as err:
        cursor.rollback()
        print('Merge failed. Rollback.')
        time_elapsed = get_time_elapsed(time_elapsed)
        cursor.close()
        engine.dispose()
        raise
    else:
        cursor.commit()
        cursor.close()
        engine.dispose()
        print('Merged successfully. Commit.')
        time_elapsed = get_time_elapsed(time_elapsed)

def var_merge_to_sql(table_name,schema,query_c_date,date_start,date_end,
                     connection_key,time_out=60,temporal='days',increment=1):
    import time
    # Open
    my_credentials = get_credentials('my_credentials.txt',schema)
    c_date_start = date_start
    c_date_end = date_end
    date_diff = days_between(c_date_start,c_date_end,temporal)
    engine = connect_sqlalchemy(my_credentials)
    cursor = engine.connect()
    query_sample_c_date = f'''
select * from {schema}.{table_name}
where 1=1
      and date_application = {ora_date_inc(c_date_start,'days',-5)}
      and rownum = 1
'''
    df = pd.read_sql_query(query_sample_c_date,engine)
    df.columns = df.columns.str.upper()
    
    try:
        print('Timeline: {} to {}'.format(c_date_start,c_date_end))
        time_elapsed = time.time()
        # Set initial value for merge boolean
        is_merge_done = False
        def func_merge(df,table_name,schema,query_c_date,date_start,date_end,connection_key):
            time_elapsed = time.time()
            query = var_query_fstring(query_c_date,date_start,date_end)
            merge_statement = query_merge(df,table_name,schema,query,connection_key)
            try:
                query = f'''BEGIN
        {merge_statement}
        COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;'''
                cursor.execute(text(query))
            except Exception as err:
                cursor.rollback()
                print('Merge failed. Rollback.')
                time_elapsed = get_time_elapsed(time_elapsed)
                cursor.close()
                engine.dispose()
                raise
            else:
                cursor.commit()
                print('Merged successfully. Commit.')
                time_elapsed = get_time_elapsed(time_elapsed)
                return True
        from func_timeout import func_timeout, FunctionTimedOut
        try:
            is_merge_done = func_timeout(time_out, func_merge, args=(df,table_name,schema,query_c_date,date_start,date_end,connection_key))
        except FunctionTimedOut:
            if temporal.upper() == 'DAYS' or temporal.upper() == 'DAY' or temporal.upper() == 'DAILY' or temporal.upper() == 'DD':
                if increment == 1:
                    print(f'Process takes too long. Partitioning timeline by day and populating individually.')
                else:
                    print(f'Process takes too long. Partitioning timeline by {increment} days and populating individually.')
            elif temporal.upper() == 'WEEKS' or temporal.upper() == 'WEEK' or temporal.upper() == 'WEEKLY' or temporal.upper() == 'WW' or temporal.upper() == 'IW':
                if increment == 1:
                    print(f'Process takes too long. Partitioning timeline by week and populating individually.')
                else:
                    print(f'Process takes too long. Partitioning timeline by {increment} weeks and populating individually.')
            elif temporal.upper() == 'MONTHS' or temporal.upper() == 'MONTH' or temporal.upper() == 'MONTHLY' or temporal.upper() == 'MM':
                if increment == 1:
                    print(f'Process takes too long. Partitioning timeline by month and populating individually.')
                else:
                    print(f'Process takes too long. Partitioning timeline by {increment} months and populating individually.')
            elif temporal.upper() == 'YEARS' or temporal.upper() == 'YEAR' or temporal.upper() == 'YEARLY' or temporal.upper() == 'YY'  or temporal.upper() == 'YYYY':
                if increment == 1:
                    print(f'Process takes too long. Partitioning timeline by year and populating individually.')
                else:
                    print(f'Process takes too long. Partitioning timeline by {increment} years and populating individually.')
            time_elapsed = get_time_elapsed(time_elapsed)
        except Exception as err:
            print('Error: ' + str(err))
            cursor.close()
            engine.dispose()
            time_elapsed = get_time_elapsed(time_elapsed)
        
        chunk_merge_start_time = time.time()
        if is_merge_done == False:
            c_date_end = ora_date_inc(c_date_start,temporal,increment)
            for i in range(date_diff):
                print('Timeline: {} to {}'.format(c_date_start,c_date_end))
                func_merge(df,table_name,schema,query_c_date,c_date_start,c_date_end,connection_key)
                c_date_start = ora_date_inc(c_date_start,temporal,increment)
                c_date_end = ora_date_inc(c_date_end,temporal,increment)
                if ora_date_to_pydate(c_date_end) > ora_date_to_pydate(date_end):
                    c_date_end = date_end
                    if c_date_end == c_date_start:
                        c_date_end = ora_date_inc(c_date_end,'days',1)
        chunk_merge_end_time = time.time()
        print('Chunks merge done. Time elapsed: {:2f}'.format(chunk_merge_end_time - chunk_merge_start_time),'s')
    except Exception as err:
        print('Error: ' + str(err))
        cursor.close()
        engine.dispose()
        
def split_dataframe(df, chunk_size): 
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    return chunks

def query_to_excel(schema,query,fname,sheet_name=None,chunk_size=1000000):
    my_credentials = get_credentials('my_credentials.txt',schema)
    fname_check_result = fname_check(fname,schema)
    is_file = fname_check_result['is_file']
    fname = fname_check_result['fname']
    folder_path = fname_check_result['folder_path']
    table_name = fname_check_result['table_name']
    
    if sheet_name == None:
        sheet_name = 'Sheet1'
        
    start_time = time.time()
    
    if is_file == True:
        print('File already exists.')
        is_overwrite = input('Do you want to overwrite the existing file? [YES/NO]  |  ')
        if is_overwrite.upper() == 'YES' or is_overwrite.upper() == 'Y':
            # Open
            engine = connect_sqlalchemy(my_credentials)
            try:
                start_time = time.time()
                if chunk_size != None:
                    # Create empty list
                    dfl = []
                    # Create empty dataframe
                    dfs = pd.DataFrame()
                    # Start Chunking SQL
                    chunk_idx = 0
                    start_time_file = time.time()
                    start_time_chunk = time.time()
                    for chunk in pd.read_sql_query(query,engine,chunksize=chunk_size):
                        # Start Appending Data Chunks from SQL Result set into List
                        dfl.append(chunk)
                        chunk_idx += 1
                        end_time_chunk = time.time()
                        print('CHUNK {} appended to list. Time elapsed: {:2f}'.format(chunk_idx,end_time_chunk - start_time_chunk))
                        start_time_chunk = time.time()
                    end_time_file = time.time()
                    engine.dispose()
                    print('Chunking done. Time elapsed: {:2f}'.format(end_time_file - start_time_file),'s')
                    # Append data from list to dataframe
                    start_time_df = time.time()
                    dfs = pd.concat(dfl, ignore_index=True)
                    dfs.columns = dfs.columns.str.upper()
                    dfs.where(pd.notnull(dfs),np.nan,inplace=True)
                    dfs = df_fix_dtype(dfs,schema)
                    del dfl # delete variable to clear memory cache
                    end_time_df = time.time()
                    print('Dataframe created. Time elapsed: {:2f}'.format(end_time_df - start_time_df),'s')
                else:
                    start_time_df = time.time()
                    dfs = pd.read_sql_query(query,engine)
                    dfs.columns = dfs.columns.str.upper()
                    dfs.where(pd.notnull(dfs),np.nan,inplace=True)
                    dfs = df_fix_dtype(dfs,schema)
                    end_time_df = time.time()
                    print('Dataframe created. Time elapsed: {:2f}'.format(end_time_df - start_time_df),'s')

                wb = Workbook()
                # Chunk dataframe and insert into spreah sheet separately
                if dfs.shape[0] >= chunk_size:
                    start_time_file = time.time()
                    wb = Workbook()
                    chunks = split_dataframe(dfs,chunk_size)
                    for i in range(len(chunks)):
                        start_time_chunk = time.time()
                        df_col = [chunks[i].columns]
                        df_data = chunks[i].values.tolist()
                        df_data = [['' if (str(val).upper() == 'NAN') or (str(val).upper() == 'NONE') or (str(val).upper() == 'NAT') else val for val in row] for row in df_data]
                        values = df_col + df_data
                        ws = wb.new_sheet('CHUNK_'+str(i+1), data=values)
                        chunks[i] = None # replace chunk with None to clear memory cache
                        end_time_chunk = time.time()
                        print('New sheet created for CHUNK {}. Time elapsed: {:2f}'.format(i+1,end_time_chunk - start_time_chunk))
                    del chunks # delete variable to clear memory cache
                    wb.save(fname)
                    end_time_file = time.time()
                    print('Workbook created. Time elapsed: {:2f}'.format(end_time_file - start_time_file),'s')
                else:
                    start_time_file = time.time()
                    wb = Workbook()
                    df_col = [dfs.columns]
                    df_data = dfs.values.tolist()
                    df_data = [['' if (str(val).upper() == 'NAN') or (str(val).upper() == 'NONE') or (str(val).upper() == 'NAT') else val for val in row] for row in df_data]
                    values = df_col + df_data
                    ws = wb.new_sheet(sheet_name, data=values)
                    wb.save(fname)
                    end_time_file = time.time()
                    print('Workbook created. Time elapsed: {:2f}'.format(end_time_file - start_time_file),'s')
                end_time = time.time()
                print('Execution time: {:2f}'.format(end_time - start_time),'s')
            except Exception as err:
                print('Error: ' + str(err))
                engine.dispose()
    else:
        # Open
        engine = connect_sqlalchemy(my_credentials)
        try:
            start_time = time.time()
            if chunk_size != None:
                # Create empty list
                dfl = []
                # Create empty dataframe
                dfs = pd.DataFrame()
                # Start Chunking SQL
                chunk_idx = 0
                start_time_file = time.time()
                start_time_chunk = time.time()
                for chunk in pd.read_sql_query(query,engine,chunksize=chunk_size):
                    # Start Appending Data Chunks from SQL Result set into List
                    dfl.append(chunk)
                    chunk_idx += 1
                    end_time_chunk = time.time()
                    print('CHUNK {} appended to list. Time elapsed: {:2f}'.format(chunk_idx,end_time_chunk - start_time_chunk))
                    start_time_chunk = time.time()
                end_time_file = time.time()
                engine.dispose()
                print('Chunking done. Time elapsed: {:2f}'.format(end_time_file - start_time_file),'s')
                # Append data from list to dataframe
                start_time_df = time.time()
                dfs = pd.concat(dfl, ignore_index=True)
                dfs.columns = dfs.columns.str.upper()
                dfs.where(pd.notnull(dfs),np.nan,inplace=True)
                dfs = df_fix_dtype(dfs,schema)
                end_time_df = time.time()
                print('Dataframe created. Time elapsed: {:2f}'.format(end_time_df - start_time_df),'s')
            else:
                start_time_df = time.time()
                dfs = pd.read_sql_query(query,engine)
                dfs.columns = dfs.columns.str.upper()
                dfs.where(pd.notnull(dfs),np.nan,inplace=True)
                dfs = df_fix_dtype(dfs,schema)
                end_time_df = time.time()
                print('Dataframe created. Time elapsed: {:2f}'.format(end_time_df - start_time_df),'s')
                

            wb = Workbook()
            # Chunk dataframe and insert into spreah sheet separately
            if dfs.shape[0] >= chunk_size:
                start_time_file = time.time()
                wb = Workbook()
                chunks = split_dataframe(dfs,chunk_size)
                for i in range(len(chunks)):
                    start_time_chunk = time.time()
                    df_col = [chunks[i].columns]
                    df_data = chunks[i].values.tolist()
                    df_data = [['' if (str(val).upper() == 'NAN') or (str(val).upper() == 'NONE') or (str(val).upper() == 'NAT') else val for val in row] for row in df_data]
                    values = df_col + df_data
                    ws = wb.new_sheet('CHUNK_'+str(i+1), data=values)
                    end_time_chunk = time.time()
                    print('New sheet created for CHUNK {}. Time elapsed: {:2f}'.format(i+1,end_time_chunk - start_time_chunk))
                del chunks # delete variable to clear memory cache
                fname = folder_path+'/'+table_name+''.join(pathlib.Path(fname).suffixes)
                wb.save(fname)
                end_time_file = time.time()
                print('Workbook created. Time elapsed: {:2f}'.format(end_time_file - start_time_file),'s')
            else:
                start_time_file = time.time()
                wb = Workbook()
                df_col = [dfs.columns]
                df_data = dfs.values.tolist()
                df_data = [['' if (str(val).upper() == 'NAN') or (str(val).upper() == 'NONE') or (str(val).upper() == 'NAT') else val for val in row] for row in df_data]
                values = df_col + df_data
                ws = wb.new_sheet(sheet_name, data=values)
                fname = folder_path+'/'+table_name+''.join(pathlib.Path(fname).suffixes)
                wb.save(fname)
                end_time_file = time.time()
                print('Workbook created. Time elapsed: {:2f}'.format(end_time_file - start_time_file),'s')
            end_time = time.time()
            print('Execution time: {:2f}'.format(end_time - start_time),'s')
        except Exception as err:
            print('Error: ' + str(err))
            engine.dispose()
    return dfs

def df_to_excel(df,schema,fname,sheet_name=None,chunk_size=1000000,dtyp=None):
    my_credentials = get_credentials('my_credentials.txt',schema)
    fname_check_result = fname_check(fname,schema)
    is_file = fname_check_result['is_file']
    fname = fname_check_result['fname']
    folder_path = fname_check_result['folder_path']
    table_name = fname_check_result['table_name']
    
    if sheet_name == None:
        sheet_name = 'Sheet1'
    
    df.where(pd.notnull(df),np.nan,inplace=True)
    df = df_fix_dtype(df,schema,dtyp=dtyp)
    start_time = time.time()
    
    if is_file == True:
        print('File already exists.')
        is_overwrite = input('Do you want to overwrite the existing file? [YES/NO]  |  ')
        if is_overwrite.upper() == 'YES' or is_overwrite.upper() == 'Y':
            try:                    
                wb = Workbook()
                # Chunk dataframe and insert into spreah sheet separately
                if df.shape[0] >= chunk_size:
                    start_time_file = time.time()
                    wb = Workbook()
                    chunks = split_dataframe(df,chunk_size)
                    for i in range(len(chunks)):
                        start_time_chunk = time.time()
                        df_col = [chunks[i].columns]
                        df_data = chunks[i].values.tolist()
                        df_data = [['' if (str(val).upper() == 'NAN') or (str(val).upper() == 'NONE') or (str(val).upper() == 'NAT') else val for val in row] for row in df_data]
                        values = df_col + df_data
                        ws = wb.new_sheet('CHUNK_'+str(i+1), data=values)
                        end_time_chunk = time.time()
                        print('New sheet created for CHUNK {}. Time elapsed: {:2f}'.format(i+1,end_time_chunk - start_time_chunk))
                    del chunks # delete variable to clear memory cache
                    wb.save(fname)
                    end_time_file = time.time()
                    print('Workbook created. Time elapsed: {:2f}'.format(end_time_file - start_time_file),'s')
                else:
                    start_time_file = time.time()
                    wb = Workbook()
                    df_col = [df.columns]
                    df_data = df.values.tolist()
                    df_data = [['' if (str(val).upper() == 'NAN') or (str(val).upper() == 'NONE') or (str(val).upper() == 'NAT') else val for val in row] for row in df_data]
                    values = df_col + df_data
                    ws = wb.new_sheet(sheet_name, data=values)
                    wb.save(fname)
                    end_time_file = time.time()
                    print('Workbook created. Time elapsed: {:2f}'.format(end_time_file - start_time_file),'s')
            except Exception as err:
                print('Error: ' + str(err))
        elif is_overwrite.upper() == 'NO' or is_overwrite.upper() == 'N':
            try:
                start_time_file = time.time()
                # Load the existing workbook
                book = load_workbook(fname)
                # Open an ExcelWriter object and specify the engine to use openpyxl
                with pd.ExcelWriter(folder_path, engine='openpyxl') as writer:
                    writer.book = book
                    # Add the new DataFrame as a new sheet
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    # Save the workbook
                    writer.save()
                    end_time_file = time.time()
                    print('Sheet appended. Time elapsed: {:2f}'.format(end_time_file - start_time_file),'s')
            except Exception as err:
                print('Error: ' + str(err))
    else:
        try:                    
            wb = Workbook()
            # Chunk dataframe and insert into spreah sheet separately
            if df.shape[0] >= chunk_size:
                start_time_file = time.time()
                wb = Workbook()
                chunks = split_dataframe(df,chunk_size)
                for i in range(len(chunks)):
                    start_time_chunk = time.time()
                    df_col = [chunks[i].columns]
                    df_data = chunks[i].values.tolist()
                    df_data = [['' if (str(val).upper() == 'NAN') or (str(val).upper() == 'NONE') or (str(val).upper() == 'NAT') else val for val in row] for row in df_data]
                    values = df_col + df_data
                    ws = wb.new_sheet('CHUNK_'+str(i+1), data=values)
                    end_time_chunk = time.time()
                    print('New sheet created for CHUNK {}. Time elapsed: {:2f}'.format(i+1,end_time_chunk - start_time_chunk))
                del chunks # delete variable to clear memory cache
                fname = folder_path+'/'+table_name+''.join(pathlib.Path(fname).suffixes)
                wb.save(fname)
                end_time_file = time.time()
                print('Workbook created. Time elapsed: {:2f}'.format(end_time_file - start_time_file),'s')
            else:
                start_time_file = time.time()
                wb = Workbook()
                df_col = [df.columns]
                df_data = df.values.tolist()
                df_data = [['' if (str(val).upper() == 'NAN') or (str(val).upper() == 'NONE') or (str(val).upper() == 'NAT') else val for val in row] for row in df_data]
                values = df_col + df_data
                ws = wb.new_sheet(sheet_name, data=values)
                fname = folder_path+'/'+table_name+''.join(pathlib.Path(fname).suffixes)
                wb.save(fname)
                end_time_file = time.time()
                print('Workbook created. Time elapsed: {:2f}'.format(end_time_file - start_time_file),'s')
        except Exception as err:
            print('Error: ' + str(err))

def get_py_dtyp_as_dict(df):
    import datetime
    from datetime import time
    from datetime import date
    
    df_dtyp = pd.DataFrame(columns=['COLUMN_NAME','DTYP'])
    df_dtyp['COLUMN_NAME'] = df.columns
    for cols,rows in df.loc[:10000,:].items():
        idx = df_dtyp.index[df_dtyp['COLUMN_NAME']==cols]
        is_int_type = rows[rows.notnull()].apply(lambda x: isinstance(x,int)).any()
        is_int64_type = rows[rows.notnull()].apply(lambda x: isinstance(x,np.int64)).any()
        is_float_type = rows[rows.notnull()].apply(lambda x: isinstance(x,float)).any()
        is_float64_type = rows[rows.notnull()].apply(lambda x: isinstance(x,np.float64)).any()
        is_date_type = check_dtime(rows)['is_date']
        is_datetime_type = check_dtime(rows)['is_timestamp']
        is_str_type = rows[rows.notnull()].apply(lambda x: isinstance(x,str)).any()
        unique_dtyp = rows[rows.notnull()].apply(lambda x: type(x)).unique()


        if is_int_type == True or is_int64_type == True:
            df_dtyp.loc[idx,'DTYP'] = int
        elif is_float_type == True or is_float64_type == True:
            df_dtyp.loc[idx,'DTYP'] = float
        elif is_datetime_type == True:
            df_dtyp.loc[idx,'DTYP'] = datetime
        elif is_date_type == True:
            df_dtyp.loc[idx,'DTYP'] = date
        elif 'dtime' in str(cols).lower():
            df_dtyp.loc[idx,'DTYP'] = datetime
        elif 'date' in str(cols).lower():
            df_dtyp.loc[idx,'DTYP'] = date
        elif is_str_type == True:
            df_dtyp.loc[idx,'DTYP'] = str
        else:
            if len(unique_dtyp) == 0:
                df_dtyp.loc[idx,'DTYP'] = float
            else:
                df_dtyp.loc[idx,'DTYP'] = unique_dtyp[0]
                
    return dict(df_dtyp.values)
            
def get_sqlalchemy_dtyp_dict(df,engine,source_owner='TEST_OWNER',source_table='TEST_TABLE'):
    import time
    from datetime import date
    from datetime import datetime
    from sqlalchemy import types
    from sqlalchemy.dialects.oracle import (
                                            DATE,
                                            TIMESTAMP,
                                            NUMBER,
                                            VARCHAR2
                                            )
    time_elapsed = time.time()
    df_sample = df.loc[:10000,:]
    df = df_sample[df_sample.notnull()]
    df_col = df.columns
    df_col_typ = df.dtypes
    df_col_list = [cols for cols in df_col]
    df_col_str = str(df_col_list)
    df_col_str = df_col_str.replace("[","")
    df_col_str = df_col_str.replace("]","")
    df_col_str = df_col_str.replace(", ",", \n")
    dtyp = []
    print('Fetching Oracle data types.')
    df_col_oracle_typ = get_oracle_data_type(df_col_str,engine,source_owner=source_owner,source_table=source_table)
    time_elapsed = get_time_elapsed(time_elapsed)
    print('Fetching python data types.')
    py_dtyp_dict = get_py_dtyp_as_dict(df)
    time_elapsed = get_time_elapsed(time_elapsed)
    print('Converting to sqlalchemy data types.')
    for cols in df_col:
        if cols in df_col_oracle_typ['COLUMN_NAME'].values:
            get_index = df_col_oracle_typ.index[df_col_oracle_typ['COLUMN_NAME']==cols]
            oracle_dtyp = str(df_col_oracle_typ['DATA_TYPE'][get_index].values)
            oracle_dtyp = oracle_dtyp.replace("[","")
            oracle_dtyp = oracle_dtyp.replace("]","")
            pair = []
            pair.append(cols)
            pair.append(oracle_dtyp)
            dtyp.append(pair)
        elif py_dtyp_dict[cols] == datetime:
            pair = []
            pair.append(cols)
            pair.append('TIMESTAMP')
            dtyp.append(pair)
        elif py_dtyp_dict[cols] == date:
            pair = []
            pair.append(cols)
            pair.append('DATE')
            dtyp.append(pair)
        elif py_dtyp_dict[cols] == int:
            pair = []
            pair.append(cols)
            pair.append('INTEGER')
            dtyp.append(pair)
        elif py_dtyp_dict[cols] == float:
            pair = []
            pair.append(cols)
            pair.append('FLOAT')
            dtyp.append(pair)
        elif py_dtyp_dict[cols] == str:
            pair = []
            pair.append(cols)
            pair.append('VARCHAR')
            dtyp.append(pair)
        else:
            pair = []
            pair.append(cols)
            pair.append('VARCHAR2(80 CHAR)')
            dtyp.append(pair)
    df_col_len = pd.DataFrame(columns=['COLUMN_NAME','CHAR_LENGTH'])
    df_col_len['COLUMN_NAME'] = df_col
    for idx in range(df_col_len.shape[0]):
        df_col_len.loc[idx,'CHAR_LENGTH'] = df[df_col_len.loc[idx,'COLUMN_NAME']].astype(str).map(len).max()
    df_col_len.set_index('COLUMN_NAME', inplace=True)
    dtyp = dict(dtyp)
    data = {p:[q] for p,q in dtyp.items()}
    df_dict = pd.DataFrame(data).transpose()
    df_dict.rename(columns={0:'DTYP'}, inplace=True)
    df_dtyp = pd.DataFrame(columns=['COLUMN_NAME','DTYP'])
    df_dtyp['COLUMN_NAME'] = df_col
    for idx in range(df_dtyp.shape[0]):
        col_dtyp = df_dict.loc[df_dtyp.loc[idx,'COLUMN_NAME']].values[0]
        if 'TIMESTAMP' in col_dtyp:
            df_dtyp.loc[idx,'DTYP'] = TIMESTAMP(6)
        elif 'DATE' in col_dtyp:
            df_dtyp.loc[idx,'DTYP'] = DATE
        elif 'NUMBER' in col_dtyp or 'INTEGER' in col_dtyp or 'FLOAT' in col_dtyp:
            df_dtyp.loc[idx,'DTYP'] = NUMBER
        else:
            varchar_len = df_col_len.loc[df_dtyp.loc[idx,'COLUMN_NAME']].values[0]
            df_dtyp.loc[idx,'DTYP'] = VARCHAR2(varchar_len)
    return dict(df_dtyp.values)
    
def get_ora_dtyp_dict(df,engine,source_owner='TEST_OWNER',source_table='TEST_TABLE'):
    df_col = df.columns
    df_col_typ = df.dtypes
    df_col_list = [df_col[i] for i in range(df.shape[1])]
    df_col_str = str(df_col_list)
    df_col_str = df_col_str.replace("[","")
    df_col_str = df_col_str.replace("]","")
    df_col_str = df_col_str.replace(", ",", \n")
    dtyp = []
    df_col_oracle_typ = get_oracle_data_type(df_col_str,engine,source_owner=source_owner,source_table=source_table)
    for i in range(len(df_col)):
        if 'date' in str(df_col_typ[i]).lower() or 'dtime' in str(df_col[i]).lower():
            pair = []
            pair.append(df_col[i])
            pair.append('DATE')
            dtyp.append(pair)
        elif df_col[i] in df_col_oracle_typ['COLUMN_NAME'].values:
            get_index = df_col_oracle_typ.index[df_col_oracle_typ['COLUMN_NAME']==df_col[i]]
            oracle_dtyp = str(df_col_oracle_typ['DATA_TYPE'][get_index].values)
            oracle_dtyp = oracle_dtyp.replace("[","")
            oracle_dtyp = oracle_dtyp.replace("]","")
            pair = []
            pair.append(df_col[i])
            pair.append(oracle_dtyp)
            dtyp.append(pair)
        elif 'int' in str(df_col_typ[i]) or 'float' in str(df_col_typ[i]) or 'num' in str(df_col_typ[i]):
            pair = []
            pair.append(df_col[i])
            pair.append('NUMBER')
            dtyp.append(pair)
        else:
            pair = []
            pair.append(df_col[i])
            pair.append('VARCHAR2(80 CHAR)')
            dtyp.append(pair)
    dtyp = dict(dtyp)
    return dtyp

def get_ora_dtyp_varchar_dict(df,engine,source_owner='TEST_OWNER',source_table='TEST_TABLE'):
    df_col = df.columns
    df_col_typ = df.dtypes
    df_col_list = [df_col[i] for i in range(df.shape[1])]
    df_col_str = str(df_col_list)
    df_col_str = df_col_str.replace("[","")
    df_col_str = df_col_str.replace("]","")
    df_col_str = df_col_str.replace(", ",", \n")
    dtyp = []
    df_col_oracle_typ = get_oracle_data_type(df_col_str,engine,source_owner=source_owner,source_table=source_table)
    for i in range(len(df_col)):
        if 'date' in str(df_col_typ[i]).lower() or 'dtime' in str(df_col[i]).lower():
            pair = []
            pair.append(df_col[i])
            pair.append('DATE')
            dtyp.append(pair)
        elif df_col[i] in df_col_oracle_typ['COLUMN_NAME'].values:
            get_index = df_col_oracle_typ.index[df_col_oracle_typ['COLUMN_NAME']==df_col[i]]
            oracle_dtyp = str(df_col_oracle_typ['DATA_TYPE'][get_index].values)
            oracle_dtyp = oracle_dtyp.replace("[","")
            oracle_dtyp = oracle_dtyp.replace("]","")
            pair = []
            pair.append(df_col[i])
            pair.append(oracle_dtyp)
            dtyp.append(pair)
        elif 'int' in str(df_col_typ[i]) or 'float' in str(df_col_typ[i]) or 'num' in str(df_col_typ[i]):
            pair = []
            pair.append(df_col[i])
            pair.append('NUMBER')
            dtyp.append(pair)
        else:
            pair = []
            pair.append(df_col[i])
            pair.append('VARCHAR2(80 CHAR)')
            dtyp.append(pair)
    df_col_len = pd.DataFrame(columns=['COLUMN_NAME','CHAR_LENGTH'])
    df_col_len['COLUMN_NAME'] = df_col
    for i in range(df_col_len.shape[0]):
        df_col_len['CHAR_LENGTH'][i] = df[df_col_len['COLUMN_NAME'][i]].astype(str).map(len).max()
    df_col_len.set_index('COLUMN_NAME', inplace=True)
    for i in range(len(dtyp)):
        if 'VARCHAR2(' in dtyp[i][1]:
            dtyp_str = str(dtyp[i][0]).replace("/","")
            dtyp_str = dtyp_str.replace("'","")
            dtyp_str = str(df_col_len.loc[dtyp_str].values)
            dtyp_str = dtyp_str.replace("[","")
            dtyp_str = dtyp_str.replace("]","")
            dtyp[i][1] = str('VARCHAR(' + dtyp_str + ' CHAR)')
    dtyp = dict(dtyp)
    return dtyp

def is_date(string):
    from dateutil.parser import parse
    try:
        parse(string)
        return True
    except ValueError:
        return False
    except Exception as err:
        print('Error: ' + str(err))

def is_date_valid(string):
    import datetime
    from datetime import time
    from datetime import datetime

    def is_date_valid_1(string):
        try:
            string = datetime.strptime(string,'%m/%d/%Y')
            return True,string
        except ValueError:
            return False,string
        except TypeError:
            return False,string
        except Exception as err:
            print('Error: ' + str(err))

    def is_date_valid_2(string):
        try:
            string = datetime.strptime(string,'%Y-%m-%d')
            return True,string
        except ValueError:
            return False,string
        except TypeError:
            return False,string
        except Exception as err:
            print('Error: ' + str(err))

    def is_date_valid_3(string):
        try:
            string = datetime.strptime(string,'%Y-%m-%d %H:%M:%S')
            return True,string
        except ValueError:
            return False,string
        except TypeError:
            return False,string
        except Exception as err:
            print('Error: ' + str(err))

    def is_date_valid_final(string):
        try:
            if is_date_valid_1(string)[0] == True:
                return is_date_valid_1(string)[0],is_date_valid_1(string)[1]
            elif is_date_valid_2(string)[0] == True:
                return is_date_valid_2(string)[0],is_date_valid_2(string)[1]
            elif is_date_valid_3(string)[0] == True:
                return is_date_valid_3(string)[0],is_date_valid_3(string)[1]
            else:
                datetime.strptime(string,'%Y-%m-%d %H:%M:%S')
        except ValueError:
            return False,string
        except TypeError:
            return False,string
        except Exception as err:
            print('Error: ' + str(err))
    return is_date_valid_final(string)[0],is_date_valid_final(string)[1]
        
def is_timestamp(date):
    from datetime import time
    try:
        date.time()
    except ValueError:
        return False # NaTType does not support time
    except AttributeError:
        return False # if object has no time attribute (e.g. str,float,int,datetime.datetime)
    else:
        if date.time() == time(0,0,0):
            return False # pandas.timestamps.Timestamp but with 00:00:00 time, hence date
        else:
            return True
    
def is_datetime(date):
    import datetime
    from datetime import time
    try:
        date.date()
    except ValueError:
        return False # NaTType does not support time
    except AttributeError:
        return False # if object has no time attribute (e.g. str,float,int,datetime.datetime)
    else:
        if date.time() == time(0,0,0):
            return True
        else:
            return False

def check_dtime(series):
    from datetime import time
    notnull = series.apply(lambda x: np.nan if is_nan(x) == True else x)
    notnull_to_dt = pd.to_datetime(notnull,format='%Y-%m-%d %H:%M:%S',errors='coerce')
    notnull_to_dt = notnull_to_dt.loc[notnull_to_dt.notnull()]
    is_date = notnull_to_dt.apply(lambda x: True if x.time() == time(0,0,0) else False).all()
    is_timestamp = notnull_to_dt.apply(lambda x: True if x.time() != time(0,0,0) else False).any()
    
    if is_date == True and len(notnull_to_dt) != 0:
        is_date = True
        is_timestamp = False
    elif is_date == True and is_timestamp == True:
        is_date = True
        is_timestamp = True
    elif 'date' in series.name.lower():
        is_date = True
        is_timestamp = False
    elif 'dtime' in series.name.lower():
        is_date = True
        is_timestamp = True
    elif 'time' in series.name.lower() and 'dtime' not in series.name.lower():
        is_date = False
        is_timestamp = True
    else:
        is_date = False
        is_timestamp = False
    
    _dict_ = {
        'is_date': is_date,
        'is_timestamp': is_timestamp
    }
    return _dict_
        
def is_nan(val):
    if str(val) == 'nan':
        return True
    elif str(val) == 'NaN':
        return True
    elif str(val) == 'None':
        return True
    elif str(val) == '':
        return True
    else:
        return False

def excel_df_fix_strftime(df):
    from datetime import date
    df_sample = df.loc[:10000,:]
    df = df_sample[df_sample.notnull()]
    for cols in df.columns:
        if ((check_dtime(df[cols])['is_date'] == True or check_dtime(df[cols])['is_timestamp'] == True) and
            (df[df[cols].notnull()][cols].apply(lambda x: isinstance(x,int)).all() == False)
           ):
            if check_dtime(df[cols])['is_date'] == True and check_dtime(df[cols])['is_timestamp'] == False:
                dt = pd.to_datetime(df[cols],format="%Y-%m-%d %H:%M:%S",errors='coerce')
                dt = dt.apply(lambda x: x.date())
                df[cols] = dt
            elif check_dtime(df[cols])['is_date'] == True and check_dtime(df[cols])['is_timestamp'] == True:
                dt = pd.to_datetime(df[cols],format="%Y-%m-%d %H:%M:%S",errors='coerce')
                df[cols] = dt
    return df

def excel_to_sql(fname,schema,table_name,sheet_name,dtyp=None):
    import time
    start_time = time.time()
    time_elapsed = start_time
    
    # Set credential
    my_credentials = get_credentials('my_credentials.txt',schema)
    
    fname_check_result = fname_check(fname,schema,table_name)
    is_file = fname_check_result['is_file']
    fname = fname_check_result['fname']
    
    if table_name == '' or table_name == None:
        table_name = fname_check_result[3]
    else:
        table_name = table_name.upper()
                    
    # Open
    engine = connect_sqlalchemy(my_credentials)
    
    if is_file == True:
        # Read file as dataframe
        if fname.endswith('.xlsx') or fname.endswith('.xlsm') or fname.endswith('.xls'):
            print('Reading file as dataframe.')
            df = pd.read_excel(fname, sheet_name=sheet_name, index_col=False, engine='calamine')
            time_elapsed = get_time_elapsed(time_elapsed)
            if sheet_name == None:
                for sheetnames in df.keys():
                    try:
                        df[sheetnames].columns = df[sheetnames].columns.str.upper()
                        if len(df.keys()) > 1:
                            table_name = table_name+'_'+sheetnames
                        print('Pre-processing data types.')
                        if dtyp == None:
                            dtyp = get_sqlalchemy_dtyp_dict(df[sheetnames],engine)
                        else:
                            dtyp = dtyp
                        df[sheetnames] = df_fix_dtype(df[sheetnames],schema,dtyp=dtyp)
                        time_elapsed = get_time_elapsed(time_elapsed)
                        print('Fixing string datetime.')
                        df[sheetnames] = excel_df_fix_strftime(df[sheetnames])
                        time_elapsed = get_time_elapsed(time_elapsed)
                    except Exception as err:
                        print('Error: ' + str(err))
                        engine.dispose()
                        time_elapsed = get_time_elapsed(time_elapsed)
                    else:
                        print('Importing to database.')
                        try:
                            # Convert dataframe to sql
                            event.listens_for(engine, 'before_cursor_execute')
                            sql = df[sheetnames].to_sql(name=table_name, con=engine, schema=schema, dtype=dtyp, index=False, chunksize=10000)
                            end_time = time.time()
                            print('Execution time: {:2f}'.format(end_time - start_time),'s')
                            # Close
                            engine.dispose()
                        except ValueError:
                            engine.dispose()
                            display(dtyp)
                            time_elapsed = get_time_elapsed(time_elapsed)
                            raise
                        except Exception as err:
                            print('Error: ' + str(err))
                            engine.dispose()
                            time_elapsed = get_time_elapsed(time_elapsed)
            else:
                try:
                    df.columns = df.columns.str.upper()
                    print('Pre-processing data types.')
                    try:
                        if dtyp == None:
                            dtyp = get_sqlalchemy_dtyp_dict(df,engine)
                        else:
                            dtyp = dtyp
                        df = df_fix_dtype(df,schema,dtyp=dtyp)
                        time_elapsed = get_time_elapsed(time_elapsed)
                        print('Fixing string datetime.')
                        df = excel_df_fix_strftime(df)
                        time_elapsed = get_time_elapsed(time_elapsed)
                    except:
                        engine.dispose()
                except Exception as err:
                    print('Error: ' + str(err))
                    engine.dispose()
                    time_elapsed = get_time_elapsed(time_elapsed)
                else:
                    print('Importing to database.')
                    try:
                        # Convert dataframe to sql
                        for idx, (key,value) in enumerate(dtyp.items()):
                            if 'VARCHAR' in str(value) and key == df.columns[idx]:
                                df[key] = df[key].apply(lambda x: '' if is_nan(x) == True else str(x))
                        event.listens_for(engine, 'before_cursor_execute')
                        '''
                        col_sample is for testing Database Error DPI 1001: out of memory. When using pd.read_sql_query()
                        and importing the generated dataframe back into database, there are no issues. However, when the
                        generated dataframe is exported into a spreadsheet, re-opened via pandas, and imported back to
                        database, there will be memory error due to large number of columns. Workaround will be to limit 
                        chunksize parameter as small as possible or upgrade to higher RAM.
                        fix it.
                        '''
#                         df_copy = df.copy()
#                         col_sample = []
#                         for idx,col in enumerate(df_copy.columns):
#                             if idx < 10:
#                                 col_sample.append(col)
#                         df_copy[col_sample]
#                         df = df_copy[col_sample]
                        sql = df.to_sql(name=table_name, con=engine, schema=schema, dtype=dtyp, index=False, chunksize=10000)
                        end_time = time.time()
                        print('Execution time: {:2f}'.format(end_time - start_time),'s')
                        # Close
                        engine.dispose()
                    except ValueError as err:
                        engine.dispose()
                        display(dtyp)
                        time_elapsed = get_time_elapsed(time_elapsed)
                    except Exception as err:
                        print('Error: ' + str(err))
                        engine.dispose()
                        time_elapsed = get_time_elapsed(time_elapsed)
        elif fname.endswith('.csv'):
            try:
                print('Reading file as dataframe.')
                df = pd.read_csv(fname, index_col=False)
                time_elapsed = get_time_elapsed(time_elapsed)
                df.columns = df.columns.str.upper()
                print('Pre-processing data types.')
                if dtyp == None:
                    dtyp = get_sqlalchemy_dtyp_dict(df,engine)
                else:
                    dtyp = dtyp
                df = df_fix_dtype(df,schema,dtyp=dtyp)
                time_elapsed = get_time_elapsed(time_elapsed)
                print('Fixing string datetime.')
                df = excel_df_fix_strftime(df)
                time_elapsed = get_time_elapsed(time_elapsed)
            except Exception as err:
                print('Error: ' + str(err))
                engine.dispose()
                time_elapsed = get_time_elapsed(time_elapsed)
            else:
                print('Importing to database.')
                try:
                    # Convert dataframe to sql
                    event.listens_for(engine, 'before_cursor_execute')
                    sql = df.to_sql(name=table_name, con=engine, schema=schema, dtype=dtyp, index=False, chunksize=10000)
                    end_time = time.time()
                    print('Execution time: {:2f}'.format(end_time - start_time),'s')
                    # Close
                    engine.dispose()
                except ValueError:
                    engine.dispose()
                    display(dtyp)
                    time_elapsed = get_time_elapsed(time_elapsed)
                    raise
                except Exception as err:
                    print('Error: ' + str(err))
                    engine.dispose()
                    time_elapsed = get_time_elapsed(time_elapsed)
    else:
        print('Reading file as dataframe.')
        df = pd.read_excel(fname, sheet_name=sheet_name, index_col=False, engine='calamine')
        if sheet_name == None:
            for sheetnames in df.keys():
                try:
                    df[sheetnames].columns = df[sheetnames].columns.str.upper()
                    if len(df.keys()) > 1:
                        table_name = table_name+'_'+sheetnames
                    print('Pre-processing data types.')
                    if dtyp == None:
                        dtyp = get_sqlalchemy_dtyp_dict(df[sheetnames],engine)
                    else:
                        dtyp = dtyp
                    df[sheetnames] = df_fix_dtype(df[sheetnames],schema,dtyp=dtyp)
                    time_elapsed = get_time_elapsed(time_elapsed)
                    print('Fixing string datetime.')
                    df[sheetnames] = excel_df_fix_strftime(df[sheetnames])
                    time_elapsed = get_time_elapsed(time_elapsed)
                except Exception as err:
                    print('Error: ' + str(err))
                    engine.dispose()
                    time_elapsed = get_time_elapsed(time_elapsed)
                else:
                    print('Importing to database.')
                    try:
                        # Convert dataframe to sql
                        event.listens_for(engine, 'before_cursor_execute')
                        sql = df[sheetnames].to_sql(name=table_name, con=engine, schema=schema, dtype=dtyp, index=False, chunksize=10000)
                        end_time = time.time()
                        print('Execution time: {:2f}'.format(end_time - start_time),'s')
                        # Close
                        engine.dispose()
                    except ValueError:
                        engine.dispose()
                        display(dtyp)
                        time_elapsed = get_time_elapsed(time_elapsed)
                        raise
                    except Exception as err:
                        print('Error: ' + str(err))
                        engine.dispose()
                        time_elapsed = get_time_elapsed(time_elapsed)
        else:
            try:
                df.columns = df.columns.str.upper()
                print('Pre-processing data types.')
                if dtyp == None:
                    dtyp = get_sqlalchemy_dtyp_dict(df,engine)
                else:
                    dtyp = dtyp
                df = df_fix_dtype(df,schema,dtyp=dtyp)
                time_elapsed = get_time_elapsed(time_elapsed)
                print('Fixing string datetime.')
                df = excel_df_fix_strftime(df)
                time_elapsed = get_time_elapsed(time_elapsed)
            except Exception as err:
                print('Error: ' + str(err))
                engine.dispose()
                time_elapsed = get_time_elapsed(time_elapsed)
            else:
                print('Importing to database.')
                try:
                    # Convert dataframe to sql
                    event.listens_for(engine, 'before_cursor_execute')
                    sql = df.to_sql(name=table_name, con=engine, schema=schema, dtype=dtyp, index=False, chunksize=10000)
                    end_time = time.time()
                    print('Execution time: {:2f}'.format(end_time - start_time),'s')
                    # Close
                    engine.dispose()
                except ValueError:
                    engine.dispose()
                    display(dtyp)
                    time_elapsed = get_time_elapsed(time_elapsed)
                    raise
                except Exception as err:
                    print('Error: ' + str(err))
                    engine.dispose()
                    time_elapsed = get_time_elapsed(time_elapsed)

def df_to_sql(df,schema,table_name,dtyp=None,is_dtyp_fixed='NO'):
    import time
    start_time = time.time()
    time_elapsed = start_time
    
    # Set credential
    my_credentials = get_credentials('my_credentials.txt',schema)
    
    # Open
    engine = connect_sqlalchemy(my_credentials)
    
    try:
        df.columns = df.columns.str.upper()
        print('Pre-processing data types.')
        try:
            if dtyp == None:
                dtyp = get_sqlalchemy_dtyp_dict(df,engine)
                time_elapsed = get_time_elapsed(time_elapsed)
            else:
                dtyp = dtyp
            if is_dtyp_fixed.upper() == 'NO' or is_dtyp_fixed.upper() == 'N':
                df = df_fix_dtype(df,schema,dtyp=dtyp)
                time_elapsed = get_time_elapsed(time_elapsed)
                print('Fixing string datetime.')
                df = excel_df_fix_strftime(df)
                time_elapsed = get_time_elapsed(time_elapsed)
        except:
            engine.dispose()
    except Exception as err:
        print('Error: ' + str(err))
        engine.dispose()
        time_elapsed = get_time_elapsed(time_elapsed)
    else:
        print('Importing to database.')
        try:
            # Convert dataframe to sql
            for idx, (key,value) in enumerate(dtyp.items()):
                if 'VARCHAR' in str(value) and key == df.columns[idx]:
#                     df[key] = df[key].apply(lambda x: '' if is_nan(x) == True else str(x))
                    df[key] = df[key].replace('nan','')
            event.listens_for(engine, 'before_cursor_execute')
            '''
            col_sample is for testing Database Error DPI 1001: out of memory. When using pd.read_sql_query()
            and importing the generated dataframe back into database, there are no issues. However, when the
            generated dataframe is exported into a spreadsheet, re-opened via pandas, and imported back to
            database, there will be memory error due to large number of columns. Workaround will be to limit 
            chunksize parameter as small as possible or upgrade to higher RAM.
            fix it.
            '''
#             df_copy = df.copy()
#                         col_sample = []
#                         for idx,col in enumerate(df_copy.columns):
#                             if idx < 10:
#                                 col_sample.append(col)
#                         df_copy[col_sample]
#                         df = df_copy[col_sample]
            sql = df.to_sql(name=table_name, con=engine, schema=schema, dtype=dtyp, index=False, chunksize=10000)
            end_time = time.time()
            print('Execution time: {:2f}'.format(end_time - start_time),'s')
            # Close
            engine.dispose()
        except ValueError as err:
            engine.dispose()
            display(dtyp)
            time_elapsed = get_time_elapsed(time_elapsed)
        except Exception as err:
            print('Error: ' + str(err))
            engine.dispose()
            time_elapsed = get_time_elapsed(time_elapsed)
                    
def query_check(schema,table_name):
    query = f'''
                select count(*) num_cases
                from all_tab_columns
                where 1=1
                      and upper(owner) = '{str(schema.upper())}'
                      and upper(table_name) = '{str(table_name.upper())}'
              '''
    return query

def create_table_sql(query,fname,schema,table_name,sheet_name):
    start_time = time.time()
    # Set credential
    my_credentials = get_credentials('my_credentials.txt',schema)
    
    special_characters = "!@#$%^&*()[]{};:,./<>?\|`~-=+"
    
    
    fname_check_result = fname_check(fname,schema,table_name)
    is_file = fname_check_result['is_file']
    folder_path = fname_check_result['folder_path']
    fullpath = fname_check_result['fullpath']
    fname = fname_check_result['fname']
    
    if is_nan(table_name) == True:
        table_name = fname_check_result['table_name']
        table_name = clean_char(table_name,"_") # replace special characters with "_" and remove ending "_" character.
    else:
        table_name = table_name.upper()
        table_name = clean_char(table_name,"_") # replace special characters with "_" and remove ending "_" character.
                    
        
    print(f'''Query used: \n
-----------------------------------------------------------------
{query}
-----------------------------------------------------------------
''')
    if is_file == True:
        try:
            print('File already exists.')
            print('Fullpath:',fullpath)
        except Exception as err:
            print('Error: ' + str(err))
        else:
            # Open
            engine = connect_sqlalchemy(my_credentials)
            conn = engine.raw_connection()
            curr = conn.cursor()
            Session = sessionmaker(bind=engine)
            session = Session()
            
            try:
                df = pd.read_sql_query(query,engine)
            except Exception as err:
                print('Error: ' + str(err))
                curr.close
                engine.dispose()
            else:
                df.columns = df.columns.str.upper()
            
                dtyp = get_sqlalchemy_dtyp_dict(df,engine)

                # Check if table exists in database
                print('Checking in the database.')

                # Open file
                df_open = pd.read_excel(fname, sheet_name=None, engine='calamine')
                sheet_names = df_open.keys()
                for sheetnames in sheet_names:
                    table_name_base = table_name
                    if len(sheet_names) == 1:
                        curr.execute(query_check(schema,table_name_base))
                        if curr.fetchone()[0] >= 1:
                            is_new_table = input('Table already exists in the database. Do you want to create a new table? [YES/NO]  |  ')
                            if is_new_table.upper() == 'YES' or is_new_table.upper() == 'Y':
                                new_table_name = input('Insert new table name.  |  ')
                                new_table_name.upper()
                                new_table_name = clean_char(new_table_name,"_") # replace special characters with "_" and remove ending

                                new_fname = folder_path+'/'+new_table_name+''.join(pathlib.Path(fname).suffixes)
                                try:
                                    curr.execute(query_check(schema,new_table_name))
                                except Exception as err:
                                    print('Error: ' + str(err))
                                    curr.close()
                                    engine.dispose()
                                    break
                                else:
                                    if curr.fetchone()[0] >= 1:
                                        print('New table name already exists in the database.')
                                        if_exists_proceed = input('Do you want to replace the existing table with new values? [YES/NO]  |  ')
                                        if if_exists_proceed.upper() == 'YES' or if_exists_proceed.upper() == 'Y':
                                            # Reflect table and bind metadata to engine
                                            metadata = MetaData(conn)
                                            metadata.bind = engine
                                            reflected_table = Table(new_table_name, metadata, autoload_with=engine)
                                            # Drop the reflected table
                                            print('Dropping existing table.')
                                            reflected_table.drop(checkfirst=True)
                                            session.commit()
                                            # Convert dataframe to sql
                                            print('Importing to database.')
                                            event.listens_for(engine, 'before_cursor_execute')
                                            try:
                                                sql = df.to_sql(name=new_table_name, con=engine, schema=schema, dtype=dtyp, index=False)
                                            except Exception as err:
                                                print('Error: ' + str(err))
                                                curr.close()
                                                engine.dispose()
                                            else:
                                                print('Table replaced.')
                                        else:
                                            print('No action done.')
                                    else:
                                        print('New fullpath:',new_fname.replace('/','\\'))
                                        # Convert dataframe to excel and save
                                        if fname.endswith('.xlsx') or fname.endswith('.xlsm') or fname.endswith('.xls') or fname.endswith('.csv'):
                                            df_copy = df.copy()
                                            df_to_excel(df_copy,schema,new_fname,sheetnames,dtyp=dtyp)
                                            del df_copy
                                        curr.close()
                                        # Convert dataframe to sql
                                        print('Importing to database.')
                                        event.listens_for(engine, 'before_cursor_execute')
                                        try:
                                            sql = df.to_sql(name=new_table_name, con=engine, schema=schema, dtype=dtyp, index=False)
                                        except Exception as err:
                                            print('Error: ' + str(err))
                                            curr.close()
                                            engine.dispose()
                                        else:
                                            print(f"Table [{new_table_name}] created.")
                            elif is_new_table.upper() == 'NO' or is_new_table.upper() == 'N':
                                if_exists_proceed = input('Do you want to replace the existing table? [YES/NO]')
                                if if_exists_proceed.upper() == 'YES' or if_exists_proceed.upper() == 'Y':
                                    # Reflect table and bind metadata to engine
                                    metadata = MetaData(conn)
                                    metadata.bind = engine
                                    reflected_table = Table(table_name, metadata, autoload_with=engine)
                                    # Drop the reflected table
                                    print('Dropping existing table.')
                                    reflected_table.drop(checkfirst=True)
                                    session.commit()
                                    # Convert dataframe to sql
                                    print('Importing to database.')
                                    event.listens_for(engine, 'before_cursor_execute')
                                    try:
                                        sql = df.to_sql(name=table_name, con=engine, schema=schema, dtype=dtyp, index=False)
                                    except Exception as err:
                                        print('Error: ' + str(err))
                                        curr.close()
                                        engine.dispose()
                                    else:
                                        print('Table replaced.')
                                else:
                                    print('No action done.')
                            else:
                                print('Invalid input. Ending prompt.')
                        else:
                            print('No existing table. Importing to database.')
                            table_name.upper()
                            table_name = clean_char(table_name,"_") # replace special characters with "_" and remove ending
                            # Convert dataframe to sql
                            event.listens_for(engine, 'before_cursor_execute')
                            try:
                                sql = df.to_sql(name=table_name, con=engine, schema=schema, dtype=dtyp, index=False)
                            except Exception as err:
                                print('Error: ' + str(err))
                                curr.close()
                                engine.dispose()
                            else:
                                print(f"Table [{table_name}] created.")
                    else:
                        table_name_sheet = table_name_base + '_' +  sheetnames
                        table_name_sheet.upper()
                        table_name_sheet = clean_char(table_name_sheet,"_") # replace special characters with "_" and remove ending
                        curr.execute(query_check(schema,table_name_sheet))
                        if curr.fetchone()[0] >= 1:
                            query_column = f'''
                                                select
                                                       column_name
                                                      ,count(*) num_cases
                                                from all_tab_columns
                                                where 1=1
                                                      and upper(owner) = '{str(schema.upper())}'
                                                      and upper(table_name) = '{str(table_name.upper())}'
                                                group by
                                                       column_name
                                            '''
                            df_query = pd.read_sql_query(query_column,engine)
                            df_query = df_query.drop('num_cases', axis=1)

                            df_col = df.columns.str.upper()
                            df_exist = pd.DataFrame({'column_name': df_col})

                            # Check if any value in df1['col1'] does not exist in df2['col1']
                            missing_values = df_exist[~df_exist['column_name'].isin(df_query['column_name'])]

                            if not missing_values.empty:
                                is_new_table = input('Table already exists in the database. Do you want to create a new table? [YES/NO]  |  ')
                                if is_new_table.upper() == 'YES' or is_new_table.upper() == 'Y':
                                    new_table_name = input('Insert new table name.  |  ')
                                    new_table_name.upper()
                                    new_table_name = clean_char(new_table_name,"_") # replace special characters with "_" and remove ending

                                    new_fname = folder_path+'/'+new_table_name+''.join(pathlib.Path(fname).suffixes)
                                    try:
                                        curr.execute(query_check(schema,new_table_name))
                                    except Exception as err:
                                        print('Error: ' + str(err))
                                        curr.close()
                                        engine.dispose()
                                    else:
                                        if curr.fetchone()[0] >= 1:
                                            print('New table name already exists in the database.')
                                            if_exists_proceed = input('Do you want to replace the existing table with new values? [YES/NO]  |  ')
                                            if if_exists_proceed.upper() == 'YES' or if_exists_proceed.upper() == 'Y':
                                                # Reflect table and bind metadata to engine
                                                metadata = MetaData(conn)
                                                metadata.bind = engine
                                                reflected_table = Table(new_table_name, metadata, autoload_with=engine)
                                                # Drop the reflected table
                                                print('Dropping existing table.')
                                                reflected_table.drop(checkfirst=True)
                                                session.commit()
                                                # Convert dataframe to sql
                                                print('Importing to database.')
                                                event.listens_for(engine, 'before_cursor_execute')
                                                try:
                                                    sql = df.to_sql(name=new_table_name, con=engine, schema=schema, dtype=dtyp, index=False)
                                                except Exception as err:
                                                    print('Error: ' + str(err))
                                                    curr.close()
                                                    engine.dispose()
                                                else:
                                                    print('Table replaced.')
                                            else:
                                                print('No action done.')
                                        else:
                                            print('New fullpath:',new_fname.replace('/','\\'))
                                            # Convert dataframe to excel and save
                                            if fname.endswith('.xlsx') or fname.endswith('.xlsm') or fname.endswith('.xls') or fname.endswith('.csv'):
                                                df_copy = df.copy()
                                                df_to_excel(df_copy,schema,new_fname,table_name_sheet,dtyp=dtyp)
                                                del df_copy
                                            curr.close()
                                            # Convert dataframe to sql
                                            print('Importing to database.')
                                            event.listens_for(engine, 'before_cursor_execute')
                                            try:
                                                sql = df.to_sql(name=new_table_name, con=engine, schema=schema, dtype=dtyp, index=False)
                                            except Exception as err:
                                                print('Error: ' + str(err))
                                                curr.close()
                                                engine.dispose()
                                            else:
                                                print(f"Table [{new_table_name}] created.")
                                elif is_new_table.upper() == 'NO' or is_new_table.upper() == 'Y':
                                    if_exists_proceed = input('Do you want to replace the existing table? [YES/NO]  |  ')
                                    if if_exists_proceed.upper() == 'YES' or if_exists_proceed.upper() == 'Y':
                                        # Reflect table and bind metadata to engine
                                        metadata = MetaData(conn)
                                        metadata.bind = engine
                                        reflected_table = Table(table_name_sheet, metadata, autoload_with=engine)
                                        # Drop the reflected table
                                        print('Dropping existing table.')
                                        reflected_table.drop(checkfirst=True)
                                        session.commit()
                                        # Convert dataframe to sql
                                        print('Importing to database.')
                                        event.listens_for(engine, 'before_cursor_execute')
                                        try:
                                            sql = df.to_sql(name=table_name_sheet, con=engine, schema=schema, dtype=dtyp, index=False)
                                        except Exception as err:
                                            print('Error: ' + str(err))
                                            curr.close()
                                            engine.dispose()
                                        else:
                                            print('Table replaced.')
                                    else:
                                        print('No action done.')
                                else:
                                    print('Invalid input. Ending prompt.')

                            else:
                                is_new_table = input('Identical table with same table_name and column_names is found in the database. Do you want to create a new table? [YES/NO]  |  ')
                                if is_new_table.upper() == 'YES' or is_new_table.upper() == 'Y':
                                    new_table_name = input('Insert new table name.  |  ')
                                    new_table_name.upper()
                                    new_table_name = clean_char(new_table_name,"_") # replace special characters with "_" and remove ending "_" character.

                                    new_fname = folder_path+'/'+new_table_name+''.join(pathlib.Path(fname).suffixes)
                                    try:
                                        curr.execute(query_check(schema,new_table_name))
                                    except Exception as err:
                                        print('Error: ' + str(err))
                                        curr.close()
                                        engine.dispose()
                                    else:
                                        if curr.fetchone()[0] >= 1:
                                            print('New table name already exists in the database.')
                                            if_exists_proceed = input('Do you want to replace the existing table with new values? [YES/NO]  |  ')
                                            if if_exists_proceed.upper() == 'YES' or if_exists_proceed.upper() == 'Y':
                                                # Reflect table and bind metadata to engine
                                                metadata = MetaData(conn)
                                                metadata.bind = engine
                                                reflected_table = Table(new_table_name, metadata, autoload_with=engine)
                                                # Drop the reflected table
                                                print('Dropping existing table.')
                                                reflected_table.drop(checkfirst=True)
                                                session.commit()
                                                # Convert dataframe to sql
                                                print('Importing to database.')
                                                event.listens_for(engine, 'before_cursor_execute')
                                                try:
                                                    sql = df.to_sql(name=new_table_name, con=engine, schema=schema, dtype=dtyp, index=False)
                                                except Exception as err:
                                                    print('Error: ' + str(err))
                                                    curr.close()
                                                    engine.dispose()
                                                else:
                                                    print('Table replaced.')
                                            else:
                                                print('No action done.')
                                        else:
                                            print('New fullpath:',new_fname.replace('/','\\'))
                                            # Convert dataframe to excel and save
                                            if fname.endswith('.xlsx') or fname.endswith('.xlsm') or fname.endswith('.xls') or fname.endswith('.csv'):
                                                df_copy = df.copy()
                                                df_to_excel(df_copy,schema,new_fname,table_name_sheet,dtyp=dtyp)
                                                del df_copy
                                            curr.close()                                            
                                            # Convert dataframe to sql
                                            print('Importing to database.')
                                            event.listens_for(engine, 'before_cursor_execute')
                                            try:
                                                sql = df.to_sql(name=new_table_name, con=engine, schema=schema, dtype=dtyp, index=False)
                                            except Exception as err:
                                                print('Error: ' + str(err))
                                                curr.close()
                                                engine.dispose()
                                            else:
                                                print(f"Table [{new_table_name}] created.")
                                elif is_new_table.upper() == 'NO' or is_new_table.upper() == 'N':
                                    if_exists_proceed = input('Do you want to replace the existing table? [YES/NO]  |  ')
                                    if if_exists_proceed.upper() == 'YES' or if_exists_proceed.upper() == 'Y':
                                        # Reflect table and bind metadata to engine
                                        metadata = MetaData(conn)
                                        metadata.bind = engine
                                        reflected_table = Table(table_name, metadata, autoload_with=engine)
                                        # Drop the reflected table
                                        print('Dropping existing table.')
                                        reflected_table.drop(checkfirst=True)
                                        session.commit()
                                        # Convert dataframe to sql
                                        print('Importing to database.')
                                        event.listens_for(engine, 'before_cursor_execute')
                                        try:
                                            sql = df.to_sql(name=table_name, con=engine, schema=schema, dtype=dtyp, index=False)
                                        except Exception as err:
                                            print('Error: ' + str(err))
                                            curr.close()
                                            engine.dispose()
                                        else:
                                            print('Table replaced.')
                                    else:
                                        print('No action done.')
                                else:
                                    print('Invalid input. Ending prompt.')
                        else:
                            print('No existing table. Importing to database.')
                            try:
                                sql = df[sheetnames].to_sql(name=table_name_sheet, con=engine, schema=schema, dtype=dtyp, index=False)
                            except Exception as err:
                                print('Error: ' + str(err))
                                curr.close()
                                engine.dispose()
                            else:
                                print(f"Table [{table_name}] created.")

                # Close
                engine.dispose()
                print('Finished.')
    else:
        print('No matching file found.')
        # Open
        engine = connect_sqlalchemy(my_credentials)
        conn = engine.raw_connection()
        curr = conn.cursor()
        Session = sessionmaker(bind=engine)
        session = Session()
        if sheet_name == None:
            sheet_name = 'Sheet1'
        try:
            df = pd.read_sql_query(query,engine)
        except Exception as err:
            print('Error: ' + str(err))
            engine.dispose()
        else:
            df.columns = df.columns.str.upper()

            dtyp = get_sqlalchemy_dtyp_dict(df,engine)

            # Check if table exists in database
            print('Checking in the database.')

            try:
                curr.execute(query_check(schema,table_name))
            except Exception as err:
                print('Error: ' + str(err))
                curr.close()
                engine.dispose()
            else:
                if curr.fetchone()[0] >= 1:
                    query_column = f'''
                                        select
                                               column_name
                                              ,count(*) num_cases
                                        from all_tab_columns
                                        where 1=1
                                              and upper(owner) = '{str(schema.upper())}'
                                              and upper(table_name) = '{str(table_name.upper())}'
                                        group by
                                               column_name
                                    '''
                    df_query = pd.read_sql_query(query_column,engine)
                    df_query = df_query.drop('num_cases', axis=1)

                    df_col = df.columns.str.upper()
                    df_exist = pd.DataFrame({'column_name': df_col})

                    # Check if any value in df1['col1'] does not exist in df2['col1']
                    missing_values = df_exist[~df_exist['column_name'].isin(df_query['column_name'])]

                    if not missing_values.empty:
                        is_new_table = input('Table already exists in the database. Do you want to create a new table? [YES/NO]  |  ')
                        if is_new_table.upper() == 'YES' or is_new_table.upper() == 'Y':
                            new_table_name = input('Insert new table name.  |  ')
                            new_table_name.upper()
                            new_table_name = clean_char(new_table_name,"_") # replace special characters with "_" and remove ending "_" character.

                            new_fname = folder_path+'/'+new_table_name+''.join(pathlib.Path(fname).suffixes)
                            try:
                                curr.execute(query_check(schema,new_table_name))
                            except Exception as err:
                                print('Error: ' + str(err))
                                curr.close()
                                engine.dispose()
                            else:
                                if curr.fetchone()[0] >= 1:
                                    print('New table name already exists in the database.')
                                    is_replace = input('Do you want to replace the existing table with new values? [YES/NO]  |  ')
                                    if is_replace.upper() == 'YES' or is_replace.upper() == 'Y':
                                        # Reflect table and bind metadata to engine
                                        metadata = MetaData(conn)
                                        metadata.bind = engine
                                        reflected_table = Table(new_table_name, metadata, autoload_with=engine)
                                        # Drop the reflected table
                                        print('Dropping existing table.')
                                        reflected_table.drop(checkfirst=True)
                                        session.commit()
                                        print('Importing to database.')
                                        event.listens_for(engine, 'before_cursor_execute')
                                        try:
                                            sql = df.to_sql(name=new_table_name, con=engine, schema=schema, dtype=dtyp, index=False)
                                        except Exception as err:
                                            print('Error: ' + str(err))
                                            curr.close()
                                            engine.dispose()
                                        else:
                                            print('Table replaced.')
                                    else:
                                        print('No action done.')
                                else:
                                    print('New fullpath:',new_fname)
                                    # Convert dataframe to excel and save
                                    if fname.endswith('.xlsx') or fname.endswith('.xlsm') or fname.endswith('.xls') or fname.endswith('.csv'):
                                        df_copy = df.copy()
                                        df_to_excel(df_copy,schema,new_fname,sheet_name,dtyp=dtyp)
                                        del df_copy
                                    curr.close()
                                    # Convert dataframe to sql
                                    print('Importing to database.')
                                    event.listens_for(engine, 'before_cursor_execute')
                                    try:
                                        sql = df.to_sql(name=new_table_name, con=engine, schema=schema, dtype=dtyp, index=False)
                                    except Exception as err:
                                        print('Error: ' + str(err))
                                        curr.close()
                                        engine.dispose()
                                    else:
                                        print(f"Table [{new_table_name}] created.")
                        elif is_new_table.upper() == 'NO' or is_new_table.upper() == 'N':
                            if_exists_proceed = input('Do you want to replace the existing table? [YES/NO]')
                            if if_exists_proceed.upper() == 'YES' or if_exists_proceed.upper() == 'Y':
                                # Reflect table and bind metadata to engine
                                metadata = MetaData(conn)
                                metadata.bind = engine
                                reflected_table = Table(table_name, metadata, autoload_with=engine)
                                # Drop the reflected table
                                print('Dropping existing table.')
                                reflected_table.drop(checkfirst=True)
                                session.commit()
                                # Convert dataframe to sql
                                print('Importing to database.')
                                event.listens_for(engine, 'before_cursor_execute')
                                try:
                                    sql = df.to_sql(name=table_name, con=engine, schema=schema, dtype=dtyp, index=False)
                                except Exception as err:
                                    print('Error: ' + str(err))
                                    curr.close()
                                    engine.dispose()
                                else:
                                    print('Table replaced.')
                        else:
                            print('Invalid input. Ending prompt.')

                    else:
                        is_new_table = input('Identical table with same table_name and column_names is found in the database. Do you want to create a new table? [YES/NO]  |  ')
                        if is_new_table.upper() == 'YES' or is_new_table.upper() == 'Y':
                            new_table_name = input('Insert new table name.  |  ')
                            new_table_name.upper()
                            new_table_name = clean_char(new_table_name,"_") # replace special characters with "_" and remove ending "_" character.

                            new_fname = folder_path+'/'+new_table_name+''.join(pathlib.Path(fname).suffixes)
                            try:
                                curr.execute(query_check(schema,new_table_name))
                            except Exception as err:
                                print('Error: ' + str(err))
                                curr.close()
                                engine.dispose()
                            else:
                                if curr.fetchone()[0] >= 1:
                                    print('New table name already exists in the database.')
                                    if_exists_proceed = input('Do you want to replace the existing table with new values? [YES/NO]  |  ')
                                    if if_exists_proceed.upper() == 'YES' or if_exists_proceed.upper() == 'Y':
                                        # Reflect table and bind metadata to engine
                                        metadata = MetaData(conn)
                                        metadata.bind = engine
                                        reflected_table = Table(new_table_name, metadata, autoload_with=engine)
                                        # Drop the reflected table
                                        print('Dropping existing table.')
                                        reflected_table.drop(checkfirst=True)
                                        session.commit()
                                        # Convert dataframe to sql
                                        print('Importing to database.')
                                        event.listens_for(engine, 'before_cursor_execute')
                                        try:
                                            sql = df.to_sql(name=new_table_name, con=engine, schema=schema, dtype=dtyp, index=False)
                                        except Exception as err:
                                            print('Error: ' + str(err))
                                            curr.close()
                                            engine.dispose()
                                        else:
                                            print('Table replaced.')
                                    else:
                                        print('No action done.')
                                else:
                                    print('New fullpath:',new_fname.replace('/','\\'))
                                    # Convert dataframe to excel and save
                                    if fname.endswith('.xlsx') or fname.endswith('.xlsm') or fname.endswith('.xls') or fname.endswith('.csv'):
                                        df_copy = df.copy()
                                        df_to_excel(df_copy,schema,new_fname,sheet_name,dtyp=dtyp)
                                        del df_copy
                                    curr.close()
                                    # Convert dataframe to sql
                                    print('Importing to database.')
                                    event.listens_for(engine, 'before_cursor_execute')
                                    try:
                                        sql = df.to_sql(name=new_table_name, con=engine, schema=schema, dtype=dtyp, index=False)
                                    except Exception as err:
                                        print('Error: ' + str(err))
                                        curr.close()
                                        engine.dispose()
                                    else:
                                        print(f"Table [{new_table_name}] created.")
                        elif is_new_table.upper() == 'NO' or is_new_table.upper() == 'N':
                            if_exists_proceed = input('Do you want to replace the existing table? [YES/NO]  |  ')
                            if if_exists_proceed.upper() == 'YES' or if_exists_proceed.upper() == 'Y':
                                # Reflect table and bind metadata to engine
                                metadata = MetaData(conn)
                                metadata.bind = engine
                                reflected_table = Table(table_name, metadata, autoload_with=engine)
                                # Drop the reflected table
                                print('Dropping existing table.')
                                reflected_table.drop(checkfirst=True)
                                session.commit()
                                # Convert dataframe to sql
                                print('Importing to database.')
                                event.listens_for(engine, 'before_cursor_execute')
                                try:
                                    sql = df.to_sql(name=table_name, con=engine, schema=schema, dtype=dtyp, index=False)
                                except Exception as err:
                                    print('Error: ' + str(err))
                                    curr.close()
                                    engine.dispose()
                                else:
                                    print('Table replaced.')
                            else:
                                print('No action done.')
                        else:
                            print('Invalid input. Ending prompt.')
                else:
                    # Convert dataframe to excel and save
                    if fname.endswith('.xlsx') or fname.endswith('.xlsm') or fname.endswith('.xls') or fname.endswith('.csv'):
                        df_copy = df.copy()
                        df_to_excel(df_copy,schema,fname,sheet_name,dtyp=dtyp)
                        del df_copy
                    # Convert dataframe to sql
                    print('No existing table. Importing to database.')
                    print(fname)
                    event.listens_for(engine, 'before_cursor_execute')
                    try:
                        sql = df.to_sql(name=table_name, con=engine, schema=schema, dtype=dtyp, index=False)
                    except Exception as err:
                        print('Error: ' + str(err))
                        curr.close()
                        engine.dispose()
                    else:
                        print(f"Table [{table_name}] created.")
        
            # Close
            engine.dispose()
            print('Finished.')
        
    end_time = time.time()
    print('Execution time: {:2f}'.format(end_time - start_time),'s')

