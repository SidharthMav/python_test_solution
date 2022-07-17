# imports
import pandas as pd
import pathlib
import glob
import os
import json
import argparse
from datetime import datetime
from datetime import timedelta
import logging
import logging.handlers

def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
    # return vars(parser.parse_args())
    args, unknown = parser.parse_known_args()
    return args

def getListOfFiles(dirName):
    # create a list of file and sub directories names in the given directory 
    listOfFile = os.listdir(dirName)
    allFiles = list()
    # Iterate over all the entries
    for entry in listOfFile:
        # Create full path
        fullPath = os.path.join(dirName, entry)
        # If entry is a directory then get the list of files in this directory 
        if os.path.isdir(fullPath):
            allFiles = allFiles + getListOfFiles(fullPath)
        else:
            allFiles.append(fullPath)
                
    return allFiles

def get_file_paths(path, list_of_files, extension):
    # search 'path' parent directory and child directories
    # append the path of files with specific extension to 'list_of_files'
    
    for file in getListOfFiles(path):
        if os.path.splitext(file)[-1]==extension:
            list_of_files.append(file)
    return list_of_files

def flatten_df(record, column_to_flatten, other_columns ,dfs_list):
    # for each 'record', flatten the 'record' on the basis of a 'column_to_flatten',
    # and retain the columns passed in the list 'other_columns'
    df = pd.DataFrame(record[column_to_flatten])
    for column_name in other_columns:
        df[column_name] = record[column_name]
    
    dfs_list.append(df)

def exit_process_log(reason, t2, t1, log_object):
    # help with logging
    log_object.info("***************end***************. Reason: "+str(reason)+". Time taken: "+str(t2-t1))
    handlers = log_object.handlers[:]
    for handler in handlers:
        handler.close()
        log_object.removeHandler(handler)

def main():
    # logging initialisation
    log_file = 'log.log'
    log_object = logging.getLogger('LogInfo')
    log_object.setLevel(logging.DEBUG)
    handler = logging.handlers.RotatingFileHandler(log_file)
    formatter = logging.Formatter("%(asctime)s====%(levelname)s====%(message)s", "%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)
    log_object.addHandler(handler)    
    t1 = datetime.now()
    log_object.info("***************start***************")
    
    # read the parameters
    params = get_params()
    
    # read customer_master('cm') and product_master('pm')
    try:
        cm = pd.read_csv(params.customers_location)
        pm = pd.read_csv(params.products_location)
    except Exception as e:
        t2 = datetime.now()
        log_object.exception("product/customer master read error")
        exit_process_log(str(e), t2, t1, log_object)
        exit()
    
    # store the paths of transaction json files into a list 'list_of_files'
    try:
        list_of_files = []
        list_of_files = get_file_paths(params.transactions_location,
                                      list_of_files,
                                      '.json')
    except Exception as e:
        t2 = datetime.now()
        log_object.exception("transaction files paths fetch error")
        exit_process_log(str(e), t2, t1, log_object)
        exit()
        
    
    try:
        # store individual transaction dataframe into a list 'transactions_dfs_list'
        transactions_dfs_list = []
        for file_path in list_of_files:
            df = pd.read_json(file_path, lines=True)
            transactions_dfs_list.append(df)

        # concat 'transactions_dfs_list' on top of each other to create a single 'fct' dataframe
        fct = pd.concat(transactions_dfs_list, ignore_index=True)
    except Exception as e:
        t2 = datetime.now()
        log_object.exception("transaction files concat error")
        exit_process_log(str(e), t2, t1, log_object)
        exit()
        
    
    # flatten the 'fct' dataframe and store into 'fct_basket' dataframe
    try:
        basket_dfs_list = []
        column_to_flatten = 'basket'
        other_columns = ['customer_id','date_of_purchase']
        
        # flatten_df() will append individual the basket information into basket_dfs_list
        fct.apply(lambda record: flatten_df(record, 'basket', other_columns, basket_dfs_list), axis=1)
        
        # concat basket_dfs_list on top of each other to create a single fct dataframe
        fct_basket = pd.concat(basket_dfs_list, ignore_index=True)
        print(fct_basket)
    except Exception as e:
        t2 = datetime.now()
        log_object.exception("transaction merge error")
        exit_process_log(str(e), t2, t1, log_object)
        exit()
    
    
    # merge flattened transation data(fct_basket) to product master and customer master
    fct_basket_pcm = fct_basket.merge(pm).merge(cm)
    

    # convert 'date_of_purchase' column to datetime data type
    fct_basket_pcm['date_of_purchase'] = pd.to_datetime(fct_basket_pcm['date_of_purchase'])

    # add new columns 'week' and 'year' on the basis of 'date_of_purchase'
    fct_basket_pcm['week'] = fct_basket_pcm['date_of_purchase'].dt.isocalendar()['week']
    fct_basket_pcm['year'] = fct_basket_pcm['date_of_purchase'].dt.isocalendar()['year']

    # concatenate year column and week column to get 'year_week' which stores the 'year' and 'week' information into a single column
    fct_basket_pcm['year_week'] = fct_basket_pcm['year'].astype(str)+'_'+fct_basket_pcm['week'].astype(str)
    
    unique_level = ['year_week','customer_id','loyalty_score','product_id','product_category']
    to_agg = 'date_of_purchase'
    agg = 'nunique'

    # aggregate according to the requirements of the data science team
    fct_basket_pcm_week = fct_basket_pcm.groupby(unique_level).agg(purchase_count=(to_agg,agg)).reset_index()    
    
    required_columns = ['customer_id', 'loyalty_score', 'product_id', 'product_category', 'purchase_count']
    
    # create output location directory if it does not exist
    if not os.path.exists(params.output_location):
        os.makedirs(params.output_location)    
    
    # export the data in a way that each week's information is in a separate file
    # this can be changed according to the customer requirement to get all data into 1 file with latest week's data processed
    # or export only latest week's data
    fct_basket_pcm_week.groupby(pd.Grouper(key='year_week')).apply(lambda record:record[required_columns].to_json(params.output_location+"W={}.json".format(record['year_week'].unique()),
                                                                                                                  orient='records'))    
    t2 = datetime.now()
    exit_process_log("", t2, t1, log_object)

if __name__ == "__main__":
    main()