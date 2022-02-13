import time
from datetime import timedelta

import html
import re

import os
import ntpath

import subprocess as sp
import argparse

import numpy as np
import pandas as pd
import xml.etree.ElementTree as et

import dask
import dask.dataframe as dd
import dask.bag as bd
from dask.distributed import Client
from dask_jobqueue import SLURMCluster

# python s2_fetch_javaposts.py -cfp ../../../../stackexchange_v2/workspace/input -rfp posts/Posts*.csv
# python s2_fetch_javaposts.py -cfp ../SO_data_dump  -rfp posts/Posts*.csv

parser = argparse.ArgumentParser(
    description='Read Stack Overflow XML Files'
)

parser.add_argument(
    "-cfp",
    "--commonfilepath",
    type=str, 
    help="Enter the SO xml file path , use spaces to saparate multiple files"
)

parser.add_argument(
    "-rfp",
    "--relativefilepath",
    type=str, 
    help="Enter the SO xml file path , use spaces to saparate multiple files"
)


args = parser.parse_args()

common_path = args.commonfilepath
relative_path = args.relativefilepath



def fetch_javaposts():
    
    filepath = '{}/{}'.format(common_path, relative_path)
    
    ## Read csv file
    ddf = dd.read_csv(filepath, engine='python', error_bad_lines=False, warn_bad_lines=False, dtype=object)
        
    ### Get the total number of posts with Tags
    print('Columns {}'.format(ddf.columns))
    
    ## Number of partitions
    print('Partitions {}'.format(ddf.npartitions))
    
    ### Get the total number of posts
    init_posts_len = ddf.index.shape[0].compute()
    ## Get length of each partition and sum them up
    #len_of_ppt_ser=ddf.map_partitions(len).compute()
    #init_posts_len = pd.Series(len_of_ppt_ser, dtype="int64").sum()
    print('Total number of posts {}'.format(init_posts_len))
    
    ### Drop the rows with NaN in the Tag column 
    ddf = ddf.dropna(subset=['Tags'])
    
    ## Get the total number of posts with Tags
    posts_drp_nan_len = ddf.index.shape[0].compute()
    ## Get length of each partition and sum them up
    #len_of_ppt_ser=ddf.map_partitions(len).compute()
    #posts_drp_nan_len = pd.Series(len_of_ppt_ser, dtype="int64").sum()
    print('number of posts with tags {}'.format(posts_drp_nan_len))
    
    ## Get the posts with java tags
    ddf = ddf[ddf['Tags'].str.contains('<java>', case=False, regex=True)]
    
    ## Get the total number of the posts with the java tags
    posts_len_java_tags = ddf.index.shape[0].compute()
    ## Get length of each partition and sum them up
    #len_of_ppt_ser=ddf.map_partitions(len).compute()
    #posts_len_java_tags = pd.Series(len_of_ppt_ser, dtype="int64").sum()
    print('number of java posts with tags {}'.format(posts_len_java_tags))
    
    ## Make a folder in that directory
    folder = '{}/javaposts_csv'.format(common_path)
    # output: path/to => path/to/javaposts_csv
    mkdir_cmd = 'mkdir {}'.format(folder)
    cmd = sp.run(
        mkdir_cmd, # command
        capture_output=True,
        text=True,
        shell=True
    )

    ## Save files in that directory
    filename = 'JavaPosts'
    file = '{}/{}*.csv'.format(folder, filename)
    _ = ddf.to_csv(file, sep=',', index=False)
    # outpur: path/to/javaposts_csv/JavaPosts*.csv
        

## Start a Dask cluster using SLURM jobs as workers.
#http://jobqueue.dask.org/en/latest/generated/dask_jobqueue.SLURMCluster.html
dask.config.set(
    {
        "distributed.worker.memory.target": False,  # avoid spilling to disk
        "distributed.worker.memory.spill": False,  # avoid spilling to disk
    }
)

cluster = SLURMCluster(
    cores=10, #cores=24, # we set each job to have 1 Worker, each using 10 cores (threads) and 8 GB of memory
    processes=2,
    memory="8GiB",
    walltime="0-30:30",# walltime="0-00:30",
    log_directory="../dask/logs",  # folder for SLURM logs for each worker
    local_directory="../dask",  # folder for workers data
)

#cluster.adapt(minimum_jobs=20, maximum_jobs=200)
cluster.adapt(minimum_jobs=10, maximum_jobs=200)
client = Client(cluster)
client
client.get_versions(check=True)

# Execute the process to transform xml files to csv
fetch_javaposts()
print('Sucessful')