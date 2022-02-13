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


#python s3_fetch_javaanswers.py -cfp ../../../../stackexchange_v2/workspace/input -rfpp posts/Posts*.csv  -rfpjp javaposts_csv/JavaPosts*.csv 
#python s3_fetch_javaanswers.py -cfp ../SO_data_dump -rfpp posts/Posts*.csv  -rfpjp javaposts_csv/JavaPosts*.csv

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
    "-rfpp",
    "--relativefilepathpost",
    type=str, 
    help="Enter the SO xml file path , use spaces to saparate multiple files"
)

parser.add_argument(
    "-rfpjp",
    "--relativefilepathjavapost",
    type=str, 
    help="Enter the SO xml file path , use spaces to saparate multiple files"
)


args = parser.parse_args()

common_path = args.commonfilepath
relative_path_posts = args.relativefilepathpost
relative_path_javaposts = args.relativefilepathjavapost



def get_filepath_wtout_ext(file_wt_full_path):
    #/a/b/c.csv => /a/b/c
    return os.path.splitext(file_wt_full_path)[0]

def get_filename(file_wt_full_path):
    filepath_wout_ext = get_filepath_wtout_ext(file_wt_full_path)
    #/a/b/c => c
    return ntpath.basename(filepath_wout_ext)

def get_filepath(file_wt_full_path):
    #/a/b/c.csv => /a/b
    return os.path.dirname(file_wt_full_path)

def get_file(file_wt_full_path):
    #/a/b/c.csv => c.csv
    return os.path.basename(file_wt_full_path)

#### Things to note about the Answers
#    - They have value of 2 in the PostTypeId column
#    - They also have values in the ParentId column
#        - which referes to the PostId of the Question

def fetch_javaposts():
    
    filepath_posts = '{}/{}'.format(common_path, relative_path_posts) # Post
    filepath_javaposts = '{}/{}'.format(common_path, relative_path_javaposts) # JavaPost
    
    ## Read Post file
    ddf_posts = dd.read_csv(filepath_posts, engine='python', error_bad_lines=False, warn_bad_lines=False, dtype=object)
    print(ddf_posts.columns)
    print('Partitions {}'.format(ddf_posts.npartitions))
    
    ## Read JavaPost file
    ddf_javaposts = dd.read_csv(filepath_javaposts, engine='python', error_bad_lines=False, warn_bad_lines=False, dtype=object)
    print(ddf_javaposts.columns)
    print('Partitions {}'.format(ddf_javaposts.npartitions))
    
    #########################
    # Process the Java Posts#
    #########################
    ## Total number of javaposts
    #  - also known as the Java Questions
    
    ## Get all the java Answers
    # - PostId of Post must match with PostId of JavaPost
    ddf_javaanswers = ddf_posts[ddf_posts.ParentId.isin(ddf_javaposts.Id.compute())] 
    
    
    ## Get the total number of java answers
    javaanswers_len = ddf_javaanswers.index.shape[0].compute() # compute length of javaanswers_df
    
    ## Total number of java answers
    print("Total number of java answers: {}".format(javaanswers_len))
    
    
    ## Make a folder in that directory
    folder = '{}/javaanswers_csv'.format(common_path)
    # output: path/to => path/to/javaposts_csv
    mkdir_cmd = 'mkdir {}'.format(folder)
    cmd = sp.run(
        mkdir_cmd, # command
        capture_output=True,
        text=True,
        shell=True
    )

    ## Save files in that directory
    filename = 'JavaAnswers'
    file = '{}/{}*.csv'.format(folder, filename)
    _ = ddf_javaanswers.to_csv(file, sep=',', index=False)
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
cluster.adapt(minimum_jobs=50, maximum_jobs=200)
client = Client(cluster)
client

# Execute the process to transform xml files to csv
fetch_javaposts()
print('Sucessful')