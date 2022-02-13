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

#python s4_fetch_javacodes.py -cfp ../../../../stackexchange_v2/workspace/input -rfp javaanswers_csv/JavaAnswers*.csv
#python s4_fetch_javacodes.py -cfp ../SO_data_dump -rfp javaanswers_csv/JavaAnswers*.csv

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

def fetch_javaanswers():
    
    filepath = '{}/{}'.format(common_path, relative_path)
    
    ## Read Java answers file
    ddf_javaanswers = dd.read_csv(filepath, engine='python', error_bad_lines=False, warn_bad_lines=False, dtype=object)
    print(ddf_javaanswers.columns)
    print('Partitions {}'.format(ddf_javaanswers.npartitions))
    
    ddf_javaanswers.columns
   
    ###########################
    # Process the Java Answers#
    ###########################
    
    
    ## 1st Phase Computation 
    #- Join two columns Post 'Id' and 'ParentId'
    #- Set it as the new index to 'PIdx'
    
    start = time.perf_counter() # start timer
    ddf_javaanswers = ddf_javaanswers.reset_index()
    ddf_javaanswers['PIdx'] = ddf_javaanswers['Id'].str.cat(ddf_javaanswers['ParentId'],sep="_")
    #Join two columns 'index' to 'Id' and 'ParentId' 
    ddf_javaanswers['Idx'] = ddf_javaanswers['PIdx'].str.cat(ddf_javaanswers['index'].astype(str),sep="_")
    ddf_javaanswers = ddf_javaanswers.set_index('Idx')

    
    ## 2nd Phase Computation 
    #  - Extract all the code form the answers
    #  - result gives two options
    #     - 1st is enclsed in the code tags
    #     - 2nd ollets the code frim the code tags
    
    ddf_javarawcode = ddf_javaanswers.Body.str.extractall(r'(<code>(.|\n|\r\n)*?<\/code>)')
    #ddf_javarawcode = ddf_javaanswers.Body.str.extractall(r'(<code[^>]*>((?:.|\s)*?)</code>)')
    #https://stackoverflow.com/questions/51212480/regex-for-mask-function-in-dask

    #triggers the compute job but it will keep it on the works without retrieving resulte
    #ddf_javarawcode = ddf_javarawcode.persist()
    
    # rename column 0 to 'code_in_tags' and column 1 to 'Code'
    ddf_javarawcode = ddf_javarawcode.rename(columns={0: 'Code', 1: 'Others'}) 
    
    ddf_javarawcode.columns
    
    # retrieve just the column 1 in a form of dataferam
    ddf_javarawcode = ddf_javarawcode[['Code']].astype(str)
    
    #https://stackoverflow.com/questions/60088353/convert-html-characters-to-strings-in-pandas-dataframe
    #ddf_javarawcode = ddf_javarawcode.applymap(html.unescape)
    
    ddf_javarawcode = ddf_javarawcode.reset_index() # to unstack the group by
    
    #replace the <code> </code> Tags in the code column with empty string
    ddf_javarawcode['Code'] = ddf_javarawcode.Code.str.replace(r'(<code>)|(<\/code>)', r'')
    
    ## 4th Phase Computation 
    # - Check Lengths
    
    #Get the Length of initial java answers
    init_javaanswers_len = len(ddf_javaanswers.index) #####
    
    #Get the Length after raw codes are extracted
    javarawcode_len = len(ddf_javarawcode.index)
    
    #Get the Number of Answers related to Java Post
    print("Initial Number of Java Post: {}".format(init_javaanswers_len))
    
    #Get the Length after raw codes are extracted 
    # because the regex extractall() gets multiple <code>...<\code> matches in just one post
    # the javarawcode_len is expected to be more
    print("Number after Java code after extraction: {}".format(javarawcode_len))
    
    ## 5th Phase Computation 
    #- Save ddf_javarawcode into a csv file
    ### Make a folder in that directory
    ## Make a folder in that directory
    folder = '{}/javarawcodes_csv'.format(common_path)
    # output: path/to/Post.csv => path/to
    mkdir_cmd = 'mkdir {}'.format(folder)
    cmd = sp.run(
        mkdir_cmd, # command
        capture_output=True,
        text=True,
        shell=True
    )
    
    ## Save files in that directory
    filename = 'JavaRawCodes'
    file = '{}/{}*.csv'.format(folder, filename)
    _ = ddf_javarawcode.to_csv(file, sep=',', index=False)
    


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
    memory="32GiB",
    walltime="0-30:30",# walltime="0-00:30",
    log_directory="../dask/logs",  # folder for SLURM logs for each worker
    local_directory="../dask",  # folder for workers data
)

#cluster.adapt(minimum_jobs=20, maximum_jobs=200)
cluster.adapt(minimum_jobs=50, maximum_jobs=200)
client = Client(cluster)
client

# Execute the process to transform xml files to csv
fetch_javaanswers()
print('Sucessful')