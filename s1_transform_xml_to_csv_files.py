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


# transform_xml_to_csv_files.py -xmlf Posts.xml

#1st dataset
# python s1_transform_xml_to_csv_files.py -xmlf ../../../../stackexchange_v2/workspace/input/Posts.xml

#2nd dataset
# python s1_transform_xml_to_csv_files.py -xmlf ../SO_data_dump/Posts.xml

parser = argparse.ArgumentParser(
    description='Read Stack Overflow XML Files'
)

parser.add_argument(
    "-xmlf",
    "--xmlfile",
    type=str, 
    help="Enter the SO xml file path , use spaces to saparate multiple files"
)


args = parser.parse_args()

xml_file = args.xmlfile

xmlfile = xml_file

## Read XML Files

def get_xml_filepath_wtout_ext(file_wt_full_path):
    #/a/b/c.xml => /a/b/c
    return os.path.splitext(file_wt_full_path)[0]

def get_xml_filename(file_wt_full_path):
    filepath_wout_ext = get_xml_filepath_wtout_ext(file_wt_full_path)
    #/a/b/c => c
    return ntpath.basename(filepath_wout_ext)
    
def get_filepath(file_wt_full_path):
    #/a/b/c.csv => /a/b
    return os.path.dirname(file_wt_full_path)

def get_file(file_wt_full_path):
    #/a/b/c.csv => c.csv
    return os.path.basename(file_wt_full_path)

def transform_xml_to_csv_files():
    
    
    ## Read xml file in block sizes
    bg = bd.read_text(xml_file, blocksize='10MB')
            
    #output:
    #\ufeff<?xml version="1.0" encoding="utf-8"?>\n
    #<post> 
    #  <row a=b c=d e=f/>\n
    #  <row a=b c=d e=f/>\n
    #</post>
        
    ## Replace the xml header with empty string '<xml ...>' => ''
    bg = bg.map(lambda line: re.sub(r'\ufeff\s*<\s*\?xml.*\?>\n', '', line))
    #output:
    #''
    #<post> 
    #  <row a=b c=d e=f/>\n
    #  <row a=b c=d e=f/>\n
    #</post>
        
        
    ## Get the filename without the xml extension 
    filename = get_xml_filename(xml_file)
    #output: Post.xml => Post, PostHistory.xml => PostHistory 
        
    ## Make the filename lower case to get a tag
    tag = filename.lower()
    #output: Post => post, PostHistory => posthistory
        
    ## Drop wherever this tag occurs in the xml file
    bg = bg.map(lambda line: re.sub(r'\s*<{}\s*.*?>\n|</{}>'.format(tag, tag), '', line))
    #output:
    #''
    #''
    #  <row a=b c=d e=f/>\n
    #  <row a=b c=d e=f/>\n
    #''
        
        
    ## Filter out just the rows
    bg = bg.filter(lambda line: line.find('<row') >= 0)
    #output
    #  <row a=b c=d e=f/>\n
    #  <row a=b c=d e=f/>\n
        
    ## Use ElementTree to get all the attrbutes in the row tags
    bg = bg.map(lambda row: et.fromstring(row).attrib)
        
    ## Convert Bags to Dataframes
    df = bg.to_dataframe()

    ## Make a folder in that directory
    #folder = get_xml_filepath_wtout_ext(xml_file).lower() # the challenge is that the whole path to lowercase
    # output: path/to/Post.xml => path/to/post
    
    ## I just want the last folder to be a lower case
    path = get_filepath(xml_file)
    # output: path/to/Post.xml => path/to
    
    folder =  '{}/{}'.format(path, filename.lower())
    mkdir_cmd = 'mkdir {}'.format(folder)
    cmd = sp.run(
        mkdir_cmd, # command
        capture_output=True,
        text=True,
        shell=True
    )

    csvfile = '{}/{}*.csv'.format(folder, filename)
    ## Save files in that directory
    _ = df.to_csv(csvfile, sep=',', index=False)
        

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

# Execute the process to transform xml files to csv
transform_xml_to_csv_files()
print('Transformed {} to {}.csv sucessfully'.format(get_file(xmlfile), get_xml_filename(xmlfile)))