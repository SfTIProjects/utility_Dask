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

#python s5_fetch_javacodesnippets.py -cfp ../../../../stackexchange_v2/workspace/input -rfp javarawcodes_csv/JavaRawCodes*.csv
#python s5_fetch_javacodesnippets.py -cfp ../SO_data_dump -rfp javarawcodes_csv/JavaRawCodes*.csv

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
    ddf_javarawcodes = dd.read_csv(filepath, engine='python', error_bad_lines=False, warn_bad_lines=False, dtype=object)
    print(ddf_javarawcodes.columns)
    print('Partitions {}'.format(ddf_javarawcodes.npartitions))
    
    ddf_javarawcodes.columns
   
    ############################
    # Process the Java Raw Code#
    ############################
    
    # Keep a copy
    ddf_javarawcodes_temp = ddf_javarawcodes.copy()
    
    ## Drop all row with any column having a NaN value
    ddf_javarawcodes = ddf_javarawcodes[~ddf_javarawcodes.Code.isna()]
    
    # Get all the dropped rows with NA
    javarawcodes_na_df = ddf_javarawcodes_temp[~ddf_javarawcodes_temp.Idx.isin(ddf_javarawcodes.Idx.compute())]
    # Number of dropped records
    javarawcodes_na_len = len(javarawcodes_na_df.index)
    print("Number of NaN records that dropped: {}".format(javarawcodes_na_len))
    
    # Number of records left
    #javarawcodes_notna_len = len(ddf_javarawcodes.index)
    #javarawcodes_notna_len
    
    ## Remove Codes that have any form of html or xml tags ##
    #########################################################
    #### Convert all the unicode strings into actual symbol
    #https://stackoverflow.com/questions/60088353/convert-html-characters-to-strings-in-pandas-dataframe
    ddf_javarawcodes = ddf_javarawcodes.applymap(html.unescape)
    # keep a copy
    ddf_javarawcodes_temp = ddf_javarawcodes.copy()
    
    ## Remove code with any form of HTML/XML
    #Replace all the single line html tags <.../> with empty string
    #javarawcodes_html_df1 = ddf_javarawcodes[ddf_javarawcodes.Code.str.contains('^(//.*|\'|\n|\r|\s|\r\n)*<(.|\n|\r\n)*?>(.|\n|\r\n)*?<\/(.|\n|\r\n)*?(>|\'|\n|\r\n)$', regex=True)]
    #ddf_javarawcodes_htmlxml = ddf_javarawcodes[ddf_javarawcodes.Code.str.contains('<.*>(.|\n|\r\n)*?<\/.*>', regex=True)] # this gets stuck at some point
    ddf_javarawcodes_htmlxml = ddf_javarawcodes[ddf_javarawcodes.Code.str.contains('<.*>(.|\n|\r\n)<\/.*>*?', regex=True)]
    
    # Number of records with html xml tags that dropped
    javarawcodes_htmlxml_len = len(ddf_javarawcodes_htmlxml.index)
    print("Number of html xml records that dropped: {}".format(javarawcodes_htmlxml_len))
    
    # Get all the records that DO NOT contains html tags regex
    ddf_javarawcodes = ddf_javarawcodes[~ddf_javarawcodes.Idx.isin(ddf_javarawcodes_htmlxml.Idx.compute())]
    
    # Number of records left
    #javarawcodes_len = len(ddf_javarawcodes.index)
    #javarawcodes_len
    
    
    
    ## Drop java code with one line of code ##
    ##########################################
    # Keep a copy
    ddf_javarawcodes_temp = ddf_javarawcodes.copy()
    
    # Get df contains more than one line of java code
    ddf_javarawcodes = ddf_javarawcodes[ddf_javarawcodes.Code.str.contains('\n')]
    
    # Number of rows that contains more than one line of java code
    javarawcodes_len = len(ddf_javarawcodes.index)
    print("Number of rows that contains more than one line of java code: {}".format(javarawcodes_len))
    
    # Get the dataframe that contains one line of java code
    javarawcodes_oneline_df = ddf_javarawcodes_temp[~ddf_javarawcodes_temp.Idx.isin(ddf_javarawcodes.Idx.compute())]
    
    # Number of rows that contains one line of java code
    javarawcodes_oneline_len = len(javarawcodes_oneline_df.index)
    print("Number of rows that contains one line of java code: {}".format(javarawcodes_oneline_len))
    
    
    ## Get complete java code snippet ##
    ####################################
    
    # Get the rows that contain import, package or class
    # using regex to search for text that contain import, package or class
    ddf_complete_javacodes = ddf_javarawcodes[(ddf_javarawcodes['Code'].str.contains('^import\s+\w+(\.+\w+)*', case=False, regex=True)) | (ddf_javarawcodes['Code'].str.contains('^package\s+\w+(\.+\w+)*', case=False, regex=True)) | (ddf_javarawcodes['Code'].str.contains('(class|interface)(\s+\w+)(\s+\w+)*(\s*{)', case=False, regex=True))]
    
    
    # Check lenght of complete java code snippet
    complete_javacodes_len = len(ddf_complete_javacodes.index)
    print("Number of complete java code: {}".format(complete_javacodes_len))
    
    
    ## Get incomplete java code snippet ##
    ######################################
    
    # Get all rows not contained in ddf_complete_javacodes
    ddf_incomplete_javacodes = ddf_javarawcodes[~ddf_javarawcodes.Idx.isin(ddf_complete_javacodes.Idx.compute())]
    
    # Check lenght of incomplete java code snippet
    #print("Number of incomplete java code: {}".format(len(ddf_incomplete_javacodes.index)))
    
    ## Furthermore drop codes that starts with $, WARNING:, >java, java, >javac, javac or contain some build, src directories
    ddf_non_codes = ddf_incomplete_javacodes[
        (ddf_incomplete_javacodes.Code.str.contains('^\$.*', case=False, regex=True))|
        (ddf_incomplete_javacodes.Code.str.contains('^WARNING:(\s*\w*)*', case=False, regex=True))|
        (ddf_incomplete_javacodes.Code.str.contains('^($|>|java|javac|jar|javacc)(\s*\w*\.*)*', case=False, regex=True))|
        (ddf_incomplete_javacodes.Code.str.contains('(\\n)*\s*\.+\/+.*\s*\.*\/*(build|src)', case=False, regex=True))
    ]
    
    # Check lenght of the non codes java code snippet 
    non_codes_len = len(ddf_non_codes.index)
    print("Number of non code code: {}".format(non_codes_len))
    
    # get all the dropped rows that starts with $, WARNING:, >java, java, >javac, javac or directory
    ddf_incomplete_javacodes = ddf_incomplete_javacodes[~ddf_incomplete_javacodes.Idx.isin(ddf_non_codes.Idx.compute())]
    
    # Check lenght of incomplete java code snippet after html tags are dropped
    incomplete_javacodes_len = len(ddf_incomplete_javacodes.index)
    incomplete_javacodes_len
    
    print("Number of incomplete java code: {}".format(len(ddf_incomplete_javacodes.index)))
    
    
    ## Make a complete codes folder in that directory ##
    ####################################################
    folder = '{}/javacompletecodesnippets_csv'.format(common_path)
    # output: path/to/Post.csv => path/to
    mkdir_cmd = 'mkdir {}'.format(folder)
    cmd = sp.run(
        mkdir_cmd, # command
        capture_output=True,
        text=True,
        shell=True
    )

    ### Save Java complete codes csv files in that directory
    filename = 'JavaCompleteCodeSnippets'
    file = '{}/{}*.csv'.format(folder, filename)
    _ = ddf_complete_javacodes.to_csv(file, sep=',', index=False)
    
    
    ## Make a incomplete codes folder in that directory ##
    ######################################################
    ## Make a folder in that directory
    folder = '{}/javaincompletecodesnippets_csv'.format(common_path)
    # output: path/to/Post.csv => path/to
    mkdir_cmd = 'mkdir {}'.format(folder)
    cmd = sp.run(
        mkdir_cmd, # command
        capture_output=True,
        text=True,
        shell=True
    )
    
    ### Save Java incomplete codes csv files in that directory
    filename = 'JavaIncompleteCodeSnippets'
    file = '{}/{}*.csv'.format(folder, filename)
    _ = ddf_incomplete_javacodes.to_csv(file, sep=',', index=False)

    
    ## Make a code that contains HTML/XML folder in that directory ##
    #################################################################
    folder = '{}/javahtmlxmltags_csv'.format(common_path)
    # output: path/to/Post.csv => path/to
    mkdir_cmd = 'mkdir {}'.format(folder)
    cmd = sp.run(
        mkdir_cmd, # command
        capture_output=True,
        text=True,
        shell=True
    )
    
    ### Save Java codes that contains HTML/XML csv files in that directory
    filename = 'JavaCodeHtmlXmlTags'
    file = '{}/{}*.csv'.format(folder, filename)
    _ = ddf_javarawcodes_htmlxml.to_csv(file, sep=',', index=False)
    
    
    ## Make a noncode folder in that directory ##
    #############################################
    folder = '{}/noncode_csv'.format(common_path)
    # output: path/to/Post.csv => path/to
    mkdir_cmd = 'mkdir {}'.format(folder)
    cmd = sp.run(
        mkdir_cmd, # command
        capture_output=True,
        text=True,
        shell=True
    )

    ### Save non codes csv files in that directory
    filename = 'NonCodes'
    file = '{}/{}*.csv'.format(folder, filename)
    _ = ddf_non_codes.to_csv(file, sep=',', index=False)
    
    
    

    ## Replace all the interface and class names to all have a common name (e.g. Code) in the ddf_complete_javacodes
    ddf_complete_javacodes = ddf_complete_javacodes.replace(to_replace = 'class\s+\w+', value = 'class Code', regex=True)
    
    # Also Replace interface with the interface Code
    ddf_complete_javacodes = ddf_complete_javacodes.replace(to_replace = 'interface\s+\w+', value = 'interface Code', regex=True)
    
    ## Encapsulate incomplete written java program with public Code { ... }
    ddf_incomplete_javacodes['Code'] = 'public class Code {\n'+ ddf_incomplete_javacodes['Code'].astype(str) +'\n}'
    
    
    ## Merge the data frame containing the completed java codes with the data frame containing the incomplete code (encapsulated with public Code {})
    
    # merge complete and incomplete into one dataframe
    df = dd.concat([ddf_complete_javacodes, ddf_incomplete_javacodes])
    
    
    ## Replace all cases where three dots ... appeared at the begining of a line within the code with comment i.e. //...
    df['Code'] = df.Code.replace(to_replace ='(\\n)+\s*\.{2,}', value = '\n\n//...\n\n', regex=True)
    df['Code'] = df.Code.replace(to_replace ='(\\n)+\s*\.{2,}', value = '\n\n//...\n\n', regex=True)
    df['Code'] = df.Code.replace(to_replace ='(\\n\.)+', value = '\n\n//...\n\n', regex=True)
    df['Code'] = df.Code.replace(to_replace ='\{(\\n)+\s*\.{2,}', value = '{\n\n//...\n\n', regex=True)
    df['Code'] = df.Code.replace(to_replace ='\[(\\n)+\s*\.{2,}', value = '[\n\n//...\n\n', regex=True)
    df['Code'] = df.Code.replace(to_replace ='\{\s*\.{2,}', value = '{\n\n//...\n\n', regex=True)
    df['Code'] = df.Code.replace(to_replace ='\[\s*\.{2,}', value = '[\n\n//...\n\n', regex=True)
    
    
    ## Uniquely identify each class name by appending the index_questionid to the end of the class name
    # To uniquely identify each class, append the index number of each row to Classname

    # e.g. 
    # Code_44671882_44671797_3688_0 
    # Code_44671882_44671797_3688_2
    # Code_44671809_32500182_1042_0
    # ...

    # Explanation:
    # e.g. Code0_44671882_44671797_3688_0
    # CodeUniqueIndex_PostID_ParentID_GroupID_MatchCaseNumber
    # Code0_44671882_44671797_3688_1 did not appear becous it could have been removed while preprocessing

    # Importing re package for using regular expressions 
    import re 

    #def appendIndexToClassNames(index, questionIdx, match, javacode): 
    def appendIndexToClassNames(questionIdx, match, javacode): 

        # Search for class ClassName 
        # i.e. class then any characters repeated any number of times 
        if re.search('(class|interface)\s+\w+', javacode): 

            # Extract the position of end of pattern 
            #start = re.search('(class|interface)\s+\w+', javacode).start()
            end = re.search('(class|interface)\s+\w+', javacode).end()

            # return the cleaned name 
            # return javacode[ : pos]+str(javacode.index)+javacode[pos:]
            return javacode[ : end]+'_'+str(questionIdx)+'_'+str(match)+javacode[end: ]

        else: 
            # if clean up needed return the same name 
            return javacode 
          
            
    # Updated the Java Code answers column  
    df['Code'] = df.apply(
             #lambda row: appendIndexToClassNames(row.name, row['Idx'], row['match'], row['Code']),axis=1)
        lambda row: appendIndexToClassNames(row['Idx'], row['match'], row['Code']),
             axis=1)
   
    ## Make a folder in that directory ##
    #####################################
    folder = '{}/codesnippets_csv'.format(common_path)
    # output: path/to/Post.csv => path/to
    mkdir_cmd = 'mkdir {}'.format(folder)
    cmd = sp.run(
        mkdir_cmd, # command
        capture_output=True,
        text=True,
        shell=True
    )
    
    ### Save files in that directory
    filename = 'JavaCodeSnippets'
    file = '{}/{}*.csv'.format(folder, filename)
    _ = df.to_csv(file, sep=',', index=False)
    
    
    
    
    
    
    
    
    

#http://jobqueue.dask.org/en/latest/generated/dask_jobqueue.SLURMCluster.html
dask.config.set(
    {
        "distributed.worker.memory.target": False,  # avoid spilling to disk
        "distributed.worker.memory.spill": False,  # avoid spilling to disk
    }
)
cluster = SLURMCluster(
    cores=2, #cores=10, # we set each job to have 1 Worker, each using 10 cores (threads) and 8 GB of memory
    processes=2, #processes=2,
    memory="16GiB",
    walltime="1-30:30",# walltime="0-00:50",
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