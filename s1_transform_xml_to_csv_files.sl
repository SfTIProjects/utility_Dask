#!/bin/bash -e
#SBATCH --time=00:10:00
#SBATCH --job-name=s1_transform_so_data
#SBATCH --output s1_trans_logs.txt
#SBATCH --hint=multithread
#SBATCH --ntasks=1 --cpus-per-task=2 --mem=2G

module load Python/3.9.5-gimkl-2020a

python s1_transform_xml_to_csv_files.py -xmlf ../SO_data_dump/PostHistory.xml
