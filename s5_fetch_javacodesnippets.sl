#!/bin/bash -e
#SBATCH --time=00:50:00
#SBATCH --job-name=s5_fetch_javacodesnippets
#SBATCH --output s5_fetch_javacodesnippets_logs.txt
#SBATCH --hint=multithread
#SBATCH --ntasks=1 --cpus-per-task=4 --mem=8G

module load Python/3.9.5-gimkl-2020a

python s5_fetch_javacodesnippets.py -cfp ../SO_data_dump -rfp javarawcodes_csv/JavaRawCodes*.csv