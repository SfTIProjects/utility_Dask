#!/bin/bash -e
#SBATCH --time=00:50:00
#SBATCH --job-name=s3_fetch_javaposts
#SBATCH --output s3_fetch_javaanswers_logs.txt
#SBATCH --hint=multithread
#SBATCH --ntasks=1 --cpus-per-task=2 --mem=8G

module load Python/3.9.5-gimkl-2020a

python s3_fetch_javaanswers.py -cfp ../../../../stackexchange_v2/workspace/input -rfpp posts/Posts*.csv  -rfpjp javaposts_csv/JavaPosts*.csv 
