#!/bin/bash -e
#SBATCH --time=00:50:00
#SBATCH --job-name=s4_fetch_javaposts
#SBATCH --output s4_fetch_javacodes_logs.txt
#SBATCH --hint=multithread
#SBATCH --ntasks=1 --cpus-per-task=32 --mem=40G

module load Python/3.9.5-gimkl-2020a

python s4_fetch_javacodes.py -cfp ../../../../stackexchange_v2/workspace/input -rfp javaanswers_csv/JavaAnswers*.csv
