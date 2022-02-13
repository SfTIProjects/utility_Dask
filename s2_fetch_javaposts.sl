#!/bin/bash -e
#SBATCH --time=00:15:00
#SBATCH --job-name=s2_fetch_javaposts
#SBATCH --output s2_fetch_javaposts_logs.txt
#SBATCH --hint=multithread
#SBATCH --ntasks=1 --cpus-per-task=2 --mem=2G

module load Python/3.9.5-gimkl-2020a

python s2_fetch_javaposts.py -cfp ../../../../stackexchange_v2/workspace/input -rfp posts/Posts*.csv