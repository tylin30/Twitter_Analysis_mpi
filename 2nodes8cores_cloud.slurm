#!/bin/bash
#SBATCH -p cloud
#SBATCH --output=2nodes8cores_cloud_result.txt
#SBATCH --error=2nodes8cores_cloud_error.txt
#SBATCH --nodes=2
#SBATCH --ntasks=8
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=1
#SBATCH --time=00:10:00
module load Python/3.7.3-spartan_gcc-8.1.0
mpirun -np 8 python3 twitter_analysis_mpi.py