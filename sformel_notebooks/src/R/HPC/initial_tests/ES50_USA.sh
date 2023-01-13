#!/bin/bash

#SBATCH --job-name=ES50_USA
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -p workq
#SBATCH --account=sas
#SBATCH --time=10:00:00
#SBATCH -o /home/sformel/job_output/ES50_USA-%j.out
#SBATCH -e /home/sformel/job_output/ES50_USA-%j.error
#SBATCH --mail-type=ALL
#SBATCH --mail-user=sformel@usgs.gov>

module switch PrgEnv-cray/6.0.10 PrgEnv-gnu/6.0.10
module load geos/3.8.1 gdal/3.4.2 udunits2/2.2.26 ImageMagick/7.0.10_60 proj/6.2.1 gsl/2.6 cray-R/4.1.2.0

export R_BATCH_OPTIONS="--no-save"
srun Rscript /home/sformel/ES50_gbif/ES50_USA.R
