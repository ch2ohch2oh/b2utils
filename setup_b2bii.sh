#!/bin/bash
# Setting up env variables for B2BII
# See: https://confluence.desy.de/display/BI/Physics+B2BII#PhysicsB2BII-Segmentationviolationforrecentversions

echo "Setting up B2BII env variables..."

# Deal with segmentation violation for some versions
export LD_LIBRARY_PATH=/sw/belle/local/neurobayes-4.3.1/lib/:$LD_LIBRARY_PATH

# Running on data for exp 31-65
export USE_GRAND_REPROCESS_DATA=1

# Set Belle DB
export BELLE_POSTGRES_SERVER=can01

# Access to Belle DB
export PGUSER=g0db

echo "Finished setting up B2BII env varaibles"
