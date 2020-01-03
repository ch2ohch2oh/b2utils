#!/bin/bash
echo "Setting up basf2 environment..."
source /cvmfs/belle.cern.ch/tools/b2setup

# Use a particular release if speficified
if [ -z "$1" ]; then
    release=`b2help-releases`
else
    release=$1
fi

b2setup $release


