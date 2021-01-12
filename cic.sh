#!/bin/bash

cd /export/ames4/git/cic

source ~/conda/etc/profile.d/conda.sh
conda activate fresh2

thedate=`date +%y%m%d_%H%M`
destpath=/p/user_pub/publish-queue/inconsistencies/$thedate/
cmorpath=/usr/local/cmip6-cmor-tables/Tables/
mkdir -p $destpath



python cic.py $destpath $cmorpath
