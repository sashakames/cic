#!/bin/bash

cd /export/witham3/etools
export PATH=~/anaconda2/bin:$PATH
. ~/.bashrc
conda activate cic

thedate=`date +%y%m%d_%H%M`
destpath=/p/user_pub/publish-queue/inconsistencies/$thedate

mkdir -p $destpath

python3 cic.py $destpath/ /export/witham3/cmor/
