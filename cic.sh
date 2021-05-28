#!/bin/bash

srcdir=`dirname $0`
cd $srcdir

bash /export/witham3/esgf/conda.sh
. ~/.bashrc

conda activate cic

thedate=`date +%y%m%d_%H%M`
destpath=/p/user_pub/publish-queue/inconsistencies/$thedate/
cmorpath=/usr/local/cmip6-cmor-tables/Tables/
mkdir -p $destpath



nohup python3 cic.py --output-dir $destpath --cmor-tables $cmorpath --email e.witham@columbia.edu ames4@llnl.gov --fix-errors --enable-email > $destpath/cic.$thedate.log
