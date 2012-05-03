#! /usr/bin/bash

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# May 2012

# shortcut to construct command-line options for create_all_qc_plots QC script and run test
# run from genotyping/src/perl/t/qc_plots
SCRIPTDIR="../../bin/"
INPUTDIR=./testInput
OUTPUTDIR=./testOutput
REFDIR=./testOutputRef
LOGPATH=./test.out
echo ${SCRIPTDIR}create_all_qc_plots.pl --input_dir=$INPUTDIR --output_dir=$OUTPUTDIR --ref_dir=$REFDIR --test_log=$LOGPATH
# check if sample_cr_het.txt exists (also need xydiff file in same directory)
if [[ ! -e ${INPUTDIR}/sample_cr_het.txt ]]
then
    echo "Input sample_cr_het.txt not found!"
    exit 1
fi
perl ${SCRIPTDIR}create_all_qc_plots.pl --input_dir=$INPUTDIR --output_dir=$OUTPUTDIR --ref_dir=$REFDIR --test_log=$LOGPATH