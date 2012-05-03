#! /usr/bin/bash

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# April 2012

# script to test gender check is functioning correctly:
# - does script run without an error?
# - is output identical to reference version?

# assume script is being run from genotyping/src/perl/t/xhet_gender directory

# construct command-line options and run check_xhet_gender script
SCRIPTDIR="../../bin/"
SCRIPTNAME="check_xhet_gender.pl"
INPUTDIR="."
OUTPUTDIR="./testGenderOutput"
REFDIR="."
CMD="perl ${SCRIPTDIR}${SCRIPTNAME} --input_dir=${INPUTDIR} --output_dir=${OUTPUTDIR}"
echo $CMD
$CMD
# did perl script run successfully?
if [ $? -eq 0 ]; then 
    echo -e "OK\tcheck_xhet_gender\tExecution test"
else
    echo -e "ERROR\tcheck_xhet_gender\tExecution test"
fi
# do new output and reference output differ?  (Tests sample gender assignment, not detailed model params.)
OLD_GENDER=${REFDIR}/sample_xhet_gender_model.txt
NEW_GENDER=${OUTPUTDIR}/sample_xhet_gender_model.txt
diff --brief $OLD_GENDER $NEW_GENDER #&> /dev/null
if [ $? -eq 0 ]; then 
    echo -e "OK\tcheck_xhet_gender\tOutput test"
else
    echo -e "ERROR\tcheck_xhet_gender\tOutput test"
fi