#! /usr/bin/python

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# April 2012

# repeatedly run stability test with differing numbers of samples

import os, sys

from stabilityTest import stabilityTester

# start, increment and steps for sample total
start = int(sys.argv[1])
incr =  int(sys.argv[2])
steps = int(sys.argv[3])
dataTotal = int(sys.argv[4])
modelTotal = int(sys.argv[5])
outDir = sys.argv[6]

# other parameters set to default values
#dataTotal = 10 # 50 # total datasets
#modelTotal = 10 # 50 # total training repeats per dataset

if not os.path.exists(outDir): os.makedirs(outDir)
out = open(os.path.join(outDir, 'consensus.txt'), 'w')
out.write("# "+"\t".join(sys.argv)+"\n")
for i in range(steps):
    sampleTotal = start + i*incr
    scratch = os.path.join(outDir, 'scratch')
    archive = os.path.join(outDir, 'archive'+str(i).zfill(3))
    for myDir in (scratch, archive):
        if not os.path.exists(myDir): os.makedirs(myDir)
    tester = stabilityTester(scratch, archive)
    dataParams = tester.defaultDataParams()
    consensus = tester.runTrials(dataTotal, sampleTotal, modelTotal, dataParams)
    result = "%s\t%s\t%s" % (i+1, sampleTotal, round(consensus, 5))
    out.write(result+"\n")
    out.flush()
out.close()
