#! /usr/bin/python

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# April 2012

# want to test stability of mixture model
# how consistent are thresholds for a given data set?
# important factors: number of training points, "messiness" of data (component s.d. and noise factors)

# want to find minimum "safe" number of training points to carry out check

# idea: repeatedly generate test data, and repeat training for each test data set
# check for "non-equivalent" models on each data set

import os, re, sys, time
from copy import copy
from concoctXhet import xhetGenerator

class stabilityTester:

    def __init__(self, scratchDir, archiveDir):
        self.scratchDir = scratchDir
        self.archiveDir = archiveDir
        self.keys1 = ['loglik_final', 'Max_xhet_M', 'Min_xhet_F']
        self.keys2 = ['lambda', 'mu', 'sigma']
        self.allKeys = self.keys1+self.keys2
        self.scriptDir = '/nfs/users/nfs_i/ib5/mygit/genotype_qc/'
        self.trainScript = 'check_xhet_gender'
        self.dataName = 'sample_xhet_gender.txt'
        self.modelSummaryName = 'sample_xhet_gender_model_summary.txt'
        subs = (self.scriptDir, self.trainScript, self.scratchDir,  self.scratchDir)
        self.trainCmd = "perl %s%s --input_dir=%s --output_dir=%s --cancel_sanity_check >& /dev/null" % subs
        self.maxDist = 1e-5

    def runTrials(self, generateTotal, sampleTotal, trainTotal, dataParams, digits=5):
        logPath = os.path.join(self.archiveDir, 'log.txt')
        log = open(logPath, 'w')
        log.write("Generating xhet data sets:\n")
        log.write("Data_total\t"+str(generateTotal)+"\n")
        log.write("Sample_total\t"+str(sampleTotal)+"\n")
        log.write("Model_total\t"+str(trainTotal)+"\n")
        names = self.keys2
        for i in range(len(dataParams)):
            words = [names[i], ]
            for p in dataParams[i]: words.append(str(p))
            log.write('\t'.join(words)+'\n')
        log.flush()
        cRates = [] # consensus rates
        for i in range(generateTotal):
            self.generateData(dataParams, sampleTotal)
            (allParams, consensus) = self.repeatTraining(trainTotal)
            cRates.append(consensus)
            log.write("%s\t%s\t%s\n" % (i, round(consensus, digits), time.time()))
            log.flush()
            self.archiveData(i)
            self.archiveParams(i, allParams)
        log.close()
        cMean = sum(cRates)/len(cRates) 
        return cMean

    def archiveData(self, index, fill=3):
        archiveName = 'sample_xhet_gender'+str(index).zfill(fill)+'.txt'
        cmd = 'cp %s %s' % (os.path.join(self.scratchDir, self.dataName), 
                            os.path.join(self.archiveDir, archiveName))
        os.system(cmd)

    def archiveParams(self, index, allParams, fill=3):
        archiveName = 'model_params'+str(index).zfill(fill)+'.txt'
        headers = ['Trial',]
        for key in self.keys1: 
            headers.append(key)
        for key in self.keys2:
            for i in ('1', '2'): headers.append(key+'_'+i)
        out = open(os.path.join(self.archiveDir, archiveName), 'w')
        out.write('\t'.join(headers)+'\n')
        for i in range(len(allParams)):
            indices = self.getWeightIndices(allParams[i])
            words = [str(i+1),]
            for key in self.allKeys: 
                if key in self.keys1: 
                    words.append(str(allParams[i][key]))
                else:
                    for j in indices:
                        words.append(str(allParams[i][key][j]))
            out.write('\t'.join(words)+'\n')
        out.close()

    def generateData(self, dataParams, total):
        # generate data from given mixture params
        (weights, means, sdevs) = dataParams
        outPath = os.path.join(self.scratchDir, self.dataName)
        generator = xhetGenerator(weights, means, sdevs)
        generator.writeNamedSamples(total, outPath)

    def repeatTraining(self, reps, verbose=False):
        # repeatedly train mixture models on data in scratch directory, and find param distance
        allParams = []
        for i in range(reps):
            if verbose: print "Training repeat %s of %s" % (i+1, reps)
            os.system(self.trainCmd)
            params = self.readModelParams(os.path.join(self.scratchDir, self.modelSummaryName))
            allParams.append(params)
        consensus = self.findConsensusRate(allParams)
        return (allParams, consensus)

    def findConsensusRate(self, allParams):
        # find frequency of 'consensus model' (most common params)
        # compare xhet boundaries
        counts = {}
        for params in allParams:
            bounds = (params['Max_xhet_M'], params['Min_xhet_F'])
            try: counts[bounds] += 1
            except KeyError: counts[bounds] = 1
        maxCount = 0
        for bound in counts.keys():
            if counts[bound] > maxCount: maxCount = counts[bound]
        consensus = maxCount / float(len(allParams))
        return consensus

    def clusterDistances(self, dists):
        # do clustering of models by param distance
        pass            

    def readModelParams(self, modelSummaryPath):
        # read params from sample_xhet_gender_model_summary.txt file
        exprs = []
        for key in self.allKeys: exprs.append(re.compile(key))
        params = {}
        lines = open(modelSummaryPath).readlines()
        for line in lines:
            for i in range(len(self.allKeys)):
                if exprs[i].match(line):
                    words = re.split('\s+', line.strip())
                    key = self.allKeys[i]
                    if key in self.keys1:
                        params[key] = float(words[1])
                    else:
                        params[key] = (float(words[1]), float(words[2]))
                    break
        return params

    def getWeightIndices(self, params):
        # get param indices in order of increasing mean
        means = params['mu']
        if means[0] <= means[1]: indices = (0,1)
        else: indices = (1,0)
        return indices

    def paramsDist(self, params1, params2):
        # define distance as maximum absolute difference between corresponding param elements
        # 'corresponding' = similar lambda
        deltas = []
        indices1 = self.getWeightIndices(params1)
        indices2 = self.getWeightIndices(params2)
        for key in self.allKeys:
            if key in self.keys1: 
                delta = abs(params1[key] - params2[key])
            else:
                deltaList = []
                for i in range(len(params1[key])):
                    d = abs(params1[key][indices1[i]] - params2[key][indices2[i]])
                    deltaList.append(d)
                delta = max(deltaList)
            deltas.append(delta)
        return max(deltas)

    def defaultDataParams(self):
        # return default generation params if needed
        # proportions of male, female, ambiguous samples
        amb = 0.05
        male = 0.5 - amb/2  #0.495
        female = 1 - (male+amb)
        # mean and SD for male, female, ambiguous distributions
        muM = 0.01
        muF = 0.25
        muAmb = abs(muF-muM)/2
        sigM = 0.0015 
        sigF = 0.03
        sigAmb = sigF
        weights = (male, amb, female)
        means = (muM, muAmb, muF)
        sdevs = (sigM, sigAmb, sigF)
        dataParams = (weights, means, sdevs)
        return dataParams

def main():

    dataTotal = int(sys.argv[1])
    sampleTotal = int(sys.argv[2])
    modelTotal = int(sys.argv[3])
    #scratch = './scratch/'
    #archive = './archive/'
    scratch = sys.argv[4]
    archive = sys.argv[5]

    tester = stabilityTester(scratch, archive)
    dataParams = tester.defaultDataParams()
    tester.runTrials(dataTotal, sampleTotal, modelTotal, dataParams)

if __name__ == "__main__":
    main()




