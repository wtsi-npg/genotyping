#! /software/bin/python


# Author: Iain Bancarz, ib5@sanger.ac.uk
# July 2012

#
# Copyright (c) 2012 Genome Research Ltd. All rights reserved.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#


# generate plausible fake .sim intensity data for given genotypes

# input: PLINK .ped file

# generate intensities at random:
# # major (signal) component based on genotype
# # minor (noise) component

# some 'no calls' are sampled from a completely uniform noise dsitribution
# bears no relation to what a 'real' genotype caller might decide, but good enough for simple testing

import math, random, re, struct, sys

class simGenerator:

    NO_CALL = 0
    XX_CALL = 1
    XY_CALL = 2
    YY_CALL = 3

    def __init__(self, signalMean=1, signalSD=0.25, noiseMean=0, noiseSD=0.1, bases=['A','C']):
        self.channels = 2
        bases.sort()
        (self.baseX, self.baseY) = bases
        self.signalMean = signalMean
        self.signalSD = signalSD
        self.noiseMean = noiseMean
        self.noiseSD = noiseSD
        self.noCallNoise = 0.2
        self.root2 = math.sqrt(2)
        self.nameSize = 40

    def generateIntensity(self, genotype):
        # generate (x,y) intensities for given genotype, sampled from signal/noise distributions
        # genotype XX is 'all X', YY is 'all Y', XY is 'near Y=X'
        signal = -1
        while signal<0: signal = random.gauss(self.signalMean, self.signalSD) # if signal<0, sample again
        [x,y] = [0]*2
        if genotype==0:
            if random.random() < self.noCallNoise: # nothing but completely uniform noise
                x = random.uniform(0,2)
                y = random.uniform(0,2)
            else:
                x = abs(random.gauss(self.noiseMean, self.noiseSD))
                y = abs(random.gauss(self.noiseMean, self.noiseSD))
        elif genotype==1:
            x = signal
            y = abs(random.gauss(self.noiseMean, self.noiseSD))
        elif genotype==2:
            x = signal / self.root2
            y = x
            x += random.gauss(self.noiseMean, self.noiseSD) # noise may be positive or negative
            y += random.gauss(self.noiseMean, self.noiseSD)
            if x<0: x = signal / self.root2
            if y<0: y = signal / self.root2
        elif genotype==3:
            x = abs(random.gauss(self.noiseMean, self.noiseSD))
            y = signal
        return (x,y)

    def getSimBlock(self, sample, nameSize, signals, numberF=0):
        # convert sample name and list of floats to block of binary entries
        items = []
        items.append(struct.pack(str(nameSize)+'s', sample))
        for sig in signals:
            if numberF==0: 
                packed = struct.pack('f', sig) # IEEE 754 32-bit float
            elif numberF==1: 
                packed = struct.pack('H', int(sig*1000)) # 16-bit unsigned scaled integer
            else: 
                raise ValueError("Incorrect .sim number format")
            items.append(packed)
        return items

    def getSimHeader(self, nameSize, samples, probes, channels=2, numberF=0):
        items = []
        items.append(struct.pack('3s', 'sim')) # magic
        items.append(struct.pack('B', 1)) # .sim version
        items.append(struct.pack('H', nameSize)) # number of bytes occupied by sample name (shorter names padded)
        items.append(struct.pack('I', samples)) 
        items.append(struct.pack('I', probes)) 
        items.append(struct.pack('B', channels)) 
        items.append(struct.pack('B', numberF)) # .sim number format = 0 or 1
        return items

    def readPed(self, inPath):
        # read .ped file and extract genotypes
        # This file is not recommended for large .ped files from 'real' data!
        inFile = open(inPath, 'r')
        results = {}
        samples = []
        while True:
            line = inFile.readline()
            if line=='': break
            words = re.split('\s+', line.strip())
            sample = words[1]
            samples.append(sample)
            calls = words[6:]
            callTotal = len(calls)
            genotypes = []
            i = 0
            while i < len(calls):
                if calls[i]=='0': genotypes.append(self.NO_CALL)
                elif calls[i]==self.baseX and calls[i+1]==self.baseY: genotypes.append(self.XY_CALL)
                elif calls[i]==self.baseY and calls[i+1]==self.baseX: genotypes.append(self.XY_CALL)
                elif calls[i]==self.baseX and calls[i+1]==self.baseX: genotypes.append(self.XX_CALL)
                elif calls[i]==self.baseY and calls[i+1]==self.baseY: genotypes.append(self.YY_CALL)
                else: 
                    sys.stderr.write("WARNING: Unknown genotype bases, sample "+sample+"\n")
                    genotypes.append(self.NO_CALL)
                i += 2
            results[sample] = genotypes
        inFile.close()
        probes = callTotal / 2
        return (results, samples, probes)

    def writeSim(self, outPath, results, samples, probes):
        # write .sim format file
        out = open(outPath, 'w')
        header = self.getSimHeader(self.nameSize, len(samples), probes)
        for field in header: out.write(field)
        for sample in samples:
            genotypes = results[sample]
            intensities = []
            for gt in genotypes:
                intensities.extend(self.generateIntensity(gt))
            itemsBinary = self.getSimBlock(sample, self.nameSize, intensities)
            out.write(''.join(itemsBinary))
        out.close()
        

gen = simGenerator()
inPath = sys.argv[1]
outPath = sys.argv[2]
(results, samples, probes) = gen.readPed(inPath)
gen.writeSim(outPath, results, samples, probes)

#for sample in samples:
#    for gt in results[sample]:
#        [x,y] = gen.generateIntensity(gt)
#        print "%s\t%s\t%s\t%s" % (sample, gt, x, y)


