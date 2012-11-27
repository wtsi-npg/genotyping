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


# concoct fake PLINK data for testing genotype pipeline
# initially generate in text-based .ped format, use plink tools to convert to binary

# inputs:
# number of samples
# number of SNPs (X chromosome and other)
# probability of no-calls
# probability of X snps in PAR region (	[60001,	2699520], [154931044, 155260560] on X)
# probability of female sample (male otherwise)
# het rate distributions:
# * Default
# * Xhet, male samples
# * Xhet, female samples

# most params specified in XML config file

# outputs:
# PLINK .ped, .map files for conversion to binary using 'plink --file myfile --make-bed'
# sample names in fake plate_well_id format, to allow per-plate plotting

# tends to produce somewhat "messy" data (ie. poor QC stats), but this is fine for checking QC scripts!
# more realistically, have most samples "good" with a small subpopulation of "bad" samples

# all chromosome annotation contained in .map file (including X/not-X and PAR status)

# also want to generate fake .sim files to accompany PLINK data...

import math, os, random, re, sys, time
from xml.dom import minidom

class plinkGenerator:

    MALE_GENDER = 1
    FEMALE_GENDER = 2
    HIDDEN_PAR = -1 # denotes PAR SNPs annotated as regular X chromosome SNPs
    HIDDEN_PAR_OFFSET = 60001
    MAF_HOM = 0.4 # minor allele frequency for homozygous sites
    NO_CALL_KEY = 'NO_CALL'
    MALE_XHET_KEY = 'MALE_XHET'
    AUTO_HET_KEY = 'AUTO_HET'
    MALE_KEY = 'MALE'
    NAME_PLATE = 0
    NAME_NOPLATE = 1

    def __init__(self, family='family_name', nameType=0, plateRows=12, plateCols=8):
        self.plateRows = plateRows
        self.plateCols = plateCols
        if nameType == self.NAME_PLATE: self.namePlate = True
        else: self.namePlate = False
        self.samplesPerPlate = self.plateRows * self.plateCols
        self.chroms = range(1, 27)
        self.chroms.append(self.HIDDEN_PAR)
        self.family = family

    def getAlleles(self, gender, isX, probs):
        # generate a random pair of allele values for given params
        # assume all calls are A or C (0 for no calls)
        call = True
        het = True
        if random.random() <= probs[self.NO_CALL_KEY]:
            call = False
        elif gender==self.MALE_GENDER and isX: # male X chromosome
            if random.random() <= probs[self.MALE_XHET_KEY]: het = True
            else: het = False
        else:
            if random.random() <= probs[self.AUTO_HET_KEY]: het = True
            else: het = False
        if call:
            if het: alleles = ('A', 'C') 
            elif random.random() < self.MAF_HOM: alleles = ('C', 'C')
            else: alleles = ('A', 'A')
        else:
            alleles = (0,0)
        return alleles

    def getAlleleString(self, gender, isX, probs):
        alleles = self.getAlleles(gender, isX, probs)
        return str(alleles[0])+" "+str(alleles[1])

    def getPedLines(self, sample, snps, probs, makeDuplicate=False, appendNewLine=True):
        # generate line of .ped format input; contains information on SNPs for a single sample
        # optionally, generate a duplicate line (distinct sample name, otherwise identical) for QC test
        # snps = dictionary of SNP counts by chromosome
        # initial .ped fields:
        # [Family ID, Individual ID, Paternal ID, Maternal ID, Sex (1=male; 2=female; other=unknown), Phenotype]
        [paternal, maternal] = [0]*2
        names = [self.getSampleName(sample), ]
        if makeDuplicate: names.append(self.getSampleName(sample+1))
        if random.random() <= probs[self.MALE_KEY]: gender = self.MALE_GENDER
        else: gender = self.FEMALE_GENDER
        phenotype = 0
        fields = [family, None, paternal, maternal, gender, phenotype] # None is placeholder for sample name(s)
        # generate snp calls (or uncalls) and append to fields
        for chrom in self.chroms:
            if not snps.has_key(chrom): continue
            for i in range(snps[chrom]):
                if chrom == 23 or chrom == self.HIDDEN_PAR: isX = True
                else: isX = False
                fields.append(self.getAlleleString(gender, isX, probs))
        # convert output to string(s)
        lines = []
        for name in names:
            fields[1] = name
            words = []
            for field in fields: words.append(str(field))
            line = "\t".join(words)
            if appendNewLine: line = line+"\n"
            lines.append(line)
        return lines
            
    def getSampleName(self, sample, platePrefix='plate', samplePrefix='sample',
                      uri=True):
        # input = sample number (any integer)
        # find sample name in PLATE_WELL_ID format
        # well is in format eg. H10 for row 8, column 10
        # use prefixes to generate plate & sample names
        sampleName = None
        if self.namePlate:
            plateNum = int(math.floor(sample/float(self.samplesPerPlate)))
            try: wellNum = sample % (plateNum*self.samplesPerPlate) # number within well
            except ZeroDivisionError: wellNum = sample
            col = wellNum % self.plateCols 
            row = int(math.floor(wellNum/float(self.plateCols)))
            # convert numbers to strings, starting counts from 1 (not 0)
            plate = platePrefix+str(plateNum+1).zfill(4)
            well = chr(col+65)+str(row+1).zfill(2)
            suffix = samplePrefix+str(sample).zfill(6)
            sampleName = '_'.join([plate, well, suffix])
        else:
            sampleName = samplePrefix+str(sample).zfill(6)
        if uri:
            sampleName = "urn:wtsi:"+sampleName
        return sampleName

    def makePlinkBinary(self, prefix):
        # convert text plink output to binary
        cmd = 'plink --file '+prefix+' --out '+prefix+' --make-bed'
        os.system(cmd)

    def readConfig(self, configPath):
        # read snp totals by chromosome, and probabilities, from xml config path
        doc = minidom.parse(configPath)
        snpElem = doc.documentElement.getElementsByTagName('snps')[0]
        chromElems = snpElem.getElementsByTagName('chromosome')
        snps = {}
        for elem in chromElems:
            elem.normalize()
            name = int(elem.getAttribute('name'))
            total = int(elem.firstChild.data)
            snps[name] = total
        probElem = doc.documentElement.getElementsByTagName('probs')[0]
        probs = {}
        for ch in probElem.childNodes:
            if ch.nodeType != ch.ELEMENT_NODE: continue
            ch.normalize()
            prob = float(ch.firstChild.data)
            probs[ch.tagName] = prob
        return (snps, probs)

    def writeMap(self, outPath, snps, gap=500000, namePrefix="fakeSNP"):
        # generate fake SNP annotation and write in .map format
        # fields: chromosome, SNP ID, genetic distance, base-pair position
        # gap (between SNPs) should be of similar order to 10**6, to allow duplicate check
        dist = 0
        out = open(outPath, 'w')
        count = 0
        for chrom in self.chroms:
            if not snps.has_key(chrom): continue
            for i in range(snps[chrom]):
                if chrom == self.HIDDEN_PAR: 
                    chromOutput = 23
                    pos = i + self.HIDDEN_PAR_OFFSET + 1
                    if (pos >= 2699520): raise ValueError  # end of PAR window
                else: 
                    chromOutput = chrom
                    pos = i*gap + 1 
                count += 1
                name = namePrefix+str(count).zfill(6)
                fields = [chromOutput, name, dist, pos]
                words = []
                for field in fields: words.append(str(field))
                out.write("\t".join(words)+"\n")
        out.close()

    def writePed(self, outPath, samples, snps, probs, duplicates, sampleOffset=0):
        # generate sample data in .ped format and write to given files
        # optionally, include some duplicate samples to test QC
        # sampleOffset = number from which to start counting samples 
        out = open(outPath, 'w')
        i = 0
        j = 0
        while i < (samples-duplicates):
            if j < duplicates: makeDuplicate = True; j+=1
            else: makeDuplicate = False
            pedLines = self.getPedLines(sampleOffset+i, snps, probs, makeDuplicate)
            i += len(pedLines)
            for pedLine in pedLines: out.write(pedLine)
            print "Wrote "+str(i)+" samples."; sys.stdout.flush()
        out.close()

"""
snps = {1:100,
        23:100,
        -1:10,
        }
probs = {'NO_CALL':0.05,
         'MALE':0.5,
         'MALE_XHET':0.02,
         'AUTO_HET':0.25,
}
"""

config = sys.argv[1]
gap = int(sys.argv[2])
sampleTotal = int(sys.argv[3])
duplicates = int(sys.argv[4])
sampleOffset = int(sys.argv[5])
prefix = sys.argv[6]
nameType = int(sys.argv[7])
terms = re.split('/', prefix)
filePrefix = terms.pop()
family = 'family_'+filePrefix
start = time.time()

gen = plinkGenerator(family, nameType)
(snps, probs) = gen.readConfig(config)
mapPath = prefix+'.map'
gen.writeMap(mapPath, snps, gap, namePrefix=prefix+'_fakeSNP')
print "Wrote .map file."
pedPath = prefix+'.ped'
gen.writePed(pedPath, sampleTotal, snps, probs, duplicates, sampleOffset)
gen.makePlinkBinary(prefix)
duration = time.time() - start
print "Finished.  Duration: "+str(duration)+" s"
