#! /usr/bin/python

# generate fake sample_xhet_gender.txt

import random, sys

class xhetGenerator:

    def __init__(self, weights, means, sdevs):
        self.weights = weights
        self.means = means
        self.sdevs = sdevs
        self.comps = len(weights) # total components
        self.cumulative = [0, ]
        for i in range(self.comps): self.cumulative.append(sum(weights[0:i+1]))

    def getSamples(self, total):
        samples = [0]*total
        components = [0]*total
        xhet = {}
        for i in range(total): 
            (sample, index) = self.sampleXhet()
            samples[i] = sample
            components[i] = index
            try: xhet[index] += 1
            except KeyError: xhet[index] = 1
        return (samples, components)

    def sampleXhet(self):
        # sample a single xhet value
        start = random.random()
        i = None # component from which to sample
        for j in range(self.comps):
            if start > self.cumulative[j]: i=j
            elif start < self.cumulative[j]: break
        #print start, i
        sample = -1
        while (sample<0 or sample>1):
            # repeat until a legal sample value is obtained
            sample = random.gauss(self.means[i], self.sdevs[i])
        return (sample, i)


    def writeNamedSamples(self, total, outPath, names=None, header=True, 
                          digits=6):
        # write sample_xhet_gender.txt 
        # (use dummy names and supplied/inferred genders if needed)
        (samples, components) = self.getSamples(total)
        out = open(outPath, 'w')
        if header: out.write("sample\txhet\tinferred\tsupplied\n")
        prefix='sample_'
        for i in range(len(samples)): 
            if (names!=None): name = names[i]
            else: name = prefix+("%05d" % (i+1, )) # pad to 5 digits
            if components[i]==0: gender = 1 # male
            elif components[i]==1: gender = random.choice((1,2)) # ambig
            else: gender = 2 # female (normal or high xhet)
            output = "\t".join((name, str(round(samples[i], digits)), 
                                'NA', str(gender) ))+"\n"
            out.write(output)
        out.close()

    def writeSamples(self, total, outPath, digits=6):
        samples = self.getSamples(total)
        out = open(outPath, 'w')
        for sample in samples: out.write(str(round(sample, digits))+"\n")
        out.close()

def main():
    total = int(sys.argv[1]) # total samples to generate
    outPath = sys.argv[2] # output path

    # proportions of male, female, ambiguous, high-xhet samples
    amb = 0.045
    big = 0.005
    male = 0.5 - (amb+big)/2  #0.49
    female = 1 - (male+amb+big)
    # mean and SD for male, female, ambiguous distributions
    muM = 0.01
    muF = 0.25
    muAmb = abs(muF-muM)/2
    muBig = 0.5
    sigM = 0.0015 
    sigF = 0.03
    sigAmb = abs(sigF-sigM)/2
    sigBig = sigF

    weights = (male, amb, female, big)
    means = (muM, muAmb, muF, muBig)
    sdevs = (sigM, sigAmb, sigF, sigBig)

    gen = xhetGenerator(weights, means, sdevs)
    gen.writeNamedSamples(total, outPath)

    


if __name__ == "__main__":
    main()
