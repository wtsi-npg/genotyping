#! /software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# October 2012

# find normalized magnitude and xy intensity difference for each sample
# input .sim binary format intensity file
# replaces xydiff.pl

# may take a few hours for full-sized projects, log provides progress report

use strict;
use warnings;
use Getopt::Long;
#use WTSI::NPG::Genotyping::QC::SimFiles qw/writeIntensityMetrics/;
use Inline (C => Config =>
            AUTO_INCLUDE => "#include \"stdio.h\"\n#include \"stdlib.h\"\n#include\"math.h\"\n#include\"inttypes.h\"\n",
            CCFLAGS => '-lm');
use Inline (C => Config => DIRECTORY => 'inline_dir');
use Inline C => 'DATA';

my ($help, $inPath, $outPathXY, $outPathMag, $probeNum, $probeDefault,
    $logPath, $logDefault, $outMag, $outXY);

$probeDefault = 5000;
$logDefault = "./intensity_metrics.log";

GetOptions("help"         => \$help,
           "input:s"      => \$inPath, # optional
           "log=s"        => \$logPath,
           "xydiff=s"     => \$outPathXY,
           "magnitude=s"  => \$outPathMag,
           "probes=s"     => \$probeNum,
    );

if ($help) {
    print STDERR "Usage: $0 [ options ]
Options:
--input           Input path in .sim format; if blank, use standard input.
--log             Log path; defaults to $logDefault
--magnitude       Output path for normalised magnitudes (required)
--xydiff          Output path for xydiff (required)
--probes          Size of probe input block; default = $probeDefault
--help            Print this help text and exit
";
    exit(0);
}

$logPath ||= $logDefault;
$probeNum ||= $probeDefault; # number of probes to read in at one time

#writeIntensityMetrics($inPath, $outPathMag, $outPathXY, $logPath, $probeNum);

my $verbose=1;

FindMetrics($inPath, $outPathMag, $outPathXY, $verbose);


__DATA__
__C__

# define _FILE_OFFSET_BITS 64 // enable handling of large .sim files

# define HEADSIZE 16  // size in bytes of .sim file header

typedef struct simhead simhead;
struct simhead {
    // variables in header
    char* magic;
    unsigned char version;
    uint16_t nameSize;
    uint32_t samples;
    uint32_t probes;
    unsigned char channels;
    unsigned char format;
    // additional variables for derived quantities
    int numericBytes;
    int sampleUnitBytes;
};

void metricsFromFile(char* inPath, float mags[], float xyds[], char* names[],
                     int *np, char verbose);
void magnitudes(int total, float input[], float results[]);
void readSampleProbes(FILE *in, int sampleOffset, struct simhead header, 
		      float *signals, int *nans, char *name);
void readHeader(FILE *in, struct simhead *h);
void printHeader(struct simhead *h);
float sampleMag(int totalSamples, float signals[], float magByProbe[]);
float sampleXYDiff(int totalSamples, float signals[]);
void writeResults(char* outPath, int total, char* names[], float results[]);
void FindMetrics(SV* args, ...);

/*********************************************************/

void metricsFromFile(char* inPath, float mags[], float xyds[], 
                     char* names[], int *np, char verbose) {
    /* Read a .sim file; find sample names and intensity metrics */
    FILE *in;
    in = fopen(inPath, "r");
    struct simhead header;
    struct simhead *hp;
    hp = &header;
    readHeader(in, hp);
    if (verbose) { printHeader(hp); }
    /* read intensities and compute metrics
     * need to normalize sample magnitude by mean magnitude for each probe
     */
    int total;
    total = header.probes * header.channels;
    float *signals, *magByProbe;
    char *name;
    name = (char*) malloc(header.nameSize+1);
    signals = (float*) malloc(total*sizeof(float));
    magByProbe = (float*) malloc(header.probes*sizeof(float));
    // first pass -- find mean magnitude of intensity by probe
    int i;
    int j;
    for (i=0;i<header.samples;i++) {
        float probeMags[header.probes];
        readSampleProbes(in, i, header, signals, np, name);
	if (i==0) {
	  for (j=0;j<10;j++) { printf("%f\t%f\n", signals[j], signals[j+1]); }
	}
        magnitudes(header.probes*header.channels, signals, probeMags);
        for (j=0;j<header.probes;j++) {
            magByProbe[j] += probeMags[j];
        }
    }
    for (i=0;i<header.probes;i++) {
        magByProbe[i] = magByProbe[i] / header.samples;
    }
    if (verbose) { 
      printf("Found mean magnitude by probe.\n"); 
      for (i=0;i<5;i++) { printf("%f\n", magByProbe[i]); }
    }
    // second pass -- find xydiff and normalized magnitude for each sample
    for (i=0;i<header.samples;i++) {
      readSampleProbes(in, i, header, signals, np, name);
      //printf("%d:%s\n", i, name);
      names[i] = name;
      mags[i] = sampleMag(header.samples, signals, magByProbe);
      xyds[i] = sampleXYDiff(header.samples, signals);
    }
    /* Check that end of .sim file has been reached */
    char last;
    last = fgetc(in);
    if (last==EOF) { 
      if (verbose) { printf("OK: End of .sim file found.\n"); }
    } else { 
      fprintf(stderr, "ERROR: Data found after expected end of .sim file.\n"); 
      //fprintf(stderr, "FILE_POSITION:%llu\n", ftello(in));
      //exit(1);
    }
    fclose(in);
    if (verbose) { printf("NaNs found:%d\n", *np); }
    free(signals);
}


void magnitudes(int total, float input[], float results[]) {
    /* find magnitudes of a list of (pairs of) input floats */
    int i = 0;
    while (i<total) {
        float a = input[i];
        float b = input[i+1];
        results[i/2] = sqrt(a*a + b*b);
        i+=2;
    }
}

void readSampleProbes(FILE *in, int sampleOffset, struct simhead header, 
		      float *signals, int *nans, char *name) {
    /* Read intensities for the Nth sample in the file
       Record number of NaN intensites, and convert NaN values to 0 

       start = header + sample offset + name + probe offset
       start may be very high for large files, so use 'long long' type
    */
  
  unsigned long long start, sampleUnitBytesL, offsetL, headSizeL;
  sampleUnitBytesL = (unsigned long long) header.sampleUnitBytes;
  offsetL = (unsigned long long) sampleOffset;
  headSizeL = (unsigned long long) HEADSIZE;
  start = headSizeL + (offsetL * sampleUnitBytesL);
  int signalTotal = header.probes * header.channels;
  int result;
  result = fseeko(in, start, 0); 
  if (result!=0) {  
    fprintf(stderr, "ERROR: Seek failed in .sim file.\n");  
    fprintf(stderr, "OFFSET:%llu\n", offsetL);
    fprintf(stderr, "UNIT_BYTES:%llu\n", sampleUnitBytesL);
    fprintf(stderr, "ATTEMPTED:%llu\n", start);
    fprintf(stderr, "FILE_POSITION:%llu\n", ftello(in));
    exit(1);
  }
  fgets(name, header.nameSize+1, in);
  fread(signals, header.numericBytes, signalTotal, in);
  // loop over signals, convert nan's to 0 and count nan's
  int i;
  *nans = 0;
  for (i=0;i<signalTotal;i++) {
    if (isnan(signals[i])) {
      signals[i] = 0;
      *nans++;
    }
  }
}

void readHeader(FILE *in, struct simhead *hp) {
  /* Reader header from a .sim file in standard format */
  rewind(in);
  char *magic = malloc(4);
  fread(magic, 1, 3, in);
  unsigned char version;
  fread(&version, 1, 1, in);
  uint16_t nameSize;
  fread(&nameSize, 2, 1, in);
  uint32_t samples;
  fread(&samples, 4, 1, in);
  uint32_t probes;
  fread(&probes, 4, 1, in);
  unsigned char channels;
  fread(&channels, 1, 1, in);
  unsigned char format;
  fread(&format, 1, 1, in);
  
  (*hp).magic = magic;
  (*hp).version = version;
  (*hp).nameSize = nameSize;
  (*hp).samples = samples;
  (*hp).probes = probes;
  (*hp).channels = channels;
  (*hp).format = format;
  int nb;
  if (format == 0) { nb = 4; }
  else if (format == 1) { nb = 2; }
  (*hp).numericBytes = nb;
  (*hp).sampleUnitBytes = nameSize + (probes * channels * nb);
}

void printHeader(struct simhead *hp) {
    printf("MAGIC:%s\n", (*hp).magic);
    printf("VERSION:%d\n", (*hp).version);
    printf("NAME_SIZE:%d\n", (*hp).nameSize);
    printf("SAMPLES:%d\n", (*hp).samples);
    printf("PROBES:%d\n", (*hp).probes);
    printf("CHANNELS:%d\n", (*hp).channels);
    printf("FORMAT:%d\n", (*hp).format);
    printf("NUMERIC_BYTES:%d\n", (*hp).numericBytes);
    printf("SAMPLE_UNIT_BYTES:%d\n", (*hp).sampleUnitBytes);
}

float sampleMag(int totalSamples, float signals[], float magByProbe[]) {
    /* Find mean magnitude of intensity for given sample
     * Normalize by mean magnitude for each probe 
     * Assumes data has exactly 2 intensity channels */
    int i = 0;
    float mag = 0.0;
    int totalSignals = totalSamples*2;
    while (i<totalSignals) {
        float a = signals[i];
        float b = signals[i+1];
        mag += (sqrt(a*a + b*b))/magByProbe[i/2]; 
        i+=2;
    }
    mag = mag / totalSamples;
    return(mag);
}

float sampleXYDiff(int totalSamples, float signals[]) {
    /* Find mean xydiff for given sample 
     * By definition, xydiff = (second intensity) - (first intensity )
     * Assumes data has exactly 2 intensity channels */
    int i = 0;
    float xyd = 0.0;
    int totalSignals = totalSamples*2;
    while (i<totalSignals) {
        xyd += signals[i+1] - signals[i];
        i+=2;
    }
    xyd = xyd / totalSamples;
    return(xyd);
}

void writeResults(char* outPath, int total, char* names[], float results[]) {
  /* Write arrays of names and metrics to given output path */
  FILE *out;
  out = fopen(outPath, "w");
  int i;
  for (i=0;i<total;i++) {
    fprintf(out, "%s\t%f\n", names[i], results[i]);
  }
  fclose(out);
}


void FindMetrics(SV* args, ...) {

   Inline_Stack_Vars;
   char *inPath = SvPV(Inline_Stack_Item(0), PL_na);
   char *magPath = SvPV(Inline_Stack_Item(1), PL_na);
   char *xydPath = SvPV(Inline_Stack_Item(2), PL_na);
   char verbose =  SvPV(Inline_Stack_Item(3), PL_na);

   // need header to find length of results arrays
   struct simhead header;
   struct simhead *hp;
   FILE *in;
   in = fopen(inPath, "r");
   if (in==NULL) {
     perror("Could not open .sim file");
     exit(1);
   }
   hp = &header;
   readHeader(in, hp);
   fclose(in);

   float mags[header.samples];
   float xyds[header.samples];
   char* names[header.samples];
   int nans = 0;
   int *np;
   np = &nans;
   metricsFromFile(inPath, mags, xyds, names, np, verbose);
   printf("Finished.\n");
   writeResults(magPath, header.samples, names, mags);
   writeResults(xydPath, header.samples, names, xyds);
}

