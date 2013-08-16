# Author:  Iain Bancarz, ib5@sanger.ac.uk
# May 2012

# module to read .sim intensity files
# use to find magnitude & xydiff metrics for QC
# Modified August 2013 to use inline C for greater speed

package WTSI::NPG::Genotyping::QC::SimFiles;

use strict;
use warnings;
use Carp;
use Exporter;
use Inline (C => Config =>
	    AUTO_INCLUDE => "#include \"stdio.h\"\n#include \"stdlib.h\"\n".
            "#include \"string.h\"\n#include \"math.h\"\n".
            "#include \"inttypes.h\"\n",
            CCFLAGS => '-lm');
use Inline C => 'DATA';

# Setting the Inline DIRECTORY parameter from user input (eg. command line 
# argument) does not work for module import. Instead, the 
# PERL_INLINE_DIRECTORY environment variable is set in the relevant Percolate 
# workflows. It can also be set by the user if running QC manually.

our @ISA = qw/Exporter/;
our @EXPORT_OK = qw/printSimHeader writeIntensityMetrics/;

sub printSimHeader {
    my $simPath = shift;
    PrintSimHeader($simPath);
}

sub writeIntensityMetrics {
    # find xydiff and normalised magnitude, and write to file
    # alias for FindMetrics from in-line C
    # arguments: input, magnitude output, xydiff output, verbose
    FindMetrics(@_);
    return 1;
}

1;

__DATA__
__C__

/*********************************************************/

# define _FILE_OFFSET_BITS 64 // enable handling of large .sim files

# define HEADSIZE 16  // size in bytes of .sim file header
# define MAX_ARG_LEN 1000 // maximum length of command-line argument

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

void findMagByProbe(FILE *in, struct simhead header, float magByProbe[]);
void metricsFromFile(char* inPath, float mags[], float xyds[], char* names[],
                     char verbose);
void readSampleProbes(FILE *in, int sampleOffset, struct simhead header, 
                      float signals[], long *nans, char *name, char verbose);
struct simhead readHeader(FILE *in);
struct simhead readHeaderFromPath(char* inPath);
void printHeader(struct simhead *h);
float sampleMag(int totalProbes, float signals[], float magByProbe[]);
float sampleXYDiff(int totalProbes, float signals[]);
void writeResults(char* outPath, int total, char* names[], float results[]);
void FindMetrics(SV* args, ...);
void PrintSimHeader(SV* args, ...);

/*********************************************************/

void findMagByProbe(FILE *in, struct simhead header, float magByProbe[]) {
  /* Find mean magnitude for each probe 
   Name and NaN count required for readSampleProbes, but not used here */
  int i, j;
  char *name;
  long totalSignals = header.probes * header.channels;
  float *signals;
  signals = calloc(totalSignals, sizeof(float));
  name = (char*) malloc(header.nameSize+1);
  long nans = 0;
  char verbose = 1;
  for (i=0;i<header.probes;i++) { 
      magByProbe[i]=0.0; 
  }
  for (i=0;i<header.samples;i++) {
      readSampleProbes(in, i, header, signals, &nans, name, verbose);
      for (j=0;j<totalSignals;j+=2) {
          float a = signals[j];
          float b = signals[j+1];
          magByProbe[j/2] += sqrt(a*a + b*b);
      }
  }
  for (i=0;i<header.probes;i++) {
    magByProbe[i] = magByProbe[i] / header.samples;
  }
}

void metricsFromFile(char* inPath, float mags[], float xyds[], 
                     char* names[], char verbose) {
  /* Read a .sim file; find sample names and intensity metrics */
  FILE *in;
  in = fopen(inPath, "r");
  if (in==NULL) {
      fprintf(stderr, "ERROR: Could not open .sim file %s\n", inPath);
      exit(1);
  }
  struct simhead header = readHeader(in);
  struct simhead *hp;
  hp = &header;
  if (verbose) { 
      printf("###\nHeader data from .sim file:\n");
      printHeader(hp); 
      printf("###\n");
  }
  /* read intensities and compute metrics
   * need to normalize sample magnitude by mean magnitude for each probe */
  int total;
  float signals[header.probes*2];
  float magByProbe[header.probes];
  char *name;
  name = (char*) malloc(header.nameSize+1);
  total = header.probes * header.channels;
  if (verbose) { printf("First pass; finding magnitude by probe.\n");  }
  // first pass -- find mean magnitude of intensity by probe
  findMagByProbe(in, header, magByProbe);
  if (verbose) { printf("Second pass; finding metrics.\n"); }
  // second pass -- find xydiff and normalized magnitude for each sample
  int i;
  long nans = 0;
  char pVerbose = 0; // readSampleProbes was verbose in findMagByProbe
  for (i=0;i<header.samples;i++) {
      readSampleProbes(in, i, header, signals, &nans, name, pVerbose);
      strcpy(names[i], name); 
      mags[i] = sampleMag(header.probes, signals, magByProbe);
      xyds[i] = sampleXYDiff(header.probes, signals);
  }
  if (verbose) { printf("Found intensity metrics.\n"); }
  /* Check that end of .sim file has been reached */
  char last;
  last = fgetc(in);
  if (last!=EOF) { 
    fprintf(stderr, "ERROR: Data found after expected end of .sim file.\n");
    exit(1);
  } else if (verbose) { printf("OK: End of .sim file reached.\n"); }
  int status = fclose(in);
  if (status != 0) {
      fprintf(stderr, "ERROR: Could not close .sim file %s\n");
      exit(1);
  } else if (nans>0) { 
      fprintf(stderr, "NaN and infinities found: %ld\nAll NaN and infinite intensities set to zero when computing metrics.\n", nans); 
  } else if (verbose) { 
      printf("Finished; no NaN/infinity inputs found.\n");  
  }
}

void readSampleProbes(FILE *in, int sampleOffset, struct simhead header, 
                      float signals[], long *nans, char *name, char verbose) {
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
    fprintf(stderr, "ATTEMPTED_SEEK_POSITION:%llu\n", start);
    fprintf(stderr, "TELL_POSITION:%llu\n", ftello(in));
    exit(1);
  }
  fgets(name, header.nameSize+1, in);
  fread(signals, header.numericBytes, signalTotal, in);
  if (ferror(in)) {
      fprintf(stderr, "ERROR: Failed to read sample data from input .sim\n");
      exit(1);
  }
  /* loop over signals, convert NaN/infinity to 0 and count occurrences
     Update a global count of nans/infinities, and local counts for sample
     Warn if sample nan/infinity count is greater than zero
   */
  int i;
  int nanCount = 0;
  int infCount = 0;
  for (i=0;i<signalTotal;i++) {
      if (isnan(signals[i])) {
          signals[i] = 0;
          (*nans)++;
          nanCount++;
      } else if (signals[i]==INFINITY || signals[i]==-INFINITY) {
          signals[i]=0;
          (*nans)++;
          infCount++;
      }
  }
  if (verbose && (nanCount > 0 || infCount > 0)) {
      fprintf(stderr, "Warning: Sample offset %d, name %s: Found %d NaN values, %d infinities.\n", sampleOffset, name, nanCount, infCount);
  }
}

struct simhead readHeader(FILE *in) {
  /* Reader header from a .sim file in standard format */
  rewind(in);
  char *magic = calloc(4, sizeof(char));
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
  if (ferror(in)) {
      fprintf(stderr, "ERROR: Failed to read header from input .sim\n");
      exit(1);
  }
  int nb, sampleUnitBytes;
  if (format == 0) { nb = 4; }
  else if (format == 1) { nb = 2; }
  else { fprintf(stderr, "ERROR: Invalid header format code\n"); exit(1);  }
  sampleUnitBytes = nameSize + (probes * channels * nb);
  struct simhead header = { magic, version, nameSize, samples, probes, 
                            channels, format, nb, sampleUnitBytes };
  return header;
}

struct simhead readHeaderFromPath(char* inPath) {
  FILE *in;
  in = fopen(inPath, "r");
  if (in==NULL) {
    perror("ERROR: Could not open .sim file");
    exit(1);
  }
  struct simhead header = readHeader(in);
  int status = fclose(in);
  if (status!=0) {
    perror("ERROR: Could not close .sim file");
    exit(1);
  }
  return header;
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


float sampleMag(int totalProbes, float signals[], float magByProbe[]) {
  /* Find mean magnitude of intensity for given sample
   * Normalize by mean magnitude for each probe 
   * Assumes data has exactly 2 intensity channels */
  int i = 0;
  float mag = 0.0;
  long totalSignals = totalProbes*2;
  while (i<totalSignals) {
    float a = signals[i];
    float b = signals[i+1];
    mag += sqrt(a*a + b*b)/magByProbe[i/2]; 
    i+=2;
  }
  mag = mag / totalProbes;
  return(mag);
}

float sampleXYDiff(int totalProbes, float signals[]) {
  /* Find mean xydiff for given sample 
   * By definition, xydiff = (second intensity) - (first intensity )
   * Assumes data has exactly 2 intensity channels */
  int i = 0;
  float xyd = 0.0;
  long totalSignals = totalProbes*2;
  while (i<totalSignals) {
      xyd += signals[i+1] - signals[i];
      i+=2;
  }
  xyd = xyd / totalProbes;
  return(xyd);
}

void writeResults(char* outPath, int total, char* names[], float results[]) {
  /* Write arrays of names and metric values to given output path */
  FILE *out;
  out = fopen(outPath, "w");
  if (out==NULL) {
       fprintf(stderr, "ERROR: Could not open output %s\n", outPath);
       exit(1);
  }
  int i, status;
  for (i=0;i<total;i++) {
      status = fprintf(out, "%s\t%f\n", names[i], results[i]);
    if (status<=0) {
        fprintf(stderr, "ERROR: Failed to write output to %s\n", outPath);
        exit(1);
    }
  }
  status = fclose(out);
  if (status != 0) {
      fprintf(stderr, "ERROR: Could not close output %s\n", outPath);
      exit(1);
  }
}

void FindMetrics(SV* args, ...) {
  /* Find magnitude and xydiff metrics */
  Inline_Stack_Vars;
  char *inPath = SvPV(Inline_Stack_Item(0), PL_na);
  char *magPath = SvPV(Inline_Stack_Item(1), PL_na);
  char *xydPath = SvPV(Inline_Stack_Item(2), PL_na);
  bool verboseB = SvTRUE(Inline_Stack_Item(3));
  char verbose;
  if (verboseB) { verbose = 1; }
  else { verbose = 0; }
  if (verbose) { printf("Starting intensity metric calculation.\n"); }
 // need header to find length of results arrays
    struct simhead header = readHeaderFromPath(inPath);
    struct simhead *hp;
    hp = &header;
    float mags[header.samples];
    float xyds[header.samples];
    // create array of char pointers with enough space for each name
    char **names; 
    names = calloc(header.samples, sizeof(char*));
    if (names==NULL) { 
        fprintf(stderr, "ERROR: Could not allocate name array memory\n");
    }
    int i;
    for (i=0;i<header.samples;i++) {
        names[i] = calloc(header.nameSize+1, sizeof(char));
        if (names[i]==NULL) {
            fprintf(stderr, "ERROR: Could not allocate name memory\n");
            exit(1);
        }
        mags[i] = 0.0;
        xyds[i] = 0.0;
    }
    metricsFromFile(inPath, mags, xyds, names, verbose);
    writeResults(magPath, header.samples, names, mags);
    writeResults(xydPath, header.samples, names, xyds);
    if (verbose) { printf("Finished.\n"); }
}

void PrintSimHeader(SV* args, ...) {
    /* Read header from a .sim file and print to stdout  */
    Inline_Stack_Vars;
    char *inPath = SvPV(Inline_Stack_Item(0), PL_na);
    struct simhead header = readHeaderFromPath(inPath);
    struct simhead *hp;
    hp = &header;
    printHeader(hp);
}
