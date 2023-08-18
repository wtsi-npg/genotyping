# Fluidigm data processing

The Fluidigm EP1 system is a micro-fluidic analysis platform that provides WSI
with SNP genotype data for small SNP panels (24, 48, 96, 192, 384 SNPs per
sample).

(The Fluidigm company is now [Standard Bio](https://www.standardbio.com/) and
the Fluidigm EP1 system is no longer available for purchase.)

## Application at WSI

This system is commonly used to record SNPs (single-nucleotide polymorphisms)
used to the confirm identity and/or sex of samples that are being processed
through the Illumina pipeline.

## Overview of the process

1. Samples are tracked in LIMS (SequenceScape).
2. Samples are processed in the laboratory on a Fluidigm EP1 instrument, in batches.
3. Result data are written to an NFS filesystem, one directory per batch.
4. NPG run a `cron` job daily which:
   1. Locates aany batches of data added to the NFS filesystem in the last 7 days.
   2. For each batch of data, splits the output into several files, one per sample.
   3. Publishes these per-sample files to iRODS and annotates them with "primary"
      metadata.
   4. Finds information about the samples in the ML warehouse, further annotates
      them with "secondary" metadata and sets data access permissions.
5. NPG run regular cron jobs which update "secondary" metadata to reflect any
   subsequent changes in the ML warehouse.

## Implementation details

- The code is written in Perl. This code is a predecessor of [npg_irods](https://github.com/wtsi-npg/npg_irods)
  and shares the latter's dependencies on [baton](http://wtsi-npg.github.io/baton/)
  and [perl-irods-wrap](https://github.com/wtsi-npg/perl-irods-wrap) for
  interaction with iRODS.

- The script run daily by `cron` to publish new data is
  [publish_fluidigm_genotypes.pl](https://github.com/wtsi-npg/genotyping/blob/master/src/perl/bin/publish_fluidigm_genotypes.pl).  

- The publishing script is idempotent which, combined with a daily run capturing
  the past 7 days of data, means that temporary services interruptions are handled
  without additional tracking, but with the overhead of some repeated work.
