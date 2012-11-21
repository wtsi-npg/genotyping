Genotyping test datasets

Iain Bancarz, ib5@sanger.ac.uk

Artificial test data in PLINK and SIM formats, created for tests of genotyping QC.

See doc/create_test_dataset_howto.org for details of data creation.

1) Dataset Alpha

Plink data:
* Contains 1000 samples and 210 SNPs.
* 100 SNPs are annotated as chromosome 1, and 110 from chromosome 23 (X).  Of the X chromosomes, 10 are from pseudoautosomal regions or PARs; these are annotated as X, but heterozygous on male samples.  The X and PAR SNPs allow gender testing.
* Genotype calls are randomly generated for each sample.  Probability of "no call" events is 5%.  Heterozygosity is 2% for male X sites (excluding PARs) and 25% otherwise.
* 5 duplicates have been deliberately inserted; the first 10 samples are actually 5 duplicate pairs.
* Sample names are in PLATE_WELL_ID format.  An 8 row by 12 column plate configuration was used.

SIM data:
* Same samples and genotype calls as PLINK data above.
* Intensities generated using signal and noise distributions.
* Written in .sim (simple intensity matrix) format, see genotype-call on github
* Generation script was simGenerator.py, in /nfs/users/nfs_i/ib5/mygit/concoct_genotype_data/python/bin/ as of 2012-07-12.

SQLite database:
* alpha_pipeline.db is an appropriate SQLite database containing metadata for the above PLINK and .sim files.  Note that QC test write to a temporary copy of the database, so that the "master" copy in qc_test_data is unaffected.

2) Dataset Beta

Similar to Alpha above, but contains 4950 samples, 3102 of which are missing plate information.  Also has 50 duplicate pairs and 218 excluded samples.  This tests features for larger datasets, eg. breaking metric scatterplots across multiple pages.
