
# Tests generate_yml.pl

use utf8;
use strict;
use warnings;

use WTSI::NPG::Genotyping::QC::IdentityTest;

# Created a cut-down PLINK dataset (20 SNPs, 5 samples)
# see gapi/genotype_identity_test.git on http://git.internal.sanger.ac.uk
# data contains some "real" samples and calls, so not made public on github

Test::Class->runtests;
