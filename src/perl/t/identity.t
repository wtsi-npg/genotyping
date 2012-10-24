# Author:  Iain Bancarz, ib5@sanger.ac.uk
# October 2012

# test identity check against Sequenom results

use strict;
use warnings;
use Carp;
use Test::More tests => 4;
use WrapDBI;
use WTSI::Genotyping::QC::SnpID qw/illuminaToSequenomSNP 
  sequenomToIlluminaSNP/;

print "\tTranslation between Sequenom and Illumina SNP naming conventions:\n";
my $id = 'exm-rs1234';
is(illuminaToSequenomSNP($id), 'rs1234', 'Illumina to Sequenom action');
is(sequenomToIlluminaSNP($id), 'exm-rs1234', 'Sequenom to Illumina no action');
$id = 'rs5678';
is(illuminaToSequenomSNP($id), 'rs5678', 'Illumina to Sequenom no action');
is(sequenomToIlluminaSNP($id), 'exm-rs5678', 'Sequenom to Illumina action');


