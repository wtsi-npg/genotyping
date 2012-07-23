#! /usr/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# May 2012

# convenience script to look at the header and first few entries of a .sim file

use warnings;
use strict;
use WTSI::Genotyping::QC::SimFiles;

my $fh;
open $fh, "< $ARGV[0]";
my @fields = WTSI::Genotyping::QC::SimFiles::readHeader($fh);
foreach my $field (@fields) { print $field."\n"; }
print "#####\n";
my $blockSize =  WTSI::Genotyping::QC::SimFiles::blockSizeFromHeader(@fields);
my ($magic, $version, $nameLength, $samples, $probes, $channels, $numberType) = @fields;
my $numericToRead = $probes * $channels;
if ($numericToRead > 20) { $numericToRead = 20; }
foreach my $i (0..4) {
    my @items = WTSI::Genotyping::QC::SimFiles::readBlock($fh, $nameLength, $numberType, $i, $blockSize, 
							  $numericToRead);
    print join(',', @items)."\n";
}
close $fh;
