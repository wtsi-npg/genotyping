#! /software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# May 2012

# convenience script to look at the header and first few entries of a .sim file

use warnings;
use strict;
use WTSI::NPG::Genotyping::QC::SimFiles;

my $fh;
open $fh, "<", $ARGV[0];
my $header = WTSI::NPG::Genotyping::QC::SimFiles::readHeader($fh);
my @fields = WTSI::NPG::Genotyping::QC::SimFiles::unpackHeader($header);
foreach my $field (@fields) { print $field."\n"; }
print "#####\n";
my $blockSize =  WTSI::NPG::Genotyping::QC::SimFiles::blockSizeFromHeader(@fields);
my ($magic, $version, $nameLength, $samples, $probes, $channels, $numberType) = @fields;
my $numericToRead = $probes * $channels;
if ($numericToRead > 20) { $numericToRead = 20; }
foreach my $i (0..4) {
    my @items = WTSI::NPG::Genotyping::QC::SimFiles::readBlock($fh, $nameLength, $numberType,
                                                               $i, $blockSize, $numericToRead);
    print join(',', @items)."\n";
}
close $fh;
