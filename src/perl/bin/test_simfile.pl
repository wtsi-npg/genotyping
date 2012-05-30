#! /usr/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# May 2012

# simple test script for SimFiles module

use warnings;
use strict;
use WTSI::Genotyping::QC::SimFiles;

my $fh;
open $fh, "< $ARGV[0]";
my @fields = WTSI::Genotyping::QC::SimFiles::readHeader($fh);
foreach my $field (@fields) { print $field."\n"; }
print "#####\n";
foreach my $i (0..4) {
    my @items = WTSI::Genotyping::QC::SimFiles::readBlock($fh, $i, \@fields);
    print join(' # ', @items)."\n";
}
close $fh;
