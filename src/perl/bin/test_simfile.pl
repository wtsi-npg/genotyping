#! /usr/bin/perl

use warnings;
use strict;

use WTSI::Genotyping::QC::SimFiles;


my $fh;
open $fh, "< $ARGV[0]";
my @fields = WTSI::Genotyping::QC::SimFiles::readHeader($fh);
foreach my $field (@fields) { print $field."\n"; }
#my $blockSize = WTSI::Genotyping::QC::SimFiles::findBlockSize(@fields);
print "#####\n";
foreach my $i (0..4) {
    WTSI::Genotyping::QC::SimFiles::readBlock($fh, $i, \@fields);
}
close $fh;
