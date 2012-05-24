#!/software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# module to read .sim intensity files
# initially use to find xydiff stats for QC, may have other uses

package WTSI::Genotyping::QC::SimFiles;

use strict;
use warnings;

sub readHeader {
    my $fh = shift;
    my $header;
    read($fh, $header, 16); # header = first 16 bytes
    my @fields = unpack("A3CSLLCC", $header);
    return @fields;
}

sub findBlockSize {
    # find size of sample record block for given headers
    my @header = @_;
    my ($nameLength, $probeTotal, $channels);
    $nameLength = $header[2];
    $probeTotal = $header[4];
    $channels = $header[5];
    my $blockSize = $nameLength + ($probeTotal * $channels);
    print "### $nameLength, $probeTotal, $channels\n";
    return $blockSize;
}

sub readBlock {
    # read genotyping result for a particular sample
    my ($fh, $blockOffset, $headerRef) = @_;
    my @header = @$headerRef;
    my ($nameLength, $probeTotal, $channels, $numberFormat, $numberBytes, $data);
    $nameLength = $header[2];
    $probeTotal = $header[4];
    $channels = $header[5];
    if ($header[6]==0) { $numberFormat = "d"; $numberBytes = 4; }
    elsif ($header[6]==1) { $numberFormat = "f";  $numberBytes = 2; }
    else { die "Unknown format code in header: $!"; }
    my $blockSize = $nameLength + ($probeTotal * $channels * $numberBytes);
    #print $blockSize."\n";
    my $start = 16 + $blockOffset*$blockSize;
    my $dataLength = read($fh, $data, $blockSize, $start);
    #print $dataLength."\n";
    my $numericEntries = $probeTotal * $channels;
    my $format = "A$nameLength$numberFormat$numericEntries";
    #print $format."\n";
    #print $data."\n";
    #print length($data)."\n";
    my @stuff = unpack($format, $data);
    print join(' # ', @stuff)."\n";
}

return 1;
