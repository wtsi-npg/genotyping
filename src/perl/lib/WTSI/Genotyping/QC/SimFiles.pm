#!/software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# May 2012

# module to read .sim intensity files
# initially use to find xydiff stats for QC, may have other uses

package WTSI::Genotyping::QC::SimFiles;

use strict;
use warnings;

sub mean {
    # mean of given array -- use to find mean xydiff
    my ($count, $total) = (0,0);
    foreach my $term (@_) { $count += 1; $total += $term; }
    if ($count==0) { return undef; } # avoid dividing by zero
    else { return $total / $count; }
}

sub allMeanXYDiffs {
    # find mean xydiff for each sample in file
    my $fh = shift;
    my @header = readHeader($fh);
    my ($magic, $version, $nameLength, $samples, $probes, $channels, $numberType) = @header;
    if ($channels!=2) { die "Must have exactly 2 intensity channels to compute xydiff: $!"; } 
    my $i = 0;
    my @means;
    while (!eof($fh)) {
	my @block = readBlock($fh, $i, \@header);
	my $sampleMean = mean(xyDiffs(@block));
	push(@means, $sampleMean);
	$i++;	
    }
    return @means;
}

sub readHeader {
    my $fh = shift;
    my $header;
    read($fh, $header, 16); # header = first 16 bytes
    my @fields = unpack("A3CSLLCC", $header);
    return @fields;
}

sub readBlock {
    # read genotyping result for a particular sample
    # TODO check that number format for intensity measurements is correct -- may need conversion subroutine
    my ($fh, $blockOffset, $headerRef) = @_;
    my @header = @$headerRef;
    my ($nameLength, $probeTotal, $channels, $numberFormat, $numberBytes, $data);
    $nameLength = $header[2];
    $probeTotal = $header[4];
    $channels = $header[5];
    # find block size and read data
    if ($header[6]==0) { $numberFormat = "V"; $numberBytes = 4; } # little-endian 32-bit unsigned integer
    elsif ($header[6]==1) { $numberFormat = "v";  $numberBytes = 2; } # little-endian 16-bit unsigned integer
    else { die "Unknown format code in header: $!"; }
    my $blockSize = $nameLength + ($probeTotal * $channels * $numberBytes);
    my $start = 16 + $blockOffset*$blockSize;
    seek($fh, $start, 0);
    my $dataLength = read($fh, $data, $blockSize);
    my @block = ();
    if ($dataLength > 0) { # unpack binary data chunk into usable format
	my $numericEntries = $probeTotal * $channels;
	my $format = "a$nameLength $numberFormat$numericEntries";
	#print "### ".$start." ".$blockSize." ".$dataLength." FORMAT: ".$format."\n";
	@block = unpack($format, $data);
    }
    return @block;
}

sub xyDiffs {
    # find xydiffs for given sample block
    my @block = @_;
    my $name = shift(@block);
    unless (@block % 2 == 0) { die "Cannot compute xydiff on odd number of intensities: $!"; }
    my @diffs;
    while (@block) {
	my ($xint, $yint) = splice(@block, 0, 2);
	push(@diffs, ($yint-$xint));
	#print "$xint\t$yint\t".($yint-$xint)."\n";
    }
    return @diffs;
}


return 1;
