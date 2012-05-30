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

sub readHeader {
    my $fh = shift;
    my $header;
    read($fh, $header, 16); # header = first 16 bytes
    my @fields = unpack("A3CSLLCC", $header);
    return @fields;
}

sub processHeader {
    # calculate useful quantities from header
    my ($magic, $version, $nameLength, $samples, $probes, $channels, $numberType) = @_;
    my ($numberFormat, $numberBytes);
    if ($numberType==0) { $numberFormat = "V"; $numberBytes = 4; } # little-endian 32-bit unsigned integer
    elsif ($numberType==1) { $numberFormat = "v";  $numberBytes = 2; } # little-endian 16-bit unsigned integer
    my $blockSize = $nameLength + ($probes * $channels * $numberBytes);
    my $numericEntries = $probes * $channels;
    return ($numberFormat, $numberBytes, $blockSize, $numericEntries);
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

sub readBlockFast {
    # as for readBlock, but does not parse header, and can read only some SNPs for each sample
    # $numericToRead is total numeric entries to read in each record
    my ($fh, $nameLength, $numberFormat, $numericBytes, $blockOffset, $blockSize, $numericToRead) = @_;
    my $data;
    my $start = 16 + $blockOffset*$blockSize;
    seek($fh, $start, 0);
    my $size = $nameLength + $numericToRead * $numericBytes;
    my $dataLength = read($fh, $data, $size);
    my @block = ();
    if ($dataLength > 0) { # unpack binary data chunk into usable format
	my $format = "a$nameLength $numberFormat$numericToRead";
	@block = unpack($format, $data);
    }
    return @block;
}

sub readWriteXYDiffs {
    # find xydiffs for each sample in input; write to output
    # can correctly handle large input files (.sim files can be >> 1G)
    my ($in, $out, $useProbes, $verbose) = @_;
    $verbose ||= 1;
    my @header = readHeader($in);
    my ($magic, $version, $nameLength, $samples, $probes, $channels, $numberType) = @header;
    if ($channels!=2) { die "Must have exactly 2 intensity channels to compute xydiff: $!"; } 
    if (not $useProbes || $useProbes > $probes) { $useProbes = $probes; } # number of probes to use for xydiff
    my $numericToRead = $useProbes * $channels;
    my ($numberFormat, $numberBytes, $blockSize, $numericEntries) = processHeader(@header);
    my $i = 0;
    my @samples;
    my @means;
    my $maxBlocks = 50;
    while (!eof($in)) {
	#my @block = readBlock($in, $i, \@header);
	my @block = readBlockFast($in, $nameLength, $numberFormat, $numberBytes, $i, $blockSize, $numericToRead);
	my $name = $block[0];
	$name =~ s/\0//g; # strip off null padding
	push(@samples, $name);
	my $sampleMean = mean(xyDiffs(@block));
	push(@means, $sampleMean);
	$i++;	
	if (@samples==$maxBlocks || eof($in)) {
	    # print buffers to output filehandle
	    for (my $j=0;$j<@samples;$j++) {
		$name = $samples[$j];
		unless ($name) { last; } # avoid processing empty line at end-of-file
		if ($verbose) { print $name."\n"; }
		print $out "$name\t$means[$j]\n";
	    }
	    if ($verbose) { print $i." samples read.\n"; }
	    @samples = ();
	    @means = ();
	}
    }
    return $i;
}

sub sampleMeanXYDiffs {
    # find mean xydiff for each sample in file
    my $fh = shift;
    my @header = readHeader($fh);
    my ($magic, $version, $nameLength, $samples, $probes, $channels, $numberType) = @header;
    if ($channels!=2) { die "Must have exactly 2 intensity channels to compute xydiff: $!"; } 
    my $i = 0;
    my @samples;
    my @means;
    while (!eof($fh)) {
	my @block = readBlock($fh, $i, \@header);
	push(@samples, $block[0]);
	my $sampleMean = mean(xyDiffs(@block));
	push(@means, $sampleMean);
	$i++;	
    }
    return (\@samples, \@means);
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
