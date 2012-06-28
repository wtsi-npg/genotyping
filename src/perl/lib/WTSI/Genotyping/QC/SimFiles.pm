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
    # read and unpack .sim format header
    # header fields are: ($magic, $version, $nameLength, $samples, $probes, $channels, $numberType)
    my $fh = shift;
    my $header;
    read($fh, $header, 16); # header = first 16 bytes
    my @fields = unpack("A3CSLLCC", $header);
    return @fields;
}

sub blockSizeFromHeader {
    # input an unpacked .sim header; size = name_length + (probes * channels * numeric_bytes)
    my $numberBytes = numericBytesByFormat($_[6]);
    my $blockSize = $_[2] + ($_[4] * $_[5] * $numberBytes);
    return $blockSize;
}

sub numericBytesByFormat {
    # return number of bytes used for each numeric entry, for .sim format code
    my $format = shift;
    if ($format==0) { return 4; }
    elsif ($format==1) { return 2; }
    else { die "Unknown .sim numeric format code: $format : $!"; }
}

sub readBlock {
    # read block of .sim data from a filehandle; can read only some SNPs for each sample
    # $numericToRead is total numeric entries to read in each record
    # not currently in use! reading entire block for a sample exceeds memory limits, use findMeanXYDiff instead
    my ($fh, $nameLength, $numberType, $blockOffset, $blockSize, $numericToRead) = @_;
    my ($data, @block, @unPackCodes, $numericBytes);
    $numericBytes = numericBytesByFormat($numberType);
    if ($numberType==0) { @unPackCodes = qw(V L f); } 
    elsif ($numberType==1) { @unPackCodes = qw(v); }
    my $start = 16 + $blockOffset*$blockSize;
    seek($fh, $start, 0);
    my $size = $nameLength + $numericToRead * $numericBytes;
    my $dataLength = read($fh, $data, $size);
    if ($dataLength > 0) { # unpack binary data chunk into usable format
	my $format = "a$nameLength $unPackCodes[0]$numericToRead";
	@block = unpack($format, $data);
	my $name = shift(@block);
	for (my $i=0;$i<@block;$i++) { # additional processing to convert from .sim formats to native Perl float
	    if ($numberType==0) { 
		# pack to Perl long int, unpack to Perl float; Perl numeric formats depend on local installation
		# simply unpacking with 'f' *might* work, but this is safer and makes .sim endianness explicit
		$block[$i] = pack($unPackCodes[1], $block[$i]);
		$block[$i] = unpack($unPackCodes[2], $block[$i]);
	    } elsif ($numberType==1) {
		$block[$i] = $block[$i] / 1000; # convert 16-bit integer to float in range 0.0-65.535 inclusive
	    }
	}
	unshift(@block, $name);
    }
    return @block;
}

sub findMeanXYDiff {
    # find mean xydiff metric for a large number of probes (assuming exactly 2 channels)
    # data for all probes may not fit in memory!
    # seek to required location and unpack data for 2 intensities at a time (or a "not too big" chunk?)
    my ($fh, $nameLength, $numberType, $blockOffset, $blockSize, $probes) = @_;
    my ($data, @pair, @unPackCodes, $numericBytes, $xyDiffTotal, $xyDiffCount);
    $numericBytes = numericBytesByFormat($numberType);
    if ($numberType==0) { @unPackCodes = qw(V L f); } 
    elsif ($numberType==1) { @unPackCodes = qw(v); }
    my $sampleStart = 16 + $blockOffset*$blockSize; # 16 = length of header
    for (my $i=0;$i<$probes;$i++) {
	my $start = $sampleStart + $nameLength + 2*$numericBytes*$i;
	seek($fh, $start, 0);
	my $size = 2*$numericBytes;
	my $dataLength = read($fh, $data, $size);
	if ($dataLength==0) { last; }
	my $format = $unPackCodes[0].(2*$numericBytes);
	@pair = unpack($format, $data);
	for (my $j=0;$j<2;$j++) {
	    if ($numberType==0) { 
		# pack to Perl long int, unpack to Perl float; Perl numeric formats depend on local installation
		# simply unpacking with 'f' *might* work, but this is safer and makes .sim endianness explicit
		$pair[$j] = pack($unPackCodes[1], $pair[$j]);
		$pair[$j] = unpack($unPackCodes[2], $pair[$j]);
	    } elsif ($numberType==1) {
		$pair[$j] = $pair[$j] / 1000; # convert 16-bit integer to float in range 0.0-65.535 inclusive
	    }
	}
	$xyDiffTotal+= ($pair[1] - $pair[0]);
	$xyDiffCount++;
    }
    my $xyDiffMean = $xyDiffTotal / $xyDiffCount;
    return $xyDiffMean;
}

sub readName {
    # read name of sample with given $blockOffset
     my ($fh, $nameLength, $blockOffset, $blockSize) = @_;
     my $start = 16 + $blockOffset*$blockSize;
     my $data;
     seek($fh, $start, 0);
     read($fh, $data, $nameLength);
     my @words = unpack("a$nameLength", $data);
     my $name = shift(@words);
     return $name;
}

sub readWriteXYDiffs {
    # find xydiffs for each sample in input; write to output
    # can correctly handle large input files (.sim files can be >> 1G)
    # use the first $useProbes probes, or all probes, whichever is less
    my ($in, $out, $verbose) = @_;
    $verbose ||= 0;
    my @header = readHeader($in);
    if ($header[5]!=2) { die "Must have exactly 2 intensity channels to compute xydiff: $!"; } 
    my $nameLength = $header[2];
    my $probes = $header[4];
    my $channels = $header[5];
    my $numberType = $header[6];
    my $blockSize = blockSizeFromHeader(@header);
    my $i = 0;
    my @samples;
    my @means;
    my $maxBlocks = 50;
    while (!eof($in)) {
	my $name = readName($in, $nameLength, $i, $blockSize);
	my $xydiff = findMeanXYDiff($in, $nameLength, $numberType, $i, $blockSize, $probes);
	$name =~ s/\0//g; # strip off null padding
	push(@samples, $name);
	push(@means, $xydiff);
	$i++;	
	if ($verbose) { print $i."\t".$name."\t".$xydiff."\n"; }
	if (@samples==$maxBlocks || eof($in)) {
	    # print buffers to output filehandle
	    for (my $j=0;$j<@samples;$j++) {
		$name = $samples[$j];
		printf($out "%s\t%.8f\n", ($name, $means[$j]));
	    }
	    if ($verbose) { print $i." samples written.\n"; }
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
    }
    return @diffs;
}


return 1;
