# Author:  Iain Bancarz, ib5@sanger.ac.uk
# May 2012

# module to read .sim intensity files
# initially use to find xydiff stats for QC, may have other uses

package WTSI::Genotyping::QC::SimFiles;

use strict;
use warnings;
use Carp;
use bytes;
use POSIX;

sub blockSizeFromHeader {
    # input an unpacked .sim header; size = name_length + (probes * channels * numeric_bytes)
    my $numberBytes = numericBytesByFormat($_[6]);
    my $blockSize = $_[2] + ($_[4] * $_[5] * $numberBytes);
    return $blockSize;
}

sub extractSampleRange {
    # extract given range of samples from .sim file and output to given filehandle
    # useful for creating small test datasets
    my ($in, $out, $startIndex, $endIndex) = @_;
    my $header = readHeader($in); 
    print $out $header;
    my @header = unpackHeader($header);
    my $blockSize = blockSizeFromHeader(@header);
    for (my $i=$startIndex;$i<$endIndex;$i++) {
	my $data = readSampleBinary($in, $i, $blockSize);
	print $out $data;
    }
}

sub findMeanXYDiff {
    # find mean xydiff metric for a large number of probes (assuming exactly 2 channels)
    # data for all probes may not fit in memory, and reading one probe at a time is too slow!
    # compromise; seek to required location and unpack data for groups of probes (intensity pairs)
    my ($fh, $nameLength, $numberType, $sampleOffset, $sampleBlockSize, $probes, $groupNum) = @_;
    my ($data, $xyDiffTotal, $xyDiffCount);
    my $numericBytes = numericBytesByFormat($numberType);
    my $start = 16 + $sampleOffset*$sampleBlockSize + $nameLength; # 16 = length of header
    if ($groupNum > $probes) { $groupNum = $probes; } #  $groupNum = number of probes in group
    my $groups = ceil($probes/$groupNum); # total number of groups
    my $readProbes = $groupNum; # number of probes to read in at one time
    my $groupSize = 2*$groupNum*$numericBytes; # generic size of groups (applies to all but last one)
    for (my $i=0;$i<$groups;$i++) {
	if ($i>0 && $i+1==$groups) { $readProbes = $probes % $groupNum; } # update number of probes for final group
	my $size = 2*$readProbes*$numericBytes;
	my $groupStart = $start + ($i*$groupSize);
	seek($fh, $groupStart, 0); 
	my $dataLength = read($fh, $data, $size);
	my @signals = unpackSignals($data, $numericBytes, $numberType);
	while (@signals) {
	    my @pair = splice(@signals, 0, 2);
	    $xyDiffTotal+= ($pair[1] - $pair[0]);
	    $xyDiffCount++;
	}
    }
    my $xyDiffMean = $xyDiffTotal / $xyDiffCount;
    return $xyDiffMean;
}

sub numericBytesByFormat {
    # return number of bytes used for each numeric entry, for .sim format code
    my $format = shift;
    if ($format==0) { return 4; }
    elsif ($format==1) { return 2; }
    else { croak "Unknown .sim numeric format code: $format : $!"; }
}

sub readBlock {
    # read given data block from .sim filehandle
    my ($fh, $nameLength, $numberType, $blockSize, $blockOffset, $numericToRead) = @_;
    my $name = readName($fh, $nameLength, $blockOffset, $blockSize);
    my $start = 16 + $blockSize* $blockOffset + $nameLength; # start of numeric data
    seek($fh, $start, 0);
    my $binary;
    my $numericBytes = numericBytesByFormat($numberType);
    read($fh, $binary, $numericBytes * $numericToRead);
    my @block = unpackSignals($binary, $numericBytes, $numberType);
    unshift(@block, $name);
    return @block;
}

sub readHeader {
    # read .sim format header with no unpack
    # header fields are: ($magic, $version, $nameLength, $samples, $probes, $channels, $numberType)
    my $fh = shift;
    seek($fh, 0, 0);
    my $header;
    read($fh, $header, 16); # header = first 16 bytes
    return $header;
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

sub readSampleBinary {
    # read all binary data for sample with given $blockOffset (do not unpack)
     my ($fh, $blockOffset, $blockSize) = @_;
     my $start = 16 + $blockOffset*$blockSize;
     my $data;
     seek($fh, $start, 0);
     read($fh, $data, $blockSize);
     return $data;
}

sub readWriteXYDiffs {
    # find xydiffs for each sample in input; write to output
    # can correctly handle large input files (.sim files can be >> 1G)
    # use the first $useProbes probes, or all probes, whichever is less
    my ($in, $out, $verbose, $groupNum) = @_;
    $verbose ||= 0;
    my @header = unpackHeader(readHeader($in));
    if ($header[5]!=2) { croak "Must have exactly 2 intensity channels to compute xydiff: $!"; } 
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
	my $xydiff = findMeanXYDiff($in, $nameLength, $numberType, $i, $blockSize, $probes, $groupNum);
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

sub unpackHeader {
    my $header = shift;
    my @fields = unpack("A3CSLLCC", $header);
    return @fields;
}

sub unpackSignals {
    # unpack a chunk of binary data into signal values
    my ($data, $numericBytes, $numberType) = @_;
    my $dataBytes = bytes::length($data);
    if ($dataBytes % $numericBytes !=0) { croak "Incorrect number of bytes in signal data chunk: $!"; }
    my $signals = $dataBytes/$numericBytes; # how many signal vlaues?
    my @signals;
    if ($numberType==0) { 
	@signals = unpack("V$signals", $data);
	my $repacked = pack("L$signals", @signals);
	@signals = unpack("f$signals", $repacked);
    } elsif ($numberType==1) {
	@signals = unpack("v$signals", $data);
	for (my $i=0;$i<$signals;$i++) { $signals[$i] = $signals[$i] / 1000; }
    }
    return @signals;
}


return 1;
