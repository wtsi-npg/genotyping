# Author:  Iain Bancarz, ib5@sanger.ac.uk
# May 2012

# module to read .sim intensity files
# use to find magnitude & xydiff metrics for QC

package WTSI::Genotyping::QC::SimFiles;

use strict;
use warnings;
use Carp;
use bytes;
use POSIX qw(ceil ctime);
use Exporter;

our @ISA = qw/Exporter/;
our @EXPORT_OK = qw/headerParams readSampleNames writeIntensityMetrics/;
our $HEADER_LENGTH = 16;

sub computeMetric {
    # find (un-normalised) magnitude or xydiff, assuming 2 channels
    # list of intensities composed of (x,y) pairs
    my @intensities = @{ shift() };
    my $metric = shift;
    my $i = 0;
    my @values;
    while ($i < @intensities) {
        my $val;
        if ($metric eq 'magnitude') {
            $val = sqrt($intensities[$i]**2 + $intensities[$i+1]**2);
        } elsif ($metric eq 'xydiff') {
            $val = $intensities[$i+1] - $intensities[$i];
        } else {
            croak("Unknown metric name: \"$metric\"");
        }
        push(@values, $val);
        $i += 2;
    }
    return @values;
}

sub extractSampleRange {
    # extract given range of samples from .sim file, and output
    # useful for creating small test datasets
    my ($in, $out, $startIndex, $endIndex) = @_;
    my $header = readHeader($in); 
    print $out $header;
    my @header = unpackHeader($header);
    my $blockSize = blockSizeFromHeader(@header);
    for (my $i=$startIndex;$i<$endIndex;$i++) {
        my $start = $HEADER_LENGTH + $i*$blockSize;
        my $data;
        seek($in, $start, 0);
        read($in, $data, $blockSize);
        print $out $data;
    }
    return 1;
}

sub findMetrics {
    # find mean xydiff and normalised magnitude for each sample
    # read blocks of probes; find mean magnitude for each probe to normalize
    # update running totals by sample for each block
    my $in = shift;
    my $log = shift;
    select $log; $|++; # flush log immediately for each output
    my %params = %{ shift()};
    my $probesInBlock = shift;
    $probesInBlock ||= 1000;
    my $probes = $params{'probes'};
    my $blocks = ceil($probes / $probesInBlock);
    my @names = readSampleNames($in, \%params);
    my (%magTotals, %xyTotals);
    my $i = 0; # probe offset
    my $block = 0; # block count
    while ($i < $probes) {
        if ($probesInBlock > $probes - $i) {
            $probesInBlock = $probes - $i; # reduce size for final block
        }
        my @args = ($in, $i, $probesInBlock, \%params);
        my ($magRef, $xyRef) = metricTotalsForProbeBlock(@args);
        my @mags = @{$magRef};
        my @xy = @{$xyRef};
        for (my $j=0;$j<@names;$j++) {
            $magTotals{$names[$j]} += $mags[$j];
            $xyTotals{$names[$j]} += $xy[$j];
        }
        $i += $probesInBlock;
        $block++;
        if ($block % 10 == 0 || $i == $probes) {
            my $timeStamp = ctime(time()); # ends with \n
            my $msg = "Metrics found for block $block of $blocks, ".
                "probe $i of $probes: $timeStamp";
            print $log $msg;
        }
    }
    foreach my $name (@names) { # find mean values across probes
        $magTotals{$name} = $magTotals{$name} / $probes;
        $xyTotals{$name} = $xyTotals{$name} / $probes;
    }
    return (\%magTotals, \%xyTotals);
}

sub headerParams {
    # read/compute .sim file parameters form header
    my $in = shift;
    my @header = unpackHeader(readHeader($in));
    my ($magic, $version, $nameLength, $samples, $probes, $channels, 
        $numberType) = @header;
    my %params = (
        'magic' => $magic,
        'version' => $version,
        'name_bytes' => $nameLength,
        'samples' => $samples,
        'probes' => $probes,
        'channels' => $channels,
        'number_type' => $numberType,
        );
    my $numericBytes = numericBytesByFormat($numberType);
    my $sampleUnitBytes = $nameLength + ($probes * $channels * $numericBytes);
    $params{'numeric_bytes'} = $numericBytes;
    $params{'sample_unit_bytes'} = $sampleUnitBytes;
    return %params;
}

sub metricTotalsForProbeBlock {
    # find total (normalised) magnitude and xydiff 
    # for all samples, for given range of probes
    my ($in, $probeStart, $probeTotal, $paramsRef) = @_;
    my %params = %{$paramsRef};
    my $samples = $params{'samples'};
    my (@magsByProbe, @magTable, @magTotalsBySample, @xyTotals);
    for (my $i=0;$i<$samples;$i++) { # foreach sample
        my @intensities = readProbeRange($in, $i, $probeStart, 
                                         $probeTotal, $paramsRef);
        # update magnitude totals
        my @mag = computeMetric(\@intensities, 'magnitude');
        for (my $j=0;$j<@mag;$j++) { # foreach probe
            $magsByProbe[$j] += $mag[$j];
        }
        push(@magTable, \@mag);
        # update xydiff totals
        my @xy = computeMetric(\@intensities, 'xydiff');
        for (my $j=0;$j<@mag;$j++) { # foreach probe
            $xyTotals[$i] += $xy[$j];
        }
    }
    # find mean magnitude by probe
    for (my $i=0; $i<$probeTotal; $i++) {
        $magsByProbe[$i] = $magsByProbe[$i] / $samples;
    }
    # find normalized mag totals by sample
    for (my $i=0;$i<$samples;$i++) { # foreach sample
        for (my $j=0;$j<$probeTotal;$j++) { # foreach probe
            $magTotalsBySample[$i] += $magTable[$i][$j] / $magsByProbe[$j];
        }
    }
    return (\@magTotalsBySample, \@xyTotals);
}

sub numericBytesByFormat {
    # return number of bytes used for each numeric entry, for .sim format code
    my $format = shift;
    if ($format==0) { return 4; }
    elsif ($format==1) { return 2; }
    else { croak "Unknown .sim numeric format code: $format : $!"; }
}

sub readHeader {
    # read .sim format header with no unpack
    # header fields: 
    # ($magic, $version, $nameLength, $samples, $probes, $channels, $numberType)
    my $fh = shift;
    seek($fh, 0, 0);
    my $header;
    read($fh, $header, $HEADER_LENGTH); # header = first 16 bytes
    return $header;
}

sub readName {
    # read name of sample with given $blockOffset
     my ($fh, $nameLength, $blockOffset, $blockSize) = @_;
     my $start = $HEADER_LENGTH + $blockOffset*$blockSize;
     my $data;
     seek($fh, $start, 0);
     read($fh, $data, $nameLength);
     my @words = unpack("a$nameLength", $data);
     my $name = shift(@words);
     return $name;
}

sub readProbeRange {
    # read intensities for given sample and range of probes
    my ($fh, $sampleOffset, $probeStart, $probeTotal, $paramsRef) = @_;
    my %params = %{$paramsRef};
    my $channels = $params{'channels'};
    my $probeBytes = $params{'numeric_bytes'} * $channels;
    my $start = $HEADER_LENGTH + ($sampleOffset*$params{'sample_unit_bytes'}) 
        + $params{'name_bytes'} + ($probeBytes*$probeStart);
    seek($fh, $start, 0);
    my $binary;
    read($fh, $binary, $probeBytes*$probeTotal);
    my @block = unpackSignals($binary, $params{'numeric_bytes'}, 
                              $params{'number_type'});
    return @block;
}

sub readSampleNames {
    # read names of all samples in order
    my $in = shift;
    my %params = %{ shift() };
    my @names;
    for (my $i=0;$i<$params{'samples'};$i++) {
        my $name = readName($in, $params{'name_bytes'}, $i, 
                            $params{'sample_unit_bytes'});
        $name =~ s/\0//g; # strip off null padding
        push(@names, $name);
    }
    return @names;
}

sub unpackHeader {
    my $header = shift;
    my @fields = unpack("A3CSLLCC", $header);
    return @fields;
}

sub unpackSignals {
    # unpack a chunk of binary data into signal values
    # TODO replace this with C for better speed??
    my ($data, $numericBytes, $numberType) = @_;
    my $dataBytes = bytes::length($data);
    if ($dataBytes % $numericBytes !=0) { 
        croak "Incorrect number of bytes in signal data chunk: $!"; 
    }
    my $signals = $dataBytes/$numericBytes; # how many signal values?
    my @signals;
    if ($numberType==0) { 
        # unpack/repack circumvents horrible Perl ambiguities in number format
        @signals = unpack("V$signals", $data);
        my $repacked = pack("L$signals", @signals);
        @signals = unpack("f$signals", $repacked);
    } elsif ($numberType==1) {
        @signals = unpack("v$signals", $data);
        for (my $i=0;$i<$signals;$i++) { $signals[$i] = $signals[$i] / 1000; }
    }
    return @signals;
}

sub writeIntensityMetrics {
    # find xydiff and normalised magnitude, and write to file
    # arguments: paths for input/output; if no input path, use STDIN
    my ($inPath, $outPathMag, $outPathXY, $logPath, $probesInBlock) = @_;
    my $in;
    if ($inPath) { open $in, "<", $inPath; }
    else { $in = \*STDIN; $inPath = "STDIN"; }
    $logPath ||= "intensity_metrics.log";
    open my $log, ">", $logPath;
    print $log "Started: ".
        ctime(time())."Input: $inPath\n";
    my %params = headerParams($in);
    my ($magRef, $xyRef) = findMetrics($in, $log, \%params, $probesInBlock);
    close $in || croak("Cannot close filehandle!");
    my %mag = %{$magRef};
    my %xy = %{$xyRef};
    my @samples = sort(keys(%mag));
    print $log "Opening output files: $outPathMag $outPathXY\n";
    open my $outMag, ">", $outPathMag;
    open my $outXY, ">", $outPathXY;
    foreach my $sample (@samples) {
        printf($outMag "%s\t%.8f\n", ($sample, $mag{$sample})); 
        printf($outXY "%s\t%.8f\n", ($sample, $xy{$sample})); 
    }
    print $log "Finished: ".ctime(time());
    foreach my $fh ($log, $outMag, $outXY) {
        close $fh || croak("Cannot close filehandle!");
    }
    return 1;
}


1;
