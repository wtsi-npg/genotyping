# Author:  Iain Bancarz, ib5@sanger.ac.uk
# September 2012

use strict;
use warnings;
use Carp;
use Getopt::Long;
use FindBin qw($Bin);
use WTSI::Genotyping::QC::QCPlotShared qw(defaultJsonConfig 
 getPlateLocationsFromPath readMetricResultHash readQCMetricInputs
 $INI_FILE_DEFAULT);
use Exporter;

our @ISA = qw/Exporter/;
our @EXPORT_OK = qw/run/;

sub getCsvLine {
    # get CSV line with stats for given plate
    # fields: index, plate, total samples, and
    #  (pass/fail/percent) for current, other, and all metrics
    my ($index, $plate, $countsRef, $total) = @_;
    my @passCounts = @$countsRef;
    my @fields = ($index+1, $plate, $total); # count from 1, not 0
    for (my $i=0;$i<@passCounts;$i++) {
        my $failCount = $total - $passCounts[$i];
        my $passPercent = sprintf("%.2f", ($passCounts[$i]/$total)*100);
        push(@fields, ($passCounts[$i], $failCount, $passPercent));
    }
    my $line = join(",", @fields)."\n";
    return $line;
}

sub getMetricIndex {
    # which field in text input contains metric values?
    my $metric = shift;
    my %indices = (
        call_rate      => 1,
        gender         => 1,
        heterozygosity => 2,
        identity       => 3,
        magnitude      => 1,
        );
    return $indices{$metric};
}

sub getOutputFiles {
    # get output filehandles for plate data
    my ($outDir, $metric, $batchNum) = @_;
    my $index = sprintf("%03d", $batchNum);
    my $outPath = $outDir."/scatter_".$metric."_".$index.".txt";
    my $pbPath = $outDir."/plate_boundaries_".$metric."_".$index.".txt";
    my $pnPath = $outDir."/plate_names_".$metric."_".$index.".txt";
    my $csvPath = $outDir."/plate_stats_".$metric."_".$index.".csv";
    open my $out, ">", $outPath || croak "Cannot open output \"$outPath\": $!";
    open my $pb, ">", $pbPath || croak "Cannot open output \"$pbPath\": $!";
    open my $pn, ">", $pnPath || croak "Cannot open output \"$pnPath\": $!";
    open my $csv, ">", $csvPath || croak "Cannot open output \"$csvPath\": $!";
    # print headers
    print $pb "Start\tEnd\n"; # used in R script, allows empty boundary file
    my @csvHeaders = ("Index", "Plate", "Total", "P", "F",
                      "%", "OthP", "OthF", "Oth%", "AllP", "AllF", "All%");
    print $csv join(",", @csvHeaders)."\n";
    return ($out, $pb, $pn, $csv);
}

sub getPassStatus {
    # find pass/fail status wrt current metric, all other metrics, and overall
    # current/other fails may overlap, so overall is not simply current+other
    my ($metric, $resultPath, $configPath) = @_;
    my %allStatus;
    my %results = readMetricResultHash($resultPath, $configPath);
    foreach my $sample (keys(%results)) {
        my @status = (1,1,1); # current; others; all
        my %resultsByMetric = %{$results{$sample}};
        foreach my $key (keys(%resultsByMetric)) {
            my ($pass, $value) = @{$resultsByMetric{$key}};
            if (!$pass) {
                $status[2] = 0;
                if ($key eq $metric) { $status[0] = 0; }
                else { $status[1] = 0; }
            }
        }
        $allStatus{$sample} = \@status; 
    }
    return %allStatus;
}

sub plateLabel {
    # label each plate with plate count and (possibly truncated) plate name
    # ensures meaningful representation of very long plate names
    my ($plate, $i, $maxLen) = @_;
    $maxLen ||= 20;
    my $num = sprintf("%03d", $i);
    my @chars = split(//, $plate);
    my @head = splice(@chars, 0, $maxLen);
    my $name = join('', @head);
    return $num.":".$name;
}

sub readMetric {
    # read metric values from QC output file
    # return hash indexed by plate
    my ($metricPath, $metricIndex, $locsRef, $skipFirst) = @_;
    $skipFirst ||= 0;
    my %plateLocations = %$locsRef;
    my %inputs;
    open my $in, "<", $metricPath || 
        croak "Cannot open input \"$metricPath\": $!";
    my $first = 1;
    while (<$in>) {
        if (/^#/) { next; }
        elsif ($skipFirst && $first) { $first = 0; next; }
        chomp;
        my @words = split;
        my $sample = $words[0];
        my $plateRef = $plateLocations{$sample};
        my ($plate, $label) = @$plateRef;
        $plate ||= "UNKNOWN";
        my $metric = $words[$metricIndex];
        $inputs{$plate}{$sample} = $metric;
    }
    close $in || croak "Cannot close input path $metricPath: $!";
    return %inputs;
}

sub resetOutputs {
    # convenience method for writePlotInputs
    my ($outDir, $metric, $batchNum, $out, $pb, $pn, $csv) = @_;
    foreach my $fh ($out, $pb, $pn, $csv) {
        close $fh || croak "Cannot close output: $!";
    }
    ($out, $pb, $pn, $csv) = getOutputFiles($outDir, $metric, $batchNum);
    return ($out, $pb, $pn, $csv);
}

sub runPlotScript {
    # run R script to produce plot
    my ($metric, $plotText, $plotPng) = @_;
}

sub writePlotInputs {
    # create input for R plotting script
    # outputs:
    # * Sample count, metric value, and pass/fail status for each sample
    # * Plate boundaries (even-numbered plates only, for plot shading)
    # * Plate names and midpoints, for plot labels
    # * CSV file containing pass/fail stats for each plate
    # for large number of samples, split plates into multiple files
    my ($dbPath, $iniPath, $metric, $metricPath, $metricIndex, $outDir,
        $passRef, $maxBatchSize) = @_;
    my %plateLocations = getPlateLocationsFromPath($dbPath, $iniPath);
    my $skipFirst;
    if ($metric eq 'gender') { $skipFirst = 1; }
    else { $skipFirst = 0; }
    my %inputs = readMetric($metricPath, $metricIndex, \%plateLocations,
                            $skipFirst);
    my @plates = sort(keys(%inputs));
    # each plate goes into buffer; check file size before plate output
    # if file too big, close current file and open next before buffer output
    my ($batchNum, $batchSize, $plateStart, $writeStartFinish, $i) = 
        (0,0,0,0,0);
    my @plateLines = ();
    my ($out, $pb, $pn, $csv) = getOutputFiles($outDir, $metric, $batchNum);
    my %passStatus = %$passRef;
    my @passCounts = (0,0,0);
    foreach my $plate (@plates) {
        my %metricBySample = %{$inputs{$plate}};
        my @samples = sort(keys(%metricBySample));
        foreach my $sample (@samples) {
            my @status = @{$passStatus{$sample}};
            for (my $j=0;$j<@passCounts;$j++) {
                if ($status[$j]) { $passCounts[$j]++; }
            }
            my $line=$metricBySample{$sample}."\t".$status[1]."\n";
            push(@plateLines, $line);
        }
        if (@plateLines > $maxBatchSize) {
            carp "WARNING: Plate exceeds maximum output batch size; ".\
                "omitting batch division; plots may not render correctly.";
        } elsif ($batchSize + @plateLines > $maxBatchSize) {
            # close current files, open next ones, reset counters/flags
            $batchNum++;
            ($batchSize, $plateStart, $writeStartFinish) = (0,0,0);
            ($out, $pb, $pn, $csv) = resetOutputs($outDir, $metric, $batchNum,
                                                  $out, $pb, $pn, $csv);
        }
        for (my $j=0;$j<@plateLines;$j++) { # prepend sample count to line
            print $out ($batchSize+$j)."\t".$plateLines[$j];
        }
        my $total = @samples;
        print $csv getCsvLine($i, $plate, \@passCounts, $total); 
        @passCounts = (0,0,0);
        $batchSize += @plateLines;
        my $plateFinish = $plateStart + @plateLines;
        my $midPoint = $plateStart + (@plateLines/2);
        print $pn plateLabel($plate, $i+1)."\t".$midPoint."\n"; 
        @plateLines = ();
        if ($writeStartFinish) { # write for even-numbered plates only
            print $pb $plateStart."\t".$plateFinish."\n";
            $writeStartFinish = 0;
        } else {
            $writeStartFinish = 1;
        }
        $plateStart = $plateFinish+1;
        $i++;
    }
    foreach my $fh ($out, $pb, $pn, $csv) {
        close $fh || croak "Cannot close output: $!";
    }
}

sub run {
    my ($metric, $qcDir, $outDir, $config, $dbpath, $inipath, $maxBatch) = @_;
    $maxBatch ||= 2000; # was 480
    my %inputs = readQCMetricInputs($config);
    my $inputName = $inputs{$metric};
    if (!$inputName) { croak "No input name in $config for $metric: $!"; }
    my $metricPath = $qcDir."/".$inputName;
    my $metricIndex = getMetricIndex($metric);
    my %passStatus = getPassStatus($metric, $qcDir."/qc_results.json", $config);
    writePlotInputs($dbpath, $inipath, $metric, $metricPath, $metricIndex, 
                    $outDir, \%passStatus, $maxBatch);
    #runPlotScript($metric, $plotText, $plotPng);
}
