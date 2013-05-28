# Author:  Iain Bancarz, ib5@sanger.ac.uk
# September 2012

use strict;
use warnings;
use Carp;
use Getopt::Long;
use File::Temp qw(tempdir);
use POSIX qw(ceil);
use WTSI::NPG::Genotyping::QC::QCPlotShared qw(defaultJsonConfig 
 getPlateLocationsFromPath meanSd readMetricResultHash readQCMetricInputs
 readThresholds plateLabel $INI_FILE_DEFAULT);
use WTSI::NPG::Genotyping::QC::QCPlotTests qw(wrapCommand wrapPlotCommand);
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

sub getOutputFiles {
    # get output filehandles for plate data
    my ($outDir, $metric, $batchNum) = @_;
    my ($outPath, $pbPath, $pnPath) = getOutputPaths($outDir, $metric, 
                                                     $batchNum);
    open my $out, ">", $outPath || croak "Cannot open output \"$outPath\": $!";
    open my $pb, ">", $pbPath || croak "Cannot open output \"$pbPath\": $!";
    open my $pn, ">", $pnPath || croak "Cannot open output \"$pnPath\": $!";
    # print headers
    print $pb "Start\tEnd\n"; # used in R script, allows empty boundary file
    #my @csvHeaders = ("Index", "Plate", "Total", "P", "F",
    #                  "%", "OthP", "OthF", "Oth%", "AllP", "AllF", "All%");
    #print $csv join(",", @csvHeaders)."\n";
    return ($out, $pb, $pn);
}

sub getOutputPaths {
    # used by getOutputFiles and runPlotScript
    my ($outDir, $metric, $batchNum) = @_;
    my $index = sprintf("%03d", $batchNum);
    my $outPath = $outDir."/scatter_".$metric."_".$index.".txt";
    my $pbPath = $outDir."/plate_boundaries_".$metric."_".$index.".txt";
    my $pnPath = $outDir."/plate_names_".$metric."_".$index.".txt";
    return ($outPath, $pbPath, $pnPath);
}

sub getSamplesByPlate {
    # return hash of (sorted) lists of sample names for each plate
    my $dbPath = shift;
    my $iniPath = shift;
    my %plateLocs = getPlateLocationsFromPath($dbPath, $iniPath);
    my %samplesByPlate;
    foreach my $sample (keys(%plateLocs)) {
        my ($plate, $label) = @{$plateLocs{$sample}};
        my $samplesRef = $samplesByPlate{$plate};
        my @samples;
        if ($samplesRef) { @samples = @{$samplesRef}; }
        else { @samples = (); }
        push(@samples, $sample);
        $samplesByPlate{$plate} = \@samples;
    }
    foreach my $plate (keys(%samplesByPlate)) { # sort the sample lists
        my @samples = sort(@{$samplesByPlate{$plate}});
        $samplesByPlate{$plate} = \@samples;
    }
    return %samplesByPlate;
}

sub metricMeanSd {
    # return mean and s.d. of metric, or empty list if all values are "NA"
    my ($metric, $resultPath, $configPath) = @_;
    my %allResults = readMetricResultHash($resultPath, $configPath);
    my @values = ();
    foreach my $sample (keys(%allResults)) {
        my %results = %{$allResults{$sample}};
        if (!defined($results{$metric})) { next; } # excluded sample
        my @metricResults = @{$results{$metric}};
        my $value = $metricResults[1];
        if ($value eq "NA") { next; }
        else { push(@values, $value); }
    }
    if (@values == 0) { return @values; }
    else { return meanSd(@values); }
}

sub metricStatus {
    # did sample pass wrt all metrics other than the target?
    # code to record pass/fail status wrt this and other metrics
    # 0 = passed all; 1 = failed current only; 2 = failed current and at least one other; 3 = passed current, failed at least one other 
    my ($resultsRef, $target) = @_;
    my %results = %$resultsRef;
    my $failedOther = 0;
    my $failedTarget = 0;
    foreach my $metric (keys(%results)) {
        my @fields = @{$results{$metric}};
        my $pass = $fields[0];
        if (!$pass) {
            if ($metric eq $target) {
                $failedTarget = 1;
            } else {
                $failedOther = 1;
            }
        }
    }
    my $status = 0;
    if ($failedTarget && !$failedOther) {
        $status = 1;
    } elsif ($failedTarget && $failedOther) {
        $status = 2;
    } elsif (!$failedTarget && $failedOther) {
        $status = 3;
    }
    return $status;
}

sub readThresholdsForMetric {
    # want thresholds to draw on R plots
    # heterozygosity: number of standard deviations
    # call_rate, identity, magnitude: single fixed value
    # gender: M_max and F_min from adaptive check (need to record these; in R script?)
    my ($metric, $config, $gender) = @_;
    my ($thresh1, $thresh2) = ("NA", "NA");
    if ($metric eq 'gender') {
        open my $in, "<", $gender || croak "Cannot open gender file $gender";
        while (<$in>) {
            chomp;
            my @words = split;
            if ($words[0] eq 'M_max') { $thresh1 = $words[1]; }
            elsif ($words[0] eq 'F_min') { $thresh2 = $words[1]; }
        }
        close $in || croak "Cannot close gender file $gender";        
    } else {
        my %thresh = readThresholds($config);
        $thresh1 = $thresh{$metric};
    }
    return ($thresh1, $thresh2);
}

sub resetOutputs {
    # convenience method for writePlotInputs
    my ($outDir, $metric, $batchNum, $out, $pb, $pn) = @_;
    foreach my $fh ($out, $pb, $pn) {
        close $fh || croak "Cannot close output: $!";
    }
    ($out, $pb, $pn) = getOutputFiles($outDir, $metric, $batchNum);
    return ($out, $pb, $pn);
}

sub runPlotScript {
    # run R script to produce plot
    my ($metric, $plotDir, $inputTotal, $mean, $sd, $thresh1, $thresh2) = @_;
    my $script = "scatter_plot_metric.R";
    for (my $i=0;$i<$inputTotal;$i++) {
        my ($scPath, $pbPath, $pnPath) = getOutputPaths($plotDir, $metric, $i);
        foreach my $path ($scPath, $pbPath, $pnPath) {
            if (!(-r $path)) { croak "Cannot read $path"; }
        }
        my $num = sprintf("%03d", $i);
        my $outPath = $plotDir.'/scatter_'.$metric.'_'.$num.'.pdf';
        my $sdThresh;
        if ($metric eq 'heterozygosity' || $metric eq 'xydiff') { 
            $sdThresh = 'TRUE'; 
        } else  { 
            $sdThresh = 'FALSE'; 
        }
        my @args = ($script, $scPath, $pbPath, $pnPath, $metric, 
                    $mean, $sd, $thresh1, $thresh2, $sdThresh,
                    $i+1, $inputTotal, $outPath);
        wrapCommand(@args);
    }
}

sub writePlotInputs {
    # create input for R plotting script
    # outputs:
    # * Sample count, metric value, and pass/fail status for each sample
    # * Plate boundaries (even-numbered plates only, for plot shading)
    # * Plate names and midpoints, for plot labels
    # for large number of samples, split plates into multiple files
    my ($metric, $dbPath, $iniPath, $resultPath, $configPath, $outDir, 
        $maxBatchSize) = @_;
    my %samplesByPlate = getSamplesByPlate($dbPath, $iniPath);
    my %allResults = readMetricResultHash($resultPath, $configPath);
    my ($batchNum, $batchSize, $plateStart, $writeStartFinish, $i, $tooBig) = 
        (0,0,0,0,0,0);
    my ($out, $pb, $pn) = getOutputFiles($outDir, $metric, $batchNum);
    my @plateLines = ();
    my @plates = sort(keys(%samplesByPlate));
    foreach my $plate (@plates) {
        # each plate goes into buffer; check file size before plate output
        # if too big, close current files and open next before buffer output
        my @samples = @{$samplesByPlate{$plate}};
        foreach my $sample (@samples) {
            # record metric value and pass/fail status for each sample
            my $resultsRef = $allResults{$sample}{$metric};
            if (!defined($resultsRef)) { next; } # excluded sample
            my @results = @{$resultsRef};
            my $status = metricStatus($allResults{$sample}, $metric);
            push(@plateLines, $results[1]."\t".$status."\n");
        }
        if (@plateLines > $maxBatchSize) {
            # plate is too large; plots may not render correctly
            if ($i>0) { # start new plot if plate is oversized
                $batchNum++;
                ($batchSize, $plateStart, $writeStartFinish) = (0,0,0);
                $tooBig = 1;
                ($out, $pb, $pn) = resetOutputs($outDir, $metric, $batchNum,
                                                $out, $pb, $pn);
            }
        } elsif (($batchSize + @plateLines > $maxBatchSize) || $tooBig) {
            # close current files, open next ones, reset counters/flags
            $batchNum++;
            ($batchSize, $plateStart, $writeStartFinish, $tooBig) = (0,0,0,0);
            ($out, $pb, $pn) = resetOutputs($outDir, $metric, $batchNum,
                                            $out, $pb, $pn);
        }
        for (my $j=0;$j<@plateLines;$j++) { # prepend sample count to line
            print $out ($batchSize+$j)."\t".$plateLines[$j];
        }
        $batchSize += @plateLines;
        my $plateFinish = $plateStart + @plateLines;
        my $midPoint = $plateStart + (@plateLines/2);
        print $pn plateLabel($plate, $i)."\t".$midPoint."\n"; 
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
    foreach my $fh ($out, $pb, $pn) {
        close $fh || croak "Cannot close output: $!";
    }
    return $batchNum;
}

sub runMetric {
    my ($metric, $qcDir, $outDir, $config, $gender, $dbPath, $iniPath, 
        $resultPath, $maxBatch) = @_;
    $maxBatch ||= 2000; # was 480;
    my $batchNum = writePlotInputs($metric, $dbPath, $iniPath, $resultPath, 
                                   $config, $outDir, $maxBatch);
    my $inputTotal = $batchNum+1;
    my ($mean, $sd);
    my @results = metricMeanSd($metric, $resultPath, $config);
    if (@results!=0) { 
        # if @results is empty, omit scatterplot for this metric
        ($mean, $sd) = @results; 
        my ($thresh1, $thresh2) = readThresholdsForMetric($metric, $config, 
                                                          $gender);
        runPlotScript($metric, $outDir, $inputTotal, $mean, $sd, 
                      $thresh1, $thresh2);
    }
}


sub runAllMetrics {
    my ($qcDir, $outDir, $config, $gender, $dbPath, $iniPath, $resultPath,
        $maxBatch, $noIntensity) = @_;
    my @metrics = qw(call_rate duplicate heterozygosity identity gender);
    if (!$noIntensity) { push(@metrics, qw/magnitude xydiff/); }
    foreach my $metric (@metrics) {
        runMetric($metric, $qcDir, $outDir, $config, $gender, $dbPath, 
                  $iniPath, $resultPath, $maxBatch);
    }
}
