# Author:  Iain Bancarz, ib5@sanger.ac.uk
# July 2012

# Generate human-readable reports containing QC results

package WTSI::Genotyping::QC::Reports;

use strict;
use warnings;
use Carp;
use JSON;
use WTSI::Genotyping::QC::QCPlotShared qw/getDatabaseObject getSummaryStats meanSd median readQCNameArray readQCShortNameHash/; 
use WTSI::Genotyping::Database::Pipeline;
use Exporter;

our @ISA = qw/Exporter/;
our @EXPORT = qw/dbDatasetInfo dbSampleInfo textForDatasets textForPlates textForCsv writeCsv writeSummaryText/;
our @dbInfoHeaders = qw/run project supplier snpset/;


sub dbDatasetInfo {
    # get general information on analysis run(s) from pipeline database
    my $dbfile = shift;
    my $db = getDatabaseObject($dbfile);
    my @datasetInfo;
    my @runs = $db->piperun->all;
    foreach my $run (@runs) {
	my @info;
	my @datasets = $run->datasets->all;
	foreach my $dataset (@datasets) {
	    @info = ($run->name, $dataset->if_project, $dataset->datasupplier->name, $dataset->snpset->name);
	    push(@datasetInfo, \@info);
	    #print join("\t", @info)."\n";
	}
    }
    return @datasetInfo;
}

sub dbSampleInfo {
    # get general information on analysis run from pipeline database
    # return a hash indexed by sample
    my $dbfile = shift;
    my $db = getDatabaseObject($dbfile);
    my %sampleInfo;
    my @runs = $db->piperun->all;
    foreach my $run (@runs) {
	my @info;
	my @datasets = $run->datasets->all;
	foreach my $dataset (@datasets) {
	    my @samples = $dataset->samples->all;
	    foreach my $sample (@samples) {
		@info = ($run->name, $dataset->if_project, $dataset->datasupplier->name, $dataset->snpset->name);
		$sampleInfo{$sample->name} = \@info;
	    }
	}
    }
    return %sampleInfo;
}

sub getCsvHeaders {
    my @headers = @dbInfoHeaders;
    push @headers, qw/sample plate well pass/;
    my @metricNames = readQCNameArray();
    foreach my $metric (@metricNames) {
	my @suffixes;
	if ($metric eq 'gender') {
	    @suffixes = qw/pass xhet inferred supplied/;
	} else {
	    @suffixes = qw/pass value/;
	}
	foreach my $suffix (@suffixes) {
	    push(@headers, $metric."_".$suffix);
	}
    }
    return @headers;
}

sub getPlateInfo {
    # for each plate, find: samples found, samples excluded, CR mean/median, het mean
    my %records = %{ shift() };
    my @metricNames = readQCNameArray();
    my (%crByPlate, %hetByPlate, %passByPlate, %samplesByPlate, %plateStats, @cr, @het, $pass, $samples);
    foreach my $sample (keys(%records)) {
	my %record = %{$records{$sample}};
	my $plate = $record{'plate'};
	my $samplePass = 1;
	foreach my $metric (@metricNames) {
	    my @values;
	    my ($pass, $val) = @{$record{$metric}};
	    if ($pass==0) { $samplePass = 0; }
	    if ($metric eq 'call_rate') { 
		if ($crByPlate{$plate}) { push @{$crByPlate{$plate}}, $val; }
		else { $crByPlate{$plate} = [$val]; }
		push @cr, $val;
	    } elsif ($metric eq 'heterozygosity') {  
		if ($hetByPlate{$plate}) { push @{$hetByPlate{$plate}}, $val; }
		else { $hetByPlate{$plate} = [$val]; }
		push @het, $val;
	    }
	}
	$passByPlate{$plate} += $samplePass;
	$pass += $samplePass;
	$samplesByPlate{$plate}++;
	$samples++;
    }
    # stats for each plate
    foreach my $plate (keys(%samplesByPlate)) {
	my @fields = findPlateFields($samplesByPlate{$plate}, $passByPlate{$plate}, $crByPlate{$plate},
				     $hetByPlate{$plate});
	$plateStats{$plate} = [@fields];
    }
    my @aggregate = findPlateFields($samples, $pass, \@cr, \@het);  # aggregate stats for all plates
    return (\%plateStats, \@aggregate);
}

sub getSampleInfo {
    # for each sample, get plate, well, metric values and pass/fail status
    # also get overall sample pass/fail
    my %records = %{ shift() };
    my @metricNames = readQCNameArray();
    # metric name order: ["call_rate","heterozygosity","duplicate","identity","gender","xydiff]"
    my @sampleFields;
    my @samples = keys(%records);
    @samples = sort @samples;
    foreach my $sample (@samples) {
	my %record = %{$records{$sample}};
	my $samplePass = 1;
	my @fields = ($sample, $record{'plate'}, $record{'address'}, $samplePass); # $samplePass is placeholder
	foreach my $metric (@metricNames) {
	    my @status =  @{$record{$metric}}; # pass/fail and one or more metric values
	    if ($status[0]==0) { $samplePass = 0; }
	    push(@fields, @status); 
	}
	$fields[3] = $samplePass;
	push @sampleFields, \@fields;
    }
    return @sampleFields;
}

sub findPlateFields {
    # find report fields for a single plate, or an entire experiment
    my ($total, $pass, $crRef, $hetRef) = @_;
    my $excl = $total - $pass;
    my $exclPercent = ($excl/$total)*100;
    my @cr = @{$crRef};
    my ($crMean, $crSd) = meanSd(@cr);
    my $crMedian = median(@cr);
    my ($hetMean, $hetSd) = meanSd(@{$hetRef});
    return ($total, $excl, $exclPercent, $crMean, $crMedian, $hetMean);
}

sub plateFieldsToText {
    # convert title and unformatted results to array of formatted text fields
    my $title = shift;
    my @stats = @{ shift() };
    my @out = ($title, );
    for (my $i=0;$i<@stats;$i++) {
	my $stat;
	if ($i>2) { $stat = sprintf("%.3f", $stats[$i]); }
	elsif ($i==2) { $stat = sprintf("%.1f", $stats[$i]); }
	else { $stat = $stats[$i]; }
	push(@out, $stat);
    }
    return @out;
}

sub readJson {
    my $inPath = shift;
    my @lines;
    open my $in, "<", $inPath || croak "Cannot open input path $inPath";
    while (<$in>) { push(@lines, $_); }
    close $in || croak "Cannot close input path $inPath";
    my $ref = decode_json(join('', @lines));
    return $ref;
}

sub sampleFieldsToText {
    # convert unformatted results to array of formatted text fields
    my @fields = @{ shift() };
    my @out;
    for (my $i=0;$i<@fields;$i++) {
	if ($fields[$i] =~ /^\d+$/ || $fields[$i] =~ /[a-zA-Z]+/) { push(@out, $fields[$i]); } # text or integers
	else { push(@out, sprintf('%.4f', $fields[$i])); }
    }
    return @out;
}

sub textForDatasets {
    # text for datasets table
    my $dbPath = shift;
    my @text = (\@dbInfoHeaders, );
    my @datasetInfo = dbDatasetInfo($dbPath);
    foreach my $ref (@datasetInfo) {
	push(@text, $ref);
    }
    return @text;
}

sub textForPassRate {
    ### TODO this may be unnecessary, same results appear in final line of textForPlates
    my $resultPath = shift;
    my ($total, $fails, $passRate, $cr_mean, $cr_sd) = getSummaryStats($resultPath);
    my @headers = qw/total_samples failed_samples pass_rate/;
    my @text = (\@headers,);
    push(@text, [$total, $fails, sprintf('%.2f', $passRate*100)."%"]);
    return @text;
}

sub textForPlates {
    my $resultPath = shift;
    my $resultsRef = readJson($resultPath);
    my ($plateRef, $aggRef) = getPlateInfo($resultsRef);
    my %plateStats = %$plateRef;
    my @aggregate = @$aggRef;
    my @plates = keys(%plateStats);
    @plates = sort @plates;
    my @headers = qw/plate samples excluded excl% cr_mean cr_median het_mean/;
    my @text = (\@headers, );
    foreach my $plate (@plates) {
	my @out = plateFieldsToText($plate, $plateStats{$plate});
	push(@text, \@out);
    }
    my @out = plateFieldsToText("ALL_PLATES", \@aggregate);
    push(@text, \@out);
    return @text;
}

sub textForCsv {
    my ($resultPath, $dbPath) = @_;
    my $resultsRef = readJson($resultPath);
    my @headers = getCsvHeaders();
    my @text = (\@headers,);
    my @sampleFields = getSampleInfo($resultsRef);
    my %dbInfo = dbSampleInfo($dbPath);
    foreach my $ref (@sampleFields) {
	my @out = sampleFieldsToText($ref);
	my $sample = $out[0];
	unless ($dbInfo{$sample}) { print "### ".$sample."\n"; }
	unshift(@out, @{$dbInfo{$sample}});
	if ($#headers != $#out) { croak "Numbers of output headers and fields differ: $#headers != $#out"; }
	push(@text, \@out);
    }
    return @text;
}

sub writeCsv {
    my ($resultPath, $dbPath, $outPath) = @_;
    my @text = textForCsv($resultPath, $dbPath);
    open my $out, ">", $outPath || croak "Cannot open output path $outPath";
    foreach my $lineRef (@text) {
	print $out join(',', @$lineRef)."\n";
    }
    close $out || croak  "Cannot close output path $outPath";
}


sub writeSummaryText {
    # convenience method to show text output in summary
    my ($resultPath, $dbPath, $outPath) = @_;
    open my $out, ">", $outPath || croak "Cannot open output path $outPath";
    print $out "* Genotype Pipeline Run Summary *\n\n";
    print $out "Datasets\n";
    my @datasetText = textForDatasets($dbPath);
    foreach my $ref (@datasetText) {
	print $out join("\t", @$ref)."\n";
    }
    print $out "\nPass/fail statistics\n";
    my @passRateText = textForPassRate($resultPath);
    my @headers = @{$passRateText[0]};
    my @values =  @{$passRateText[1]};
    foreach my $i (0..@headers-1) {
	print $out $headers[$i]."\t".$values[$i]."\n";
    }
    print $out "\nResults by plate\n";
    my @plateText = textForPlates($resultPath);
    foreach my $ref (@plateText) {
	print $out join("\t", @$ref)."\n";
    }
    close $out || croak  "Cannot close output path $outPath";
    return 1;
}

    #my @headers = ("Sscape study name","LIMS project name","BeadChip","Sample ID","Sanger sample name","Supplier sample name","IDAT filename","Plate","Well","Infinium barcode","Region","Ethnicity","Genotype file","Sample status","Supplied gender","Sequenom Gender","Illumina gender","Gender status","Gender score","Call rate status","Call rate","Het status","Het score","Identity status","Sequenom concordance score","Duplicate status","Concordance score","Sample match","XYdiff status","XYdiff score");

#    foreach my $sample ()



