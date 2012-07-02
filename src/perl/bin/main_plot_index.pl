#! /usr/bin/env perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# script to generate an html index page for all plots

use strict;
use warnings;
use CGI::Pretty qw/:standard *table/; # writes prettier html code
use WTSI::Genotyping::QC::QCPlotShared; # qcPlots module to define constants
use WTSI::Genotyping::QC::QCPlotTests;


sub getSummaryStats {
    # read .json file of qc status and get summary values
    # interesting stats: mean/sd of call rate, and overall pass/fail
    my $inPath = shift;
    my %allResults = WTSI::Genotyping::QC::QCPlotShared::readQCResultHash($inPath);
    my (@cr, $fails);
    my @samples = keys(%allResults);
    my $total = @samples;
    foreach my $sample (@samples) {
	my %results = %{$allResults{$sample}};
	my $samplePass = 1;
	foreach my $metric (keys(%results)) {
	    my ($pass, $value) = @{$results{$metric}};
	    if ($metric eq 'call_rate') { push(@cr, $value); }
	    unless ($pass) { $samplePass = 0; }
	}
	unless ($samplePass) { $fails++; }
    }
    my ($mean, $sd) = WTSI::Genotyping::QC::QCPlotShared::meanSd(@cr);
    my $passRate = 1 - ($fails/$total);
    return ($total, $fails, $passRate, $mean, $sd);
}

sub writePlotLinks {
    # write index of QC plots in current directory
    my ($out, $descsRef) = @_;
    my %descs = %$descsRef; # output descriptions
    my @png = sort(glob('*.png'));
    my %fileNames = WTSI::Genotyping::QC::QCPlotShared::readQCFileNames();
    my $plateDir = $fileNames{'plate_dir'};
    my $plateIndex = $fileNames{'plate_index'};
    my $plateIndexPath = $plateDir."/".$plateIndex;
    my @heatMapPng = sort(glob($plateDir."/*.png"));
    my $heatMapExample = shift(@heatMapPng);
    # write box for heatmap link
    my ($height, $width) = (250,250); # thumbnail size
    if ($heatMapExample && -r $plateIndexPath) {
	my $link = h3(a({href=>$plateIndexPath}, "Plate heatmap index"));
	my $png = $heatMapExample;
	my $thumb = a({href=>$plateIndexPath}, img({height=>$height, width=>$width, src=>$png, alt=>$png}));
	print $out start_table({-border=>1, -cellpadding=>4},);
	print $out Tr(td({valign=>'top'}, $link));
	print $out Tr(td({align=>'center'}, b("Example heatmap").br.$thumb));    
	print $out end_table();
	print $out p('&nbsp;'); # spacer 
    }
    # write other QC plots
    my $contents = "contents";
    print $out a({name=>$contents});
    print $out h2("General QC Plots");
    print $out start_table({-border=>1, -cellpadding=>4},);
    print $out Tr( th("File"), th("Description"));
    foreach my $png (@png) {
	my $link = a({href=>"#".$png}, $png);
	my $desc = $descs{$png};
	$desc ||= "[No description]";
	print $out Tr(td($link), td($desc));
    }
    print $out end_table();
    print $out p('&nbsp;'); # spacer 
    # print plots (actual size)
    foreach my $png (@png) {
	print $out a({name=>$png});
	print $out img({src=>$png, alt=>$png});
	print $out p(a({href=>"#".$contents}, "Back to contents"));
    }
    
}

sub writeSummaryTable {
    # write table of basic QC stats to given filehandle
    my $qcResultsPath = shift;
    my $output = shift;
    my ($total, $fails, $passRate, $crMean, $crSD) = getSummaryStats($qcResultsPath);
    my $percentPass = sprintf("%.1f", 100*$passRate)."%";
    my $crScore = sprintf("%.0f", -10*(log(1 - $crMean)/log(10))); # Phred score
    my $cr = sprintf("%.4f", $crMean)." +/- ".sprintf("%.4f", $crSD);
    my @keys = ('Total samples', 'Total failures', 'Pass rate', 'Call rate mean (Phred)', 'Call rate mean/SD');
    my @values = ($total, $fails, $percentPass, $crScore, $cr);
    print $output start_table({-border=>1, -cellpadding=>4},);
    foreach (my $i=0;$i<@keys;$i++) {
	print $output Tr( th({align=>"left"}, $keys[$i]), td($values[$i]) );
    }
    print $output end_table();
}

# TODO split descriptions into categories for table of contents; eg. boxplot, histogram, pass/fail, other
my %descriptions = (
    'cr_beanplot.png' => "CR beanplot",
    'cr_boxplot.png' => "CR boxplot",
    'crHetDensityHeatmap.png' => "Heatmap of sample density by CR and het rate",
    'crHetDensityScatter.png' => "Scatterplot of samples by CR and het rate",
    'crHistogram.png' => "CR distribution histogram",
    'failScatterDetail.png' => "Scatterplot of failed samples passing CR/Het filters",
    'failScatterPlot.png' => "Scatterplot of all failed samples by CR and het rate",
    'failsCombined.png' => "Combinations of QC failure causes",
    'failsIndividual.png' => "Individual QC failure causes",
    'het_beanplot.png' => "Heterozygosity beanplot",
    'het_boxplot.png' => "Heterozygosity boxplot",
    'hetHistogram.png' => "Heterozygosity distribution histogram",
    'platePopulationSizes.png' => "Number of samples found per plate",
    'sample_xhet_gender_model.png' => "Summary of gender model",
    'xydiff.png' => "XYdiff histogram",
    'xydiff_beanplot.png' => 'XYdiff beanplot',
    'xydiff_boxplot.png' => 'XYdiff boxplot',
);

my $plotDir = shift(@ARGV); # input/output directory path
my $qcStatus = shift(@ARGV); # .json file with qc status
my $title = shift(@ARGV); # experiment name
$title ||= 'Untitled Analysis';
chdir($plotDir); # find and link to relative paths wrt plotdir
my %fileNames = WTSI::Genotyping::QC::QCPlotShared::readQCFileNames();
my $outPath = $fileNames{'main_index'};
my @png = glob('*.png');
@png = sort(@png);
open OUT, "> $outPath" || die "Cannot open output path $outPath: $!";
print OUT header(-type=>''), # create the HTTP header; content-type declaration not needed for writing to file
    start_html(-title=>"$title: Summary of results",
	       -author=>'Iain Bancarz <ib5@sanger.ac.uk>',
	       ),
    h1($title.": QC Results"), # level 1 header
    ;
# table of basic summary stats (if available)
if (-r $qcStatus) { writeSummaryTable($qcStatus, \*OUT); }
print OUT p('&nbsp;'); # spacer before next table
writePlotLinks(\*OUT, \%descriptions);
print OUT end_html();
close OUT;

# test output for XML validity
open my $fh, "< $outPath";
if (WTSI::Genotyping::QC::QCPlotTests::xmlOK($fh)) { close $fh; exit(0); } # no error
else { close $fh; exit(1); } # error found
