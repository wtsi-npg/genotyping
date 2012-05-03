#! /usr/bin/env perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# script to generate an html index page for all plots

use strict;
use warnings;
use CGI::Pretty qw/:standard *table/; # writes prettier html code
use WTSI::Genotyping::QC::QCPlotShared; # qcPlots module to define constants
use WTSI::Genotyping::QC::QCPlotTests;

sub writeHeatMapLinkTable {
    # write table with link to heatmap index, and thumbnail examples of first 3 heatmaps
    my $heatMapIndex = shift;
    my $heatMapDir = shift;
    my $output = shift;
    print $output start_table({-border=>1, -cellpadding=>4},);
    print $output Tr([ td({colspan=>3}, h2(a({href=>$heatMapIndex}, "Plate heatmap index"))) ]); 
    my @examples = ();
    foreach my $expr qw/cr het xydiff/ {
	my @files = glob($heatMapDir.'/plot_'.$expr.'*');
	@files = sort(@files);
	push(@examples, $files[0]); # $files[0] may be undefined, if glob results were empty
    }
    print $output Tr([ th({colspan=>3}, 'Example heatmaps: Click for full display') ]); 
    my ($height, $width) = (200,200);
    my @links;
    foreach my $png (@examples) { 
	my $href;
	if ($png) { # only link to defined files
	    $href = a({href=>$heatMapIndex}, img({height=>$height, width=>$width, src=>$png, alt=>$png}));
	    push(@links, $href); 
	}
	if (@links==0) { push(@links, 'No plots available!'); }
    }
    print $output Tr([td(\@links)]);
    print $output end_table();
}

sub writeSummaryTable {
    # write table of basic QC stats
    my $totalsPath = shift;
    my $output = shift;
    my %data;
    open IN, "< $totalsPath" || die "Cannot open input file $totalsPath: $!";
    while (<IN>) {
	if (/^#/) { next; } # omit comments
	my @words = split;
	$data{$words[0]} = $words[1];
    }
    close IN || die "Cannot close input file $totalsPath: $!";
    my $percentPass = (($data{'TOTAL_SAMPLES'} - $data{'TOTAL_FAILURES'}) / $data{'TOTAL_SAMPLES'})*100;
    $percentPass = sprintf("%.1f", $percentPass)."%";
    my @keys = ('Total samples', 'Total failures', 'Pass rate', 'Call rate mean (Phred)', 'Call rate mean/SD', 
		'Het mean/SD', 'Het max divergence');
    my $crScore = sprintf("%.0f", -10*(log(1 - $data{'CR_MEAN'})/log(10) ) );
    my $cr = sprintf("%.4f", $data{'CR_MEAN'})." +/- ".sprintf("%.4f", $data{'CR_STANDARD_DEVIATION'});
    my $het = sprintf("%.4f", $data{'HET_MEAN'})." +/- ".sprintf("%.4f", $data{'HET_STANDARD_DEVIATION'});
    my @values = ($data{'TOTAL_SAMPLES'}, $data{'TOTAL_FAILURES'}, $percentPass, $crScore, $cr, $het, 
		  sprintf("%.4f", $data{'HET_MAX_DIVERGENCE'}));
    print $output start_table({-border=>1, -cellpadding=>4},);
    foreach (my $i=0;$i<@keys;$i++) {
	print $output Tr( th({align=>"left"}, $keys[$i]), td($values[$i]) );
    }
    print $output end_table();
}


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
    'xydiff_beanplot.png' => 'XYdiff beanplot',
    'xydiff_boxplot.png' => 'XYdiff boxplot',
);

my $experiment = shift(@ARGV); # experiment name
my $plotDir = shift(@ARGV); # input/output directory path
chdir($plotDir); # find and link to relative paths wrt plotdir
my $outPath = $WTSI::Genotyping::QC::QCPlotShared::mainIndex;
my @png = glob('*.png');
@png = sort(@png);
open OUT, "> $outPath" || die "Cannot open output path $outPath: $!";
print OUT header(-type=>''), # create the HTTP header; content-type declaration not needed for writing to file
    start_html(-title=>"$experiment: Summary of results",
	       -author=>'Iain Bancarz <ib5@sanger.ac.uk>',
	       ),
    h1($experiment), # level 1 header
    ;
# table of basic summary stats (if available)
my $totalsPath = $plotDir.'/failTotals.txt';
if (-r $totalsPath) { writeSummaryTable($totalsPath, \*OUT); }
print OUT p('&nbsp;'); # spacer before next table
# link to heatmap index (if available)
my $heatMapDir = 'plate_heatmaps'; # TODO move these names into QCPlotsShared
my $heatMapIndex = $heatMapDir."/".$WTSI::Genotyping::QC::QCPlotShared::plateHeatmapIndex;
if (-r $heatMapIndex) {  
    writeHeatMapLinkTable($heatMapIndex, $heatMapDir, \*OUT);
}
# table of contents for subsequent links
print OUT h2("Summary Plots: Contents");
my @pngRefs;
foreach my $png (@png) { 
    my $desc;
    if ($descriptions{$png}) { $desc = $descriptions{$png}; }
    else { $desc = $png; }
    push (@pngRefs, a({href=>"#".$png}, $desc) ); 
}
print OUT ul( li(\@pngRefs) );
# links to general-purpose overview plots (ie. any PNG file in the plots directory)
print OUT start_table({-border=>1, -cellpadding=>4},);
print OUT Tr([th('Summary Plots: Click for large version')]);
my ($height, $width) = (400,400);
foreach my $png (@png) {
    print OUT Tr([td(a({name=>$png}),
		     a({href=>$png}, img({height=>$height, width=>$width, src=>$png, alt=>$png})),
		  )]);
}
print OUT end_table();
print OUT end_html();
close OUT;

# test output for XML validity
open my $fh, "< $outPath";
if (WTSI::Genotyping::QC::QCPlotTests::xmlOK($fh)) { close $fh; exit(0); } # no error
else { close $fh; exit(1); } # error found
