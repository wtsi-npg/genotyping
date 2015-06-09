#! /software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# Script to create an HTML index page for heatmap plots (uses functions from CGI module)
# glob plate names & plot paths from given output directory
# TODO replace hard-coded text patterns (for glob etc.) with a config data structure

use warnings; 
use strict;
use CGI::Pretty qw/:standard *table/; # writes prettier html code
use Cwd;
use File::Basename;
use WTSI::NPG::Genotyping::QC::QCPlotTests;

our $VERSION = '';

sub getLinkThumbnail {
    # get thumbnail link HTML for given image path; optionally supply height & width in pixels
    # assumes linked file is in same directory as linking page
    my ($path, $height, $width) = @_;
    $height ||= 250;
    $width ||= 250;
    my $link;
    unless (defined($path)) { $link = "NOT_FOUND"; } # eg. xy plots not produced for gencall
    else { $link = a({href=>$path}, img({height=>$height, width=>$width, src=>$path, alt=>$path}) ); }
    return $link;
}

sub getPlateName {
    # extract plate name from plot path
    # assume plot filenames are in the form plot_PREFIX_PLATE.png 
    # prefix may *not* contain _'s, plate may contain .'s and _'s)
    my $plotPath = shift;
    my $plotName = basename($plotPath);
    my @items = split(/_/, $plotName);
    my @tail = splice(@items, 2);
    my $tail = join('_', @tail);
    @items = split(/\./, $tail);
    my $name = shift(@items);
    return $name;
}

sub getPlateInfo {   
    # return list of plate names, and hashes of CR, het, xy plot paths indexed by plate
    # glob given directory; assume plot filenames are in the form PREFIX_PLATE.png (prefix may contain _'s)
    my $plotDir = shift;
    my (%plates, %crPlots, %hetPlots, %magPlots);
    my ($crExpr, $hetExpr, $magExpr) = qw(plot_cr_* plot_het_* 
                                          plot_magnitude_*);
    my @files = glob($plotDir.'/{cr,het,magnitude,}*.png');
    foreach my $file (@files) {
        my $plate = getPlateName($file);
        $plates{$plate} = 1;
        if ($file =~ $crExpr) { $crPlots{$plate} = $file; }
        elsif ($file =~ $hetExpr) { $hetPlots{$plate} = $file; }
        elsif ($file =~ $magExpr) { $magPlots{$plate} = $file; }
    }
    my @plates = sort(keys(%plates));
    return (\@plates, \%crPlots, \%hetPlots, \%magPlots);
}


my ($experiment, $plotDir, $outFileName) = @ARGV; # experiment name, input/output directory, output filename
if (@ARGV!=3) { 
    die "Usage: $0 experiment_name input/output_directory output_filename\n";
} elsif (!(-e $plotDir && -d $plotDir)) {
    die "Output path '$plotDir' does not exist or is not a directory";
} 
my @refs = getPlateInfo($plotDir);
my @plates = @{shift(@refs)};
my %crPlots = %{shift(@refs)};
my %hetPlots = %{shift(@refs)};
my %magPlots = %{shift(@refs)};
# must write index to given plot directory -- otherwise links are broken
my $outPath = $plotDir.'/'.$outFileName;
open my $out, ">", $outPath || die "Cannot open output path $outPath: $!";
print $out header(-type=>''), # create the HTTP header; content-type declaration not needed for writing to file
    start_html(-title=>"$experiment: Plate heatmap index",
	       -author=>'Iain Bancarz <ib5@sanger.ac.uk>',
	       ),
    h1("$experiment: Plate heatmap index"), # level 1 header
    #p('Some body text goes here')
    ;
print $out start_table({-border=>1, -cellpadding=>4},);
print $out Tr({-align=>'CENTER',-valign=>'TOP'}, [ 
		 th(['Plate', 'Sample CR','Sample het rate','Sample Magnitude',
		    ]),]);
foreach my $plate (@plates) {
    # for each plate -- use plate name to look up CR, Het, and Mag filenames & generate links
    unless (defined($crPlots{$plate}) || defined($hetPlots{$plate}) 
            || defined($magPlots{$plate}) ) { next; }
    print $out Tr({-valign=>'TOP'}, [ td([$plate, 
					 getLinkThumbnail($crPlots{$plate}), 
					 getLinkThumbnail($hetPlots{$plate}), 
					 getLinkThumbnail($magPlots{$plate}),
					]),
	]);
}
print $out end_table();
print $out end_html();
close $out;

# test output for XML validity
open my $fh, "<", $outPath;
if (WTSI::NPG::Genotyping::QC::QCPlotTests::xmlOK($fh)) { close $fh; exit(0); } # no error
else { close $fh; exit(1); } # error found
