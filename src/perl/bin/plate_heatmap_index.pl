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
use WTSI::Genotyping::QC::QCPlotTests;

sub getLinkThumbnail {
    # get thumbnail link HTML for given image path; optionally supply height & width in pixels
    # assumes linked file is in same directory as linking page
    my ($path, $height, $width) = @_;
    $height ||= 200;
    $width ||= 200;
    my $link;
    unless (defined($path)) { $link = "NOT_FOUND"; } # eg. xy plots not produced for gencall
    else { $link = a({href=>$path}, img({height=>$height, width=>$width, src=>$path, alt=>$path}) ); }
    return $link;
}

sub getPlateName {
    # extract plate name from plot filename
    # assume plot filenames are in the form plot_PREFIX_PLATE.png 
    # prefix may *not* contain _'s, plate may contain .'s and _'s)
    my $plotName = shift;
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
    my $startDir = getcwd;
    chdir($plotDir);
    my (%plates, %crPlots, %hetPlots, %xydiffPlots);
    my ($crExpr, $hetExpr, $xydiffExpr) = qw(plot_cr_* plot_het_* plot_xydiff_*);
    my @files = glob('{cr,het,xydiff,}*.png');
    foreach my $file (@files) {
	my $plate = getPlateName($file);
	$plates{$plate} = 1;
	if ($file =~ $crExpr) { $crPlots{$plate} = $file; }
	elsif ($file =~ $hetExpr) { $hetPlots{$plate} = $file; }
	elsif ($file =~ $xydiffExpr) { $xydiffPlots{$plate} = $file; }
    }
    my @plates = sort(keys(%plates));
    chdir($startDir);
    return (\@plates, \%crPlots, \%hetPlots, \%xydiffPlots);
}

sub getTextLinks {
    # create unordered list of links to relevant text files, to be inserted into table
    # assume files are in current working directory
    my @paths = @{shift()};
    my @links = ();
    foreach my $path (@paths) {
	my @terms = split(/\//, $path);
	my $name = pop(@terms);
	push(@links, a({href=>$name}, $name));
    }
    my $linkList = ul(li(\@links));
    return $linkList;
}

sub getTextPaths {
    # get relevant .txt files for given plate
    my $plotDir = shift;
    my @plates = @{shift()};
    my %textPaths;
    foreach my $plate (@plates) {
	my @paths = glob($plotDir.'/{cr,het,xydiff,}_'.$plate.'*.txt'); # get .txt files containing plate name
	@paths = sort(@paths);
	$textPaths{$plate} = \@paths;
    }
    return %textPaths;
}

my ($experiment, $plotDir, $outFileName) = @ARGV; # experiment name, input/output directory, output filename
my @refs = getPlateInfo($plotDir);
my @plates = @{shift(@refs)};
my %crPlots = %{shift(@refs)};
my %hetPlots = %{shift(@refs)};
my %xydiffPlots = %{shift(@refs)};
my %textPaths = getTextPaths($plotDir, \@plates);
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
		 th(['Plate', 'Sample CR','Sample het rate','Sample XYdiff', 'Plot inputs',
		    ]),]);
foreach my $plate (@plates) {
    # for each plate -- use plate name to look up CR, Het, and XYdiff filenames & generate links
    unless (defined($crPlots{$plate}) || defined($hetPlots{$plate}) || defined($xydiffPlots{$plate}) ) { next; }
    print $out Tr({-valign=>'TOP'}, [ td([$plate, 
					 getLinkThumbnail($crPlots{$plate}), 
					 getLinkThumbnail($hetPlots{$plate}), 
					 getLinkThumbnail($xydiffPlots{$plate}),
					 getTextLinks($textPaths{$plate}),
					]),
	]);
}
print $out end_table();
print $out end_html();
close $out;

# test output for XML validity
open my $fh, "<", $outPath;
if (WTSI::Genotyping::QC::QCPlotTests::xmlOK($fh)) { close $fh; exit(0); } # no error
else { close $fh; exit(1); } # error found
