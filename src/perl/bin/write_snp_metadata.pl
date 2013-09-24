#!/software/bin/perl

#
# Copyright (c) 2013 Genome Research Ltd. All rights reserved.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

# Parse a SNP manifest .csv file and write metadata in .json format
# Input: The manifest
# Outputs:
# 1. SNP manifest, converted to .json format
# 2. Chromosome boundaries with respect to position in SNP manifest

# Revised version reads and sorts the manifest in a memory-efficient fashion

use strict;
use warnings;
use Carp;
use File::Temp qw/tempdir/;
use Getopt::Long;
use Log::Log4perl qw(:easy);
use JSON;

Log::Log4perl->easy_init($ERROR);

sub getIndices {
    # indices of fields in original .csv file
    my %indices = (name => 1,
                   chromosome => 2,
                   position => 3,
                   snp => 5,
                   norm_id => 8
        );
    return %indices;
}

sub readManifest {
    # read required manifest fields into array; also find allele values
    my ($inPath, $verbose) = @_;
    $verbose ||= 0;
    my @manifest;
    open my $in, "<", $inPath || croak "Cannot read input path $inPath";
    my $i = 0;
    my %indices = getIndices();
    if ($verbose) { print "Reading manifest from $inPath\n"; }
    while (<$in>) {
	$i++;
	if ($i == 1) { next; } # first line is header
	elsif ($verbose && $i % 100_000 == 0) { print "$i lines read.\n"; }
	chomp;
	my %snp;
        my @fields = split /,/;
        foreach my $key (qw/name chromosome position norm_id/) {
            $snp{$key} = $fields[$indices{$key}];
        }
        my $alleles = $fields[$indices{'snp'}];
        $alleles =~ s/\W//g; # remove nonword characters
        my @alleles = split('', $alleles);
        if (@alleles!=2) { 
            croak "Failed to find 2 alleles at manifest line ".$i."\n"; 
        }
        foreach my $allele (@alleles) {
            if ($allele !~ /[A-Z]/) { # may include letters other than ACGT
                croak "Invalid allele character at manifest line ".$i."\n";
            }
        }
        $snp{'allele_a'} = $alleles[0];
        $snp{'allele_b'} = $alleles[1];
	push(@manifest, \%snp);
    }
    return @manifest;
}

sub splitManifest {
    # read manifest bpm.csv and split into separate files for each chromosome
    # also remove extra whitespace and convert to numeric chromosome IDs
    my $inPath = shift;
    my $outDir = shift;
    my $verbose = shift;
    my (%outPaths, %outFiles);
    my %indices = getIndices();
    my $cindex = $indices{'chromosome'};
    my $i = 0;
    if ($verbose) { print "Reading manifest $inPath for split\n"; }
    open my $in, "<", $inPath || croak "Cannot read input path $inPath";
    while (<$in>) {
        $i++;
        if ($verbose && $i % 100_000 == 0) { print "$i lines read.\n"; }
	if ($i == 1) { next; } # first line is header
	$_ =~ s/\s+$//g; # remove whitespace (including \r) from end of line
	my @fields = split /,/;
	my $chrom = $fields[$cindex];
	# ensure chromosome ID is in numeric format
	if ($chrom eq 'X') { $chrom = 23; }
	elsif ($chrom eq 'Y') { $chrom = 24; }
	elsif ($chrom eq 'XY') { $chrom = 25; }
	elsif ($chrom eq 'MT') { $chrom = 26; }
	$fields[$cindex] = $chrom;
	if (!$outPaths{$chrom}) {
	    my $outPath = $outDir."/unsorted.".$chrom.".csv";
	    $outPaths{$chrom} = $outPath;
	    open my $out, '>', $outPath || croak "Cannot open output $outPath";
	    $outFiles{$chrom} = $out;
	}
	print { $outFiles{$chrom} } join(',', @fields)."\n";
    }
    close $in || croak "Cannot close input $inPath";
    foreach my $chrom (keys(%outFiles)) {
	my $outPath = $outPaths{$chrom};
	close $outFiles{$chrom} || croak "Cannot close output $outPath";
    }
    return %outPaths;
}

sub writeSortedByPosition {
    # read unsorted input; sort by position; write to output
    # return total number of inputs
    my ($inPath, $outPath) = @_;
    open my $in, "<", $inPath || croak "Cannot read input path $inPath";
    my %input;
    my %indices = getIndices();
    my $pindex = $indices{'position'};
    while (<$in>) {
	 my @fields = split /,/;
	 $input{$fields[$pindex]} = $_;
    }
    close $in || croak "Cannot close input $inPath";
    my @positions = keys(%input);
    my @sorted = sort {$a <=> $b} @positions;
    open my $out, ">", $outPath ||  croak "Cannot open output $outPath";
    foreach my $pos (@sorted) {
	print $out $input{$pos};
    }
    close $out || croak "Cannot close output $outPath";
    my $total = @sorted;
    return $total;
}


sub run {
    # sort manifest by (chromosome, position) and write as .json
    # also find chromosome boundaries wrt sorted manifest
    my ($manifest, $chrJson, $snpJson, $out, $verbose);
    GetOptions('manifest=s' => \$manifest,
	       'chromosomes=s' => \$chrJson,
               'snp=s'=> \$snpJson,
               'verbose' => \$verbose);
    $verbose ||= 0;
    unless (-e $manifest) {
        croak("Manifest file \"$manifest\" does not exist");
    }
    my $temp = tempdir( "temp_snp_manifest_XXXXXX", CLEANUP => 1 );
    my %unsortedPaths = splitManifest($manifest, $temp, $verbose);
    my @chromosomes = keys(%unsortedPaths);
    @chromosomes = sort {$a <=> $b} @chromosomes; # ascending numeric sort
    my @bounds;
    my $start = 0;
    my $end = 0;
    my @sortedPaths;
    foreach my $chr (@chromosomes) {
	# write sorted .csv files and record chromosome boundaries 
	my $sortedPath = $temp."/sorted.".$chr.".csv";
	push @sortedPaths, $sortedPath;
	my $total = writeSortedByPosition($unsortedPaths{$chr}, $sortedPath);
	$end += $total;
	push (@bounds, { 'chromosome' => $chr,
			 'start' => $start,
			 'end' => $end });
	$start = $end;
    }
    if ($chrJson) {
	open $out, ">", $chrJson || croak "Cannot open output $chrJson";
        print $out to_json(\@bounds);
        close $out || croak "Cannot close output $chrJson";
    }
    if ($snpJson) {
	# concatenate sorted .csv files
	my $sortedAll = $temp."/sorted.all.csv";
	system("cat ".join(" ", @sortedPaths)." > ".$sortedAll);
	# read into array and write as JSON
	my @manifest = readManifest($sortedAll, $verbose);	
        open $out, ">", $snpJson || croak "Cannot open output $snpJson";
        print $out to_json(\@manifest);
        close $out || croak "Cannot close output $snpJson";
    }   
}


run();
