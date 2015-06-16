#!/software/bin/perl

use utf8;

package main;

use strict;
use warnings;
use Carp;
use Getopt::Long;
use Pod::Usage;

use WTSI::NPG::Genotyping::QC::SnpID qw(convertFromIlluminaExomeSNP);
use WTSI::NPG::Genotyping::SNPSet;

our $VERSION = '';

my ($manifestPath, $plexPath, $outPath, $verbose);

GetOptions('manifest=s'        => \$manifestPath,
           'plex=s'            => \$plexPath,
	   'out=s'             => \$outPath,
           'verbose'           => \$verbose,
           'help'       => sub { pod2usage(-verbose => 2, -exitval => 0) },);

if (!$plexPath) {
    croak("Must specify a QC plex SNP set path");
} elsif (!(-e $plexPath)) {
    croak("QC plex path '$plexPath' does not exist");
}
if (!($manifestPath)) {
    croak("Must supply a --manifest argument");
} elsif (!(-e $manifestPath)) {
    croak("Manifest path '$manifestPath' does not exist");
}

my @shared = getIntersectingSNPsManifest($manifestPath, $plexPath);
if ($verbose) {
    print "Comparing manifest to QC plex\n";
    my $total = @shared;
    print "$total shared SNPs found\n";
}
if ($outPath) {
    open my $out, ">", $outPath || croak "Cannot open output '$outPath'";
    foreach my $snp (@shared) { print $out "$snp\n"; }
    close $out || croak "Cannot close output '$outPath'";
}

sub getIntersectingSNPsManifest {
    # find SNPs in given .bpm.csv manifest which are also in QC plex
    my ($manifestPath, $plexPath) = @_;
    my @manifest;
    open my $in, "<", $manifestPath || croak("Cannot open '$manifestPath'");
    while (<$in>) {
	if (/^Index/msx) { next; } # skip header line
	chomp;
	my @words = split /,/msx;
	push(@manifest, $words[1]);
    }
    close $in || croak("Cannot close '$manifestPath'");
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new( file_name => $plexPath,
                                                     quiet => 1);
    my %plexSNPs = ();
    foreach my $name ($snpset->snp_names) { $plexSNPs{$name} = 1; }
    my @shared;
    foreach my $name (@manifest) {
        $name = convertFromIlluminaExomeSNP($name);
	if ($plexSNPs{$name}) { push(@shared, $name); }
    }
    return @shared;
}



__END__

=head1 NAME

manifest_plex_intersection

=head1 SYNOPSIS

manifest_plex_intersection --manifest <path> --plex <path> [--out <path>]
 [--quiet] [--help]

Options:

  --manifest    Path to a .bpm.csv SNP manifest file. Required.
  --plex        Path to a .tsv file containing the QC plex SNP manifest.
                Requried.
  --out         Path to a text output file containing the names of all
                intersecting SNPs. Optional.
  --verbose     Print total number of intersecting SNPS to STDOUT. Optional.
  --help        Display this help and exit

=head1 DESCRIPTION

Convenience script to find the intersection of SNP sets between a .bpm.csv
manifest and a QC plex.

=head1 METHODS

None

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2014, 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
