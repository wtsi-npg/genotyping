#!/software/bin/perl

use utf8;

package main;

use strict;
use warnings;
use Carp;
use Getopt::Long;
use Pod::Usage;

use WTSI::NPG::Genotyping::QC::Identity qw /getIntersectingSNPsManifest $PLEX_FILE/;

my ($manifestPath, $outPath, $quiet);

GetOptions('manifest=s' => \$manifestPath,
	   'out=s'      => \$outPath,
           'quiet'      => \$quiet,
           'help'       => sub { pod2usage(-verbose => 2, -exitval => 0) },);

# TODO Add an option to choose between Sequenom and Fluidigm QC plexes

if (!($manifestPath)) {
    croak("Must supply a --manifest argument");
} elsif (!(-e $manifestPath)) {
    croak("Manifest path '$manifestPath' does not exist");
}
my @shared = getIntersectingSNPsManifest($manifestPath);
if (!($quiet)) {
    print "Comparing manifest to QC plex $PLEX_FILE\n";
    my $total = @shared;
    print "$total shared SNPs found\n";
}
if ($outPath) {
    open my $out, ">", $outPath || croak "Cannot open output '$outPath'";
    foreach my $snp (@shared) { print $out "$snp\n"; }
    close $out || croak "Cannot close output '$outPath'";
}

__END__

=head1 NAME

manifest_plex_intersection

=head1 SYNOPSIS

manifest_plex_intersection --manifest <path> [--out <path>] [--quiet] [--help]

Options:

  --manifest    Path to a .bpm.csv SNP manifest file. Required.
  --out         Path to a text output file containing the names of all
                intersecting SNPs. Optional.
  --quiet       Do not print total number of intersecting SNPs to STDOUT.
  --help        Display this help and exit

=head1 DESCRIPTION

Convenience script to find the intersection of SNP sets between a .bpm.csv
manifest and a QC plex, and print to standard output.

=head1 METHODS

None

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2014 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
