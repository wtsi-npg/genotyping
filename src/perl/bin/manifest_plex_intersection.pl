#!/software/bin/perl

use utf8;

package main;

use strict;
use warnings;
use Carp;
use Getopt::Long;
use Log::Log4perl::Level;
use Pod::Usage;

use WTSI::NPG::Genotyping::QC::SnpID qw(convertFromIlluminaExomeSNP);
use WTSI::NPG::Genotyping::SNPSet;

my ($manifestPath, $plexPath, $outPath, $quiet, $is_sequenom);

GetOptions('manifest=s'   => \$manifestPath,
           'plex=s'       => \$plexPath,
           'is_sequenom'  => \$is_sequenom,
	   'out=s'        => \$outPath,
           'quiet'        => \$quiet,
           'help'         => sub { pod2usage(-verbose => 2, -exitval => 0) },);

my $DEFAULT_PLEX_FILE = '/nfs/srpipe_references/genotypes/W30467_snp_set_info_1000Genomes.tsv';

$plexPath ||= $DEFAULT_PLEX_FILE;
if (!(-e $manifestPath)) {
    croak("QC plex path '$plexPath' does not exist");
}

if (!($manifestPath)) {
    croak("Must supply a --manifest argument");
} elsif (!(-e $manifestPath)) {
    croak("Manifest path '$manifestPath' does not exist");
}

my @shared = getIntersectingSNPsManifest($manifestPath, $plexPath);
if (!($quiet)) {
    print "Comparing manifest to QC plex $DEFAULT_PLEX_FILE\n";
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
	if (/^Index/) { next; } # skip header line
	chomp;
	my @words = split(/,/);
	push(@manifest, $words[1]);
    }
    close $in || croak("Cannot close '$manifestPath'");
    return getPlexIntersection($plexPath, \@manifest);
}

# duplication of code from Identity.pm
# TODO replace with instantiation of a simplified base class

sub getPlexIntersection {
    my ($plexPath, $compareRef) = @_;
    my @compare = @{$compareRef};
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($plexPath);
    $snpset->logger->level($WARN);
    my %plexSNPs = ();
    foreach my $name ($snpset->snp_names) { $plexSNPs{$name} = 1; } 
    my @shared;
    foreach my $name (@compare) {
        $name = convertFromIlluminaExomeSNP($name);
	if ($plexSNPs{$name}) { push(@shared, $name); }
    }
    return @shared;
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
