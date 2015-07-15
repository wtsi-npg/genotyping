#!/software/bin/perl

use utf8;

package main;

use strict;
use warnings;
use File::Temp qw(tempdir tempfile);
use Getopt::Long;
use Log::Log4perl qw(:easy);
use Pod::Usage;

use WTSI::NPG::Genotyping::Plink qw(update_placeholder 
                                    update_snp_locations
                                    update_sample_genders);

our $VERSION = '';

Log::Log4perl->easy_init($ERROR);

run() unless caller();

sub run {
  my $bed_file;
  my $sample_json;
  my $snp_json;
  my $placeholder; 
  my $verbose;

  GetOptions('bed=s' => \$bed_file,
             'help' => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'samples=s' => \$sample_json,
             'snps=s' => \$snp_json,
	     'placeholder=i' => \$placeholder,
             'verbose' => \$verbose);

  unless ($bed_file) {
    pod2usage(-msg => "A --bed argument is required\n",
              -exitval => 2);
  }

  unless ($sample_json or $snp_json or $placeholder) {
    pod2usage(-msg => "At least one of --samples, --snps, or --placeholder is required\n",
              -exitval => 2);
  }

  # placeholder for missing data in .fam files must be 0 or -9
  if (defined($placeholder) && $placeholder!=0 && $placeholder!=-9) {
      pod2usage(-msg => "--placeholder argument must be one of (0, -9)\n",
		-exitval => 2);
  }

  my $tmp_dir = tempdir(CLEANUP => 1);
  if ($sample_json) {
    my $num_updated = update_sample_genders($bed_file, $bed_file,
                                            $sample_json, $tmp_dir);
    print STDERR "Updated the gender of $num_updated samples\n" if $verbose;
  }

  if ($snp_json) {
    my $num_updated = update_snp_locations($bed_file, $bed_file,
                                           $snp_json, $tmp_dir);
    print STDERR "Updated the location of $num_updated SNPs\n" if $verbose;
  }

  if (defined($placeholder)) {
    my $num_updated = update_placeholder($bed_file, $bed_file, 
                                         $placeholder, $tmp_dir);
    print STDERR "Updated placeholders for $num_updated samples\n" if $verbose;
  }

  return;
}

__END__

=head1 NAME

update_plink_annotation - Modify SNP locations and sample genders in
Plink BIM and FAM annotation files.

=head1 SYNOPSIS

update_plink_annotation --samples <filename> --snps <filename>

Options:

  --bed      The file name of the Plink data to me modified. This should be
             the name of the BED file whose corresponding BIM and/or FAM
             annotation files are to be updated.
  --samples  A JSON file of sample annotation containing the new gender
             codes.
  --snps     A JSON file of SNP annotation containing the new chromosome
             names and positions.
  --help     Display help.
  --verbose  Print messages while processing. Optional.

=head1 DESCRIPTION

This script updates the BIM and/or FAM files that correspond to a
specified Plink BED file. SNP chromosomes and positions and sample
genders will be updated from values taken from JSON files. The SNP JSON
file is produced by genotype-call and the sample JSON file by the
sample_intensities script.

=head1 METHODS

None

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2012 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
