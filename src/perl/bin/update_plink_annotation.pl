#!/software/bin/perl

use utf8;

package main;

use strict;
use warnings;
use File::Temp qw(tempdir tempfile);
use Getopt::Long;
use Log::Log4perl qw(:levels);
use Pod::Usage;

use WTSI::DNAP::Utilities::ConfigureLogger qw(log_init);
use WTSI::NPG::Genotyping::Plink qw(update_placeholder
                                    update_snp_locations
                                    update_sample_genders);
use WTSI::NPG::Utilities qw(user_session_log);

my $uid = `whoami`;
chomp($uid);
my $session_log = user_session_log($uid, 'update_plink_annotation');

our $VERSION = '';

Log::Log4perl->easy_init($ERROR);

run() unless caller();

sub run {
  my $bed_file;
  my $debug;
  my $log4perl_config;
  my $sample_json;
  my $snp_json;
  my $placeholder;
  my $verbose;

  GetOptions('bed=s'         => \$bed_file,
             'debug'         => \$debug,
             'help' => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'logconf=s'     => \$log4perl_config,
             'samples=s'     => \$sample_json,
             'snps=s'        => \$snp_json,
	     'placeholder=i' => \$placeholder,
             'verbose'       => \$verbose);

  unless ($bed_file) {
    pod2usage(-msg => "A --bed argument is required\n",
              -exitval => 2);
  }

  unless ($sample_json or $snp_json or $placeholder) {
    pod2usage(-msg => "At least one of --samples, --snps, or --placeholder is required\n",
              -exitval => 2);
  }

  my @log_levels;
  if ($debug) { push @log_levels, $DEBUG; }
  if ($verbose) { push @log_levels, $INFO; }
  log_init(config => $log4perl_config,
           file   => $session_log,
           levels => \@log_levels);
  my $log = Log::Log4perl->get_logger('main');

  # placeholder for missing data in .fam files must be 0 or -9
  if (defined($placeholder) && $placeholder!=0 && $placeholder!=-9) {
      pod2usage(-msg => "--placeholder argument must be one of (0, -9)\n",
		-exitval => 2);
  }

  my $tmp_dir = tempdir(CLEANUP => 1);
  if ($sample_json) {
    my $num_updated = update_sample_genders($bed_file, $bed_file,
                                            $sample_json, $tmp_dir);
    $log->info("Updated the gender of ", $num_updated, " samples");
  }

  if ($snp_json) {
    my $num_updated = update_snp_locations($bed_file, $bed_file,
                                           $snp_json, $tmp_dir);
    $log->info("Updated the location of ", $num_updated, " SNPs");
  }

  if (defined($placeholder)) {
    my $num_updated = update_placeholder($bed_file, $bed_file, 
                                         $placeholder, $tmp_dir);
    $log->info("Updated placeholders for ", $num_updated, " samples");
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
  --logconf  A log4perl configuration file. Optional.
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

Keith James <kdj@sanger.ac.uk>, Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2012, 2013, 2014, 2015, 2016 Genome Research Limited.
All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
