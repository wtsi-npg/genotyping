#!/software/bin/perl

use utf8;

package main;

use strict;
use warnings;
use Carp;
use Getopt::Long;
use Log::Log4perl qw(:levels);
use Pod::Usage;
use WTSI::DNAP::Utilities::ConfigureLogger qw/log_init/;

use WTSI::NPG::Utilities::DelimitedFiles qw(read_fon
                                            find_column_indices
                                            filter_columns);

use WTSI::NPG::Genotyping::Illuminus qw(read_gt_column_names
                                        filter_gt_columns);

use WTSI::NPG::Utilities qw(user_session_log);

my $uid = `whoami`;
chomp($uid);
my $session_log = user_session_log($uid, 'filter_illuminus_output');

our $VERSION = '';

run() unless caller();

sub run {
  my $columns;
  my $debug;
  my $gt_input;
  my $gt_output;
  my $log4perl_config;
  my $operation;
  my $pr_input;
  my $pr_output;
  my $verbose;

  GetOptions('columns=s'   => \$columns,
             'debug'       => \$debug,
             'gt-input=s'  => \$gt_input,
             'gt-output=s' => \$gt_output,
             'help'        => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'logconf=s'   => \$log4perl_config,
             'operation=s' => \$operation,
             'pr-input=s'  => \$pr_input,
             'pr-output=s' => \$pr_output,
             'verbose'     => \$verbose);

  unless ($gt_input) {
    pod2usage(-msg => "A --gt-input argument is required\n",
              -exitval => 2);
  }
  unless ($gt_output) {
    pod2usage(-msg => "A --gt-output argument is required\n",
              -exitval => 2);
  }
  unless ($pr_input) {
   pod2usage(-msg => "A --pr-input argument is required\n",
             -exitval => 2);
  }
  unless ($pr_output) {
    pod2usage(-msg => "A --pr-output argument is required\n",
              -exitval => 2);
  }
  unless ($columns) {
    pod2usage(-msg => "A --columns argument is required\n",
              -exitval => 2);
  }

  my @log_levels;
  if ($debug) { push @log_levels, $DEBUG; }
  if ($verbose) { push @log_levels, $INFO; }
  log_init(config => $log4perl_config,
           file   => $session_log,
           levels => \@log_levels);
  my $log = Log::Log4perl->get_logger('main');

  my $gt_offset = 1; # 1 leading column in genotype files
  my $pr_offset = 3; # 3 leading columns in probability files

  my $gt_separator = "\t"; # Different column separators for genotype
  my $pr_separator = ' ';  # and probability files. Yes, really.
  my $gt_col_group = 1; # Genotype data has one column per sample
  my $pr_col_group = 4; # Probability data comes in groups of 4
                        # columns per sample

  $operation ||= 'include';
  $operation = lc($operation);

  unless ($operation eq 'include' || $operation eq 'exclude') {
    pod2usage(-msg => "Invalid operation '$operation', expected one of " .
              "[include exclude]\n",
              -exitval => 2);
  }

  open(my $col, '<', "$columns")
    or $log->logcroak("Failed to open column file '", $columns,
                      "' for reading: $!");
  my $column_names = read_fon($col);
  close($col) or $log->logwarn("Failed to close column file '",
                               $columns, "': $!");

  open(my $gti, '<', "$gt_input")
    or $log->logcroak("Failed to open genotype file '", $gt_input,
                      "' for reading: $!");
  open(my $gto, '>', "$gt_output")
    or $log->logcroak("Failed to open genotype file '", $gt_output,
                      "' for writing: $!");

  my $headers = read_gt_column_names($gti);
  my $cols_to_use = find_column_indices($column_names, $headers);

  print $gto $gt_separator,
    join($gt_separator, filter_columns($headers, $cols_to_use, $operation)), "\n";

  my $num_genotypes =
    filter_gt_columns($gti, $gto, $gt_separator, $gt_offset, $gt_col_group,
                      $cols_to_use, $operation);

  close($gto) or $log->logwarn("Failed to close genotype file '",
                               $gt_output, "': $!");
  close($gti) or $log->logwarn("Failed to close genotype file '",
                               $gt_input, "': $!");

  open(my $pri, '<', "$pr_input")
    or $log->logcroak("Failed to open probability file '",
                      $pr_input, "' for reading: $!");
  open(my $pro, '>', ">pr_output")
    or $log->logcroak("Failed to open probability file '",
                      $pr_output, "' for writing: $!");

  my $num_probs =
    filter_gt_columns($pri, $pro, $pr_separator, $pr_offset, $pr_col_group,
                      $cols_to_use, $operation);
  close($pro) or $log->logwarn("Failed to close probability file '",
                               $pr_output, "': $!");
  close($pri) or $log->logwarn("Failed to close probability file '",
                               $pr_input, "': $!");

  unless ($num_genotypes == $num_probs) {
      $log->logcroak("Number of SNP genotype records (", $num_genotypes,
                     ") in '", $gt_input, "' was not equal to the number ",
                     "of SNP probability records (", $num_probs,
                     ") in '", $pr_input, "'");
  }

  my $verb = $operation . "d";
  my $num_cols = scalar @$cols_to_use;
  $log->info("$verb $num_cols columns from $num_genotypes records");

  return;
}


__END__

=head1 NAME

filter_illuminus_output.pl -- Filter Illuminus (Sanger version) output
by column.

=head1 SYNOPSIS

filter_illuminus_output --columns <column name file> \
   [--operation include|exclude] \
   --gt-input <genotype input file> --gt-output <genotype output file> \
   --pr-input <probability input file> --pr-output <probability output file> \
   [--verbose]

Options:

  --columns   A text file of column names, one per line, corresponding to
              column names in the genotype result file. The order of the
              lines is not meaningful.
  --gt-input  The Illuminus genotype result file to be filtered.
  --gt-output The Illuminus genotype result file to be written.
  --help      Display help.
  --operation The operation to carry out on the selected columns. The value
              maye be either 'include' or 'exclude' (case-insensitive).
              Optional, defaults to 'include'.
  --pr-input  The Illuminus probability result file to be filtered.
  --pr-output The Illuminus probability result file to be written.
  --verbose   Print messages while processing. Optional.

=head1 DESCRIPTION

Filters Illuminus (Sanger version) output by column, writing the
results to new files. Columns to include or exclude are specified via
a text file of column names, one per line, corresponding to column
names in the input genotype result file.

=head1 METHODS

None

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>, Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2011, 2012, 2015, 2016 Genome Research Limited. All Rights
Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
