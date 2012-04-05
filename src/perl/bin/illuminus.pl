#!/software/bin/perl

use utf8;

package main;

use strict;
use warnings;
use Cwd;
use File::Temp qw(tempdir);
use File::Spec::Functions qw(catfile);
use Getopt::Long;
use IO::ScalarArray;
use POSIX qw(mkfifo);
use Pod::Usage;

use WTSI::Genotyping qw(maybe_stdout read_fon write_gt_calls);

$|=1;

my $columns;
my $end;
my $genders;
my $input;
my $executable;
my $output;
my $start;
my $verbose;
my $whole_genome_amplified;

GetOptions('columns=s' => \$columns,
           'end=i' => \$end,
           'genders=s' => \$genders,
           'help' => sub { pod2usage(-verbose => 2, -exitval => 0) },
           'input=s' => \$input,
           'output=s' => \$output,
           'start=i' => \$start,
           'verbose' => \$verbose,
           'wga' => \$whole_genome_amplified);

unless ($columns) {
  pod2usage(-msg => "A --columns argument is required\n",
            -exitval => 2);
}

if (defined $start && !defined $end) {
  pod2usage(-msg => "An --end argument must be given if --start is specified",
            -exitval => 2);
}

if (!defined $start && defined $end) {
  pod2usage(-msg => "A --start argument must be given if --end is specified",
            -exitval => 2);
}

$executable = 'illuminus';
$input ||= '/dev/stdin';
my $out = maybe_stdout($output);

# Construct output header
my $column_names = read_value_list($columns);
write_gt_header($out, $column_names);

# These are what Illuminus will call its output files
my $fifo_dir = tempdir(CLEANUP => 1);
my $illuminus_out = catfile($fifo_dir, 'illuminus.' . $$);
my $calls_fifo = make_fifo($illuminus_out . '_calls');
my $probs_fifo = make_fifo($illuminus_out . '_probs');

# Tell illuminus to write both calls and probabilities
my @command = ($executable, '-c', '-p', '-in', $input, '-out', $illuminus_out);

if ($start && $end) {
  push(@command, '-s', $start, $end);
}

if ($genders) {
  check_genders(read_value_list($genders), $column_names);
  push(@command, '-x', $genders);
}

if ($whole_genome_amplified) {
  push(@command, '-w');
}

my $pid = fork();
if (! defined $pid) {
  die "Failed to fork: $!\n";
}
elsif ($pid) {
  my @calls;
  my @probs;

  # Illuminus writes all its calls, then all its probs, so we can't
  # interleave reads and make this a nice stream. We have to slurp all
  # of one, then the other.
  open(CALLS, "<$calls_fifo") or die "Failed to open '$calls_fifo': $!\n";
  while (my $line = <CALLS>) {
    push(@calls, $line);
  }
  close(CALLS) or warn "Failed to close FIFO $calls_fifo: $!\n";

  open(PROBS, "<$probs_fifo") or die "Failed to open '$probs_fifo': $!\n";
  while (my $line = <PROBS>) {
    push(@probs, $line);
  }
  close(PROBS) or warn "Failed to close FIFO $probs_fifo: $!\n";

  # write_gt_calls requires streams, so this is a shim to pretend that
  # we have such
  my $CALLS = new IO::ScalarArray(\@calls);
  my $PROBS = new IO::ScalarArray(\@probs);

  my $num_written = -1;
  while ($num_written != 0) {
    $num_written = write_gt_calls($CALLS, $PROBS, $out)
  }
}
else {
  # Maybe muffle Illuminus' STDOUT chatter
  unless ($verbose) {
    push(@command, "> /dev/null");
  }

  my $command = join(" ", @command);
  system($command) == 0 or die "Failed to execute '$command'\n";
  exit;
}

waitpid($pid, 0);

unlink($calls_fifo);
unlink($probs_fifo);

exit(0);

# Write the header line of the genotype call result
sub write_gt_header {
  my ($out, $column_names) = @_;

  foreach my $name (@$column_names) {
    print $out "\t$name";
  }
  print $out "\n";

  return $out;
}

# Check that gender values are valid and that the number of gender
# values equal the number of samples.
sub check_genders {
  my ($genders, $columns) = @_;
  my $num_genders = scalar @$genders;
  my $num_columns = scalar @$columns;

  unless ($num_genders == $num_columns) {
    die "Number of gender values ($num_genders) was not equal to the ".
      "number of columns ($num_columns)\n";
  }

  foreach my $gender (@$genders) {
    unless ($gender eq "0" || $gender eq "1") {
      die "Invalid gender value '$gender': expected one of [0, 1]\n";
    }
  }

  return 1;
}

sub read_value_list {
  my $filename = shift;
  my $values;

  open(FH, "<$filename")
    or die "Failed to open column file '$filename' for reading: $!\n";
  $values = read_fon(\*FH);
  close(FH);

  return $values;
}

sub make_fifo {
  my $filename = shift;

  mkfifo($filename, '0400') or die "Failed to create FIFO '$filename': $!\n";

  return $filename;
}


__END__

=head1 NAME

illuminus - run the Illuminus genotype caller

=head1 SYNOPSIS

illuminus --columns <filename> \
  [--start <n>] [--end <m>] < intensities > genotypes

Options:

  --columns  A text file of column names, one per line, corresponding
             to the order of the intensity pairs in the intensity
             file. The order is important because these names are used
             to annotate the columns in the genotype output file.
  --end      The 1-based index of the last SNP in the range to be
             analysed. Optional.
  --genders  File of gender codes corresponding to the samples being
             analysed.  The file must contain the same number of
             values as the column names file and in the same
             respective order. Optional.
  --help     Display help.
  --input    The Illuminus intensity file to be read. Optional, defaults
             to STDIN.
  --output   The Illuminus genotype file to be written. Optional,
             defaults to STDOUT.
  --start    The 1-based index of the first SNP in the range to be
             analysed. Optional.
  --wga      Assume that the sample is whole genome amplified.
  --verbose  Print messages while processing. Optional.

=head1 DESCRIPTION

The script wraps the Illuminus genotype caller to allow it to operate
via STDIN and STDOUT with a minimum of fuss. All preparation of sample
names and appropriate gender handling is left to the caller.

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

=head1 VERSION

  0.1.0

=head1 CHANGELOG

Thu Feb 16 17:05:03 GMT 2012 -- Initial version 0.1.0

=cut
