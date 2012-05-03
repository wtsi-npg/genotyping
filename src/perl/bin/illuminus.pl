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
use JSON;
use POSIX qw(mkfifo);
use Pod::Usage;

use WTSI::Genotyping qw(maybe_stdout read_sample_json write_gt_calls);

$|=1;

my $chromosome;
my $end;
my $input;
my $executable;
my $output;
my $plink;
my $samples;
my $start;
my $verbose;
my $whole_genome_amplified;

GetOptions('chr=s' => \$chromosome,
           'end=i' => \$end,
           'help' => sub { pod2usage(-verbose => 2, -exitval => 0) },
           'input=s' => \$input,
           'output=s' => \$output,
           'plink' => \$plink,
           'samples=s' => \$samples,
           'start=i' => \$start,
           'verbose' => \$verbose,
           'wga' => \$whole_genome_amplified);

unless ($samples) {
  pod2usage(-msg => "A --samples argument is required\n",
            -exitval => 2);
}

unless (defined $chromosome) {
  pod2usage(-msg => "A --chr argument is required\n",
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

if (defined $plink && !defined $output) {
  pod2usage(-msg => "An --output argument must be given if --plink is specified",
            -exitval => 2);
}

$chromosome = uc($chromosome);
$executable = 'illuminus';
$input ||= '/dev/stdin';

# Sample information
my @samples = read_sample_json($samples);

# These are what Illuminus will call its output files
my $tmp_dir = tempdir(CLEANUP => 1);
my $gender_file = $tmp_dir . '/' . 'gender_codes';

my @command = ($executable, '-in', $input);
if ($start && $end) {
  push(@command, '-s', $start, $end);
}

if ($chromosome eq 'X' || $chromosome eq 'Y' || $chromosome =~ /^M/) {
  write_gender_codes($gender_file, $chromosome, \@samples);
  push(@command, '-x', $gender_file);
}

if ($whole_genome_amplified) {
  push(@command, '-w');
}

if ($plink) {
  push(@command, '-b', '-out', $output);
  # Maybe muffle Illuminus' STDOUT chatter
  unless ($verbose) {
    push(@command, "> /dev/null");
  }

  my $command = join(" ", @command);
  system($command) == 0 or die "Failed to execute '$command'\n";
  exit(0);
}
else {
  my $out = maybe_stdout($output);
  # Construct output header
  my @column_names = map { $_->{'uri'} } @samples;
  write_gt_header($out, \@column_names);

  my $illuminus_out = catfile($tmp_dir, 'illuminus.' . $$);
  my $calls_fifo = make_fifo($illuminus_out . '_calls');
  my $probs_fifo = make_fifo($illuminus_out . '_probs');

  # Tell illuminus to write both calls and probabilities
  push(@command, '-c', '-p', '-out', $illuminus_out);

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
}


# Write the header line of the genotype call result
sub write_gt_header {
  my ($out, $column_names) = @_;

  foreach my $name (@$column_names) {
    print $out "\t$name";
  }
  print $out "\n";

  return $out;
}

sub write_gender_codes {
  my ($file, $chromosome, $samples) = @_;

  open(GENDERS, ">$file") or die "Failed to open '$file' for writing: $!\n";
  foreach my $sample (@$samples) {
    my $code = 0;
    if ($chromosome =~ /^M/) {
      $code = 1;
    } else {
      $code = $sample->{'gender_code'};
    }

    print GENDERS "$code\n";
  }
  close(GENDERS);

  return $file;
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

illuminus --chromsome X --samples <filename> \
  [--start <n>] [--end <m>] < intensities > genotypes

Options:

  --chr      The name of the chromsome being analysed. Required.
  --samples  A JSON file of sample annotation use to determine column
             names, corresponding to the order of the intensity pairs
             in the intensity file. The order is important because these
             names are used to annotate the columns in the genotype output
             file.
  --end      The 1-based index of the last SNP in the range to be
             analysed. Optional.
  --help     Display help.
  --input    The Illuminus intensity file to be read. Optional, defaults
             to STDIN.
  --output   The Illuminus genotype file to be written. Optional,
             defaults to STDOUT.
  --plink    Write Plink BED format output. Optional, requires --output to
             be a file.
  --start    The 1-based index of the first SNP in the range to be
             analysed. Optional.
  --wga      Assume that the sample is whole genome amplified.
  --verbose  Print messages while processing. Optional.

=head1 DESCRIPTION

The script wraps the Illuminus genotype caller to allow it to operate
via STDIN and STDOUT with a minimum of fuss. Gender information is taken
from gender_code fields in the sample JSON.

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

  0.3.0

=head1 CHANGELOG

  0.3.0

    Removed --gender option; genders are now handled internally.
    Added --chromsome option.
    Added --plink option to write Plink BED format.

  0.2.0

    Changed to read sample names from JSON input.

  0.1.0

    Initial version 0.1.0

=cut
