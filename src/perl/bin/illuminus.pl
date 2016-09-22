#!/software/bin/perl

use utf8;

package main;

use strict;
use warnings;
use Cwd;
use File::Basename;
use File::Copy;
use File::Spec::Functions qw(catfile);
use File::Temp qw(tempdir);
use Getopt::Long;
use IO::ScalarArray;
use Log::Log4perl qw(:easy);
use POSIX qw(mkfifo);
use Pod::Usage;

use WTSI::DNAP::Utilities::IO qw(maybe_stdin  maybe_stdout);
use WTSI::NPG::Genotyping qw(read_sample_json);
use WTSI::NPG::Genotyping::Illuminus qw(nullify_females
                                        read_it_column_names
                                        update_it_columns
                                        write_gt_calls
                                        write_it_header);
use WTSI::NPG::Utilities qw(user_session_log);

my $uid = `whoami`;
chomp($uid);
my $session_log = user_session_log($uid, 'perl_illuminus_wrap');
my $log;

our $VERSION = '';

my $chromosome;
my $debug;
my $end;
my $executable;
my $input;
my $log4perl_config;
my $output;
my $plink;
my $samples;
my $start;
my $verbose;
my $whole_genome_amplified;

GetOptions('chr=s'     => \$chromosome,
           'debug'     => \$debug,
           'end=i'     => \$end,
           'help'      => sub { pod2usage(-verbose => 2, -exitval => 0) },
           'input=s'   => \$input,
           'logconf=s' => \$log4perl_config,
           'output=s'  => \$output,
           'plink'     => \$plink,
           'samples=s' => \$samples,
           'start=i'   => \$start,
           'verbose'   => \$verbose,
           'wga'       => \$whole_genome_amplified);

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

if ($log4perl_config) {
    Log::Log4perl::init($log4perl_config);
} else {
    my $level;
    if ($debug) { $level = $DEBUG; }
    elsif ($verbose) { $level = $INFO; }
    else { $level = $ERROR; }
    my @log_args = ({layout => '%d %p %m %n',
                     level  => $level,
                     file   => ">>$session_log",
                     utf8   => 1},
                    {layout => '%d %p %m %n',
                     level  => $level,
                     file   => "STDERR",
                     utf8   => 1},
                );
    Log::Log4perl->easy_init(@log_args);
}
$log = Log::Log4perl->get_logger('main');

$chromosome = uc($chromosome);
$executable = 'illuminus';

# Sample information
my @samples = read_sample_json($samples);

# These are what Illuminus will call its output files
my $tmp_dir = tempdir(CLEANUP => 1);
my $gender_file = $tmp_dir . '/' . 'gender_codes';

my @command = ($executable, '-in /dev/stdin');
if ($start && $end) {
  push(@command, '-s', $start, $end);
}

if ($chromosome eq 'X' || $chromosome eq 'Y' || $chromosome =~ m{^M}msx) {
  write_gender_codes($gender_file, $chromosome, \@samples);
  push(@command, '-x', $gender_file);
}

if ($whole_genome_amplified) {
  push(@command, '-w');
}

if ($plink) {
  push(@command, '-b', '-out', $output);
  # Maybe muffle Illuminus' STDOUT chatter
  unless ($verbose || $debug) {
    push(@command, "> /dev/null");
  }

  my $command = join(" ", @command);
  $log->info("Executing '", $command, "'");

  if ($chromosome eq 'Y') {
    nullify_females($input, $command, \@samples);
  }
  else {
    system($command) && $log->logcroak("Failed to execute '", $command, "'");
  }

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
    $log->logcroak("Failed to fork");
  }
  elsif ($pid) {
    my @calls;
    my @probs;
    local $|=1;

    # Illuminus writes all its calls, then all its probs, so we can't
    # interleave reads and make this a nice stream. We have to slurp all
    # of one, then the other.
    open(my $calls, '<', "$calls_fifo")
      or $log->logcroak("Failed to open FIFO '", $calls_fifo, "'");
    while (my $line = <$calls>) {
      push(@calls, $line);
    }
    close($calls) or $log->logwarn("Failed to close FIFO '",
                                   $calls_fifo, "'");

    open(my $probs, '<', "$probs_fifo")
      or $log->logcroak("Failed to open '", $probs_fifo, "'");
    while (my $line = <$probs>) {
      push(@probs, $line);
    }
    close($probs) or $log->logwarn("Failed to close FIFO '",
                                   $probs_fifo, "'");

    # write_gt_calls requires streams, so this is a shim to pretend that
    # we have such
    my $CALLS = IO::ScalarArray->new(\@calls);
    my $PROBS = IO::ScalarArray->new(\@probs);

    my $num_written = -1;
    while ($num_written != 0) {
      $num_written = write_gt_calls($CALLS, $PROBS, $out)
    }
  }
  else {
    # Maybe muffle Illuminus' STDOUT chatter
    unless ($verbose || $debug) {
      push(@command, "> /dev/null");
    }

    my $command = join(" ", @command);
    $log->info("Executing '", $command, "'");

    if ($chromosome eq 'Y') {
      nullify_females($input, $command, \@samples, $verbose);
    }
    else {
      system($command) && $log->logcroak("Failed to execute '",
                                         $command, "'");
    }

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

  open(my $genders, '>', "$file")
    or $log->logcroak("Failed to open '", $file, "' for writing");
  foreach my $sample (@$samples) {
    my $code = 0;
    if ($chromosome =~ m{^M}msx) {
      $code = 1;
    } else {
      $code = $sample->{'gender_code'};
    }
    unless (defined $code) {
      my $uri = $sample->{uri};
      $log->logcroak("Failed to find a gender code for sample ", $uri,
                     " in '", $file, "'");
    }

    print $genders "$code\n";
  }
  close($genders) or $log->logwarn("Failed to close gender code file '",
                                   $file, "'");

  return $file;
}

sub make_fifo {
  my $filename = shift;

  mkfifo($filename, '0400') or $log->logcroak("Failed to create FIFO '",
                                              $filename, "'");

  return $filename;
}

__END__

=head1 NAME

illuminus - run the Illuminus genotype caller

=head1 SYNOPSIS

illuminus --chr X --samples <filename> [--start <n>] [--end <m>] \
  [--plink] < intensities > genotypes

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

Keith James <kdj@sanger.ac.uk>, Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2012, 2015, 2016 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
