#!/software/bin/perl

use utf8;

package main;

use strict;
use warnings;
use Getopt::Long;
use Log::Log4perl qw(:easy);
use Pod::Usage;

use WTSI::NPG::Utilities::IO qw(maybe_stdin maybe_stdout);

use WTSI::NPG::Genotyping::Database::Pipeline;

our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";

Log::Log4perl->easy_init($ERROR);

run() unless caller();

sub run {
  my $add;
  my $config;
  my $dbfile;
  my $input;
  my $select;
  my $remove;
  my $output;
  my $verbose;

  GetOptions('add=s'    => \$add,
             'config=s' => \$config,
             'dbfile=s' => \$dbfile,
             'help'     => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'input=s'  => \$input,
             'select=s' => \$select,
             'remove=s' => \$remove,
             'output=s' => \$output,
             'verbose'  => \$verbose);

  $config ||= $DEFAULT_INI;

  if ($select && $add) {
    pod2usage(-msg => "The --select argument is incompatible with --add\n",
              -exitval => 2);
  }
  if ($select && $remove) {
    pod2usage(-msg => "The --select argument is incompatible with --remove\n",
              -exitval => 2);
  }
  if ($select && $input) {
    pod2usage(-msg => "The --select argument is incompatible with --input\n",
              -exitval => 2);
  }

  my @initargs = (name    => 'pipeline',
                  inifile => $config);
  if ($dbfile) {
    push @initargs, (dbfile => $dbfile);
  }

  my $pipedb = WTSI::NPG::Genotyping::Database::Pipeline->new
    (@initargs)->connect
      (RaiseError     => 1,
       sqlite_unicode => 1,
       on_connect_do  => 'PRAGMA foreign_keys = ON');

  my $in = maybe_stdin($input);
  my $out = maybe_stdout($output);

  if ($select) {
    foreach my $sample ($pipedb->sample->all) {
      my $state = $pipedb->state->find({name => $select});
      unless ($state) {
        die "Failed to select sample state '$select': invalid state\n";
      }

      if (grep { $state->name eq $_->name } $sample->states) {
        print $out $sample->name, "\n";
        describe_sample($sample, \*STDERR) if $verbose;
      }
    }
  }
  elsif ($add or $remove) {
    $pipedb->in_transaction
      (sub {
         while (my $name = <$in>) {
           chomp($name);
           my $sample = $pipedb->sample->find({name => $name});

           unless ($sample) {
             warn "Failed to find $name\n";
             next;
           }

           if ($remove) {
             my $state = $pipedb->state->find({name => $remove});
             unless ($state) {
               die "Failed to remove sample state '$remove': invalid state\n";
             }

             if (grep { $state->name eq $_->name } $sample->states) {
               $sample->remove_from_states($state);
             }
           }

           if ($add) {
             my $state = $pipedb->state->find({name => $add});
             unless ($state) {
               die "Failed to add sample state '$add': invalid state\n";
             }

             unless (grep { $state->name eq $_->name } $sample->states) {
               $sample->add_to_states($state);
             }
           }

           $sample->include_from_state;
           $sample->update;

           print $out $sample->name, "\n";

           describe_sample($sample, \*STDERR) if $verbose;
         }
       });
  }
  else {
    foreach my $sample ($pipedb->sample->all) {
      print $out $sample->name, "\n";
      describe_sample($sample, \*STDERR) if $verbose;
    }
  }

  return;
}

sub describe_sample {
  my ($sample, $where) = @_;
  print $where $sample->include ? '+' : '-';
  print $where ' ';
  print $where join(' ', $sample->name,
                    map { $_->name } $sample->states), "\n";

  return;
}


__END__

=head1 NAME

ready_samples

=head1 SYNOPSIS

ready_samples [--config <database .ini file>]
    [--dbfile <SQLite file>] \
    [--input <filename>] [--output <filename>] \
    [--select <name>] [--add <name>] [--remove <name>] [--verbose]

Options:

  --config    Load database configuration from a user-defined .ini file.
              Optional, defaults to $HOME/.npg/genotyping.ini
  --dbfile    The SQLite database file. If not supplied, defaults to the
              value given in the configuration .ini file.
  --input     The sample name input file, one sample name per line.
              Optional, defaults to STDIN.
  --output    The sample name output file, one sample name per line.
              Optional, defaults to STDOUT.
  --add       The sample state term to add to the named samples.
              Optional.
  --remove    The sample state term to remove from the named samples.
              Optional.
  --select    The sample state term to select samples when no input names
              are provided. Optional, incompatible with --input, --add and
              --remove.
  --verbose   Print messages while processing. Optional.

=head1 DESCRIPTION

This program adjusts the state of sample records in the pipeline
database prior to running an analysis. It accepts the names of samples
from a file or STDIN and writes the same names to a file or STDOUT.

The optional --add and --remove arguments may be used to add and
remove single states from the named samples (see etc/states.ini). Both
add and remove may be applied in the same operation, the remove
operation being applied before the add operation.

Once the samples' state has been changed, it will automatically be
flagged for inclusion or exclusion fromn the analysis as
appropriate. In verbose mode, the inclusion or exclusion status, and
all current states are reported on STDERR for each sample.

The --select argument used with a state causes the names of matching
samples to be printed to the output.

If no arguments are given, the names of all samples are printed to the
output.

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
