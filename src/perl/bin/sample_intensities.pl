#!/software/bin/perl

use utf8;

package main;

use warnings;
use strict;
use Getopt::Long;
use JSON;
use Log::Log4perl qw(:easy);
use Pod::Usage;

use WTSI::Genotyping::Database::Pipeline;
use WTSI::Genotyping qw(maybe_stdout);

our $WTSI_NAMESPACE = 'wtsi';
our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";
our $ID_REGEX = qr/^[A-Za-z0-9-._]{4,}$/msx;

Log::Log4perl->easy_init($ERROR);

run() unless caller();

sub run {
  my $all;
  my $config;
  my $dbfile;
  my $gender_method;
  my $output;
  my $run_name;
  my $verbose;

  GetOptions('all' => \$all,
             'config=s' => \$config,
             'dbfile=s'=> \$dbfile,
             'gender_method=s' => \$gender_method,
             'help' => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'output=s' => \$output,
             'run=s' => \$run_name,
             'verbose' => \$verbose);

  $config ||= $DEFAULT_INI;
  $gender_method ||= 'Supplied';

  unless ($run_name) {
    pod2usage(-msg => "A --run argument is required\n", -exitval => 2);
  }

  my $pipedb = WTSI::Genotyping::Database::Pipeline->new
    (name => 'pipeline',
     inifile => $config,
     dbfile => $dbfile)->connect
       (RaiseError => 1,
        on_connect_do => 'PRAGMA foreign_keys = ON');

  my $run = $pipedb->piperun->find({name => $run_name});
  unless ($run) {
    die "Run '$run_name' does not exist. Valid runs are: [" .
      join(", ", map { $_->name } $pipedb->piperun->all) . "]\n";
  }

  my $where = {'piperun.name' => $run->name,
               'method.name' => 'Autocall'};
  unless ($all) {
    $where->{'me.include'} = 1;
  }

  my @samples;
  foreach my $sample ($pipedb->sample->search($where,
                                              {join => [{dataset => 'piperun'},
                                                        {results => 'method'}],
                                               order_by => 'me.id_sample'})) {
    my $gender = $pipedb->gender->find
      ({'sample.id_sample' => $sample->id_sample,
        'method.name' => $gender_method},
       {join => {'sample_genders' => ['method', 'sample']}},
       {prefetch =>  {'sample_genders' => ['method', 'sample']} });

    my $gender_name = defined $gender ? $gender->name : undef;
    my $gender_code = defined $gender ? $gender->code : undef;

    push @samples, {sanger_sample_id => $sample->sanger_sample_id,
                    uri => $sample->uri->as_string,
                    result => $sample->gtc,
                    gender =>  $gender_name,
                    gender_code => $gender_code};
  }

  my $fh = maybe_stdout($output);
  print $fh to_json(\@samples, {utf8 => 1, pretty => 1});
  close($fh);

  return;
}


__END__

=head1 NAME

sample_intensities

=head1 SYNOPSIS

sample_intensities [--config <database .ini file>] [--dbfile <SQLite file>] \
   [--output <JSON file>] --run <analysis run name> [--verbose]

Options:

  --all       Include all samples in output, even those marked as not for
              analysis.
  --config    Load database configuration from a user-defined .ini file.
              Optional, defaults to $HOME/.npg/genotyping.ini
  --dbfile    The SQLite database file. If not supplied, defaults to the
              value given in the configuration .ini file.
  --help      Display help.
  --output    The file to which output will be written. Optional, defaults
              to STDOUT.
  --run       The name of a pipe run defined previously using the
              ready_infinium script.
  --verbose   Print messages while processing. Optional.

=head1 DESCRIPTION

Writes JSON to STDOUT describing the sample GTC format data for a
pipeline run; a JSON array with one element per sample, in sample
order. Each element of the array describes one sample as a JSON object:

  {
     "sanger_sample_id" : <WTSI sample identifier>,
     "result" : <absolute path to the GTC format data file>,
     "uri" : <sample URI>,
     "gender" : <gender name string>,
     "gender_code" : <gender code integer>
  }

Only records for samples marked in the pipeline database for inclusion
(in analyses) will be printed.

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

  0.1.1

=head1 CHANGELOG

0.1.1

  Added --output command line option.

0.1.0

  Initial version 0.1.0

=cut
