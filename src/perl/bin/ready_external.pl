#!/software/bin/perl

use utf8;

package main;

use warnings;
use strict;
use File::Basename;
use Getopt::Long;
use Log::Log4perl qw(:easy);
use Pod::Usage;

use WTSI::Genotyping qw(maybe_stdin maybe_stdout common_stem);
use WTSI::Genotyping::Database::Pipeline;

our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";
our $ID_REGEX = qr/^[A-Za-z0-9-._]{4,}$/;

Log::Log4perl->easy_init($ERROR);

run() unless caller();

sub run {
  my $chip_design;
  my $config;
  my $dbfile;
  my $input;
  my $maximum;
  my $namespace;
  my $run_name;
  my $supplier_name;
  my $verbose;

  GetOptions('chip-design=s' => \$chip_design,
             'config=s' => \$config,
             'dbfile=s'=> \$dbfile,
             'help' => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'input=s' => \$input,
             'run=s' => \$run_name,
             'namespace=s' => \$namespace,
             'supplier=s' => \$supplier_name,
             'verbose' => \$verbose);

  $config ||= $DEFAULT_INI;

  unless ($chip_design) {
    pod2usage(-msg => "A --chip-design argument is required\n", -exitval => 2);
  }
  unless ($namespace) {
    pod2usage(-msg => "A --namespace argument is required\n", -exitval => 2);
  }
  unless ($run_name) {
    pod2usage(-msg => "A --run argument is required\n", -exitval => 2);
  }
  unless ($supplier_name) {
    pod2usage(-msg => "A --supplier argument is required\n", -exitval => 2);
  }
  unless ($run_name =~ $ID_REGEX) {
    pod2usage(-msg => "Invalid run name '$run_name'\n", -exitval => 2);
  }
  unless ($namespace =~ $ID_REGEX) {
    pod2usage(-msg => "Invalid namespace '$namespace'\n", -exitval => 2);
  }
  if ($verbose) {
    my $db = $dbfile;
    $db ||= 'configured database';
    print STDERR "Updating $db using config from $config\n";
  }

  my $pipedb = WTSI::Genotyping::Database::Pipeline->new
    (name => 'pipeline',
     inifile => $config,
     dbfile => $dbfile)->connect
       (RaiseError => 1,
        on_connect_do => 'PRAGMA foreign_keys = ON');

  my @valid_designs = map { $_->name } $pipedb->snpset->all;
  unless (grep { $chip_design eq $_ } @valid_designs ) {
    die "Invalid chip design '$chip_design'. Valid designs are: [" .
      join(", ", @valid_designs) . "]\n";
  }

  my $snpset = $pipedb->snpset->find({name => $chip_design});
  my $infinium = $pipedb->method->find({name => 'Infinium'});
  my $autocall = $pipedb->method->find({name => 'Autocall'});
  my $supplied = $pipedb->method->find({name => 'Supplied'});
  my $pi_approved = $pipedb->state->find({name => 'pi_approved'});
  my $idat_unavailable = $pipedb->state->find({name => 'idat_unavailable'});
  my $gtc_unavailable = $pipedb->state->find({name => 'gtc_unavailable'});
  my $gender_na = $pipedb->gender->find({name => 'Not Available'});

  my $in = maybe_stdin($input);
  my @ex_samples = parse_manifest($in);
  close($in) or warn "Failed to close '$input'\n";

  $pipedb->in_transaction
    (sub {
       my $supplier = $pipedb->datasupplier->find_or_create
         ({name => $supplier_name,
           namespace => $namespace});
       my $run = $pipedb->piperun->find_or_create({name => $run_name});
       validate_snpset($run, $snpset);

       my $dataset = $run->add_to_datasets({datasupplier => $supplier,
                                            snpset => $snpset});

       print_pre_report($supplier, $namespace, $snpset) if $verbose;

     SAMPLE: foreach my $ex_sample (@ex_samples) {
         my $grn_path = $ex_sample->{idat_grn_path};
         my $red_path = $ex_sample->{idat_red_path};
         my $gtc_path = $ex_sample->{gtc_path};
         my $ex_name = $ex_sample->{sample};
         my $supplied_gender = $ex_sample->{gender};
         my $ex_chip = $ex_sample->{beadchip};

         my $gender = $pipedb->gender->find({name => $supplied_gender})
           || $gender_na;

         my $sample = $dataset->add_to_samples({name => $ex_name,
                                                beadchip => $ex_chip,
                                                include => 1});
         $sample->add_to_genders($gender, {method => $supplied});
         $sample->add_to_states($pi_approved);

         $sample->add_to_results({method => $autocall,
                                  value => $gtc_path});

         if ($grn_path) {
           $sample->add_to_results({method => $infinium,
                                    value => $grn_path});
         }
         if ($red_path) {
           $sample->add_to_results({method => $infinium,
                                    value => $red_path});
         }

         unless (-e $gtc_path) {
           $sample->add_to_states($gtc_unavailable);
         }
         unless (-e $grn_path and -e $red_path) {
           $sample->add_to_states($idat_unavailable);
         }

         $sample->include_from_state;
       }
     });

  print_post_report(\@ex_samples) if $verbose;

  return;
}

sub parse_manifest {
  my ($fh) = @_;

  my @samples;
  my $i = 0;
  while (my $line = <$fh>) {
    chomp($line);
    ++$i;

    next if $line =~ m/^\s*$/msx;

    # Fields:
    # Sample name
    # Supplied gender code (must be a member of the gender dictionary names)
    # GTC file path
    # Green IDAT file path
    # Red IDAT file path
    my @fields = split(/\t/, $line, -1);
    my $num_fields = scalar @fields;

    unless ($num_fields == 6) {
      die "Parse error on line $i: expected 6 fields, but found $num_fields:\n" .
        "$line\n";
    }

    my $sample = {sample => $fields[0],
                  gender => $fields[1],
                  beadchip => $fields[2],
                  gtc_path => $fields[3],
                  idat_grn_path => $fields[4],
                  idat_red_path => $fields[5]};

    unless ($sample->{gtc_path}) {
      die "Parse error on line $i: no GTC path was provided:\n$line\n";
    }

    unless ($sample->{beadchip}) {
      my $gtc = basename($sample->{gtc_path});
      my ($beadchip_guess) = $gtc =~ m/^(\d{10})_/msx;

      if ($beadchip_guess) {
        $sample->{beadchip} = $beadchip_guess;
      }
      else {
        die "Parse error on line $i: no beadchip was supplied and unable " .
          "to infer from GTC file name:\n$line\n";
      }
    }

    if ($sample->{idat_grn_path} or $sample->{idat_red_path}) {
      unless ($sample->{idat_grn_path}) {
        die "Parse error on line $i: no green IDAT path was provided:\n$line\n";
      }
      unless ($sample->{idat_red_path}) {
        die "Parse error on line $i: no red IDAT path was provided:\n$line\n";
      }
    }

    push(@samples, $sample);
  }

  return @samples;
}

sub validate_snpset {
  my ($run, $snpset) = @_;

  unless ($run->validate_snpset($snpset)) {
    die "Cannot add this project to '", $run->name, "'; design mismatch: '",
      $snpset->name, "' cannot be added to existing designs [",
        join(", ", map { $_->snpset->name } $run->datasets), "]\n";

  }

  return $snpset;
}

sub print_pre_report {
  my ($supplier, $namespace, $snpset) = @_;
  print STDERR "Adding dataset:\n";
  print STDERR "  From '", $supplier->name, "'\n";
  print STDERR "  Into namespace '$namespace'\n";
  print STDERR "  Using '", $snpset->name, "'\n";

  return;
}

sub print_post_report {
  my ($samples) = @_;

  my $num_samples = scalar @$samples;

  print STDERR "Added dataset:\n";
  print STDERR "  $num_samples samples\n";

  return;
}


__END__

=head1 NAME

ready_external

=head1 SYNOPSIS

ready_external [--config <database .ini file>] [--dbfile <SQLite file>] \
   --namespace <sample namespace> --chip-design <chip design name> \
   --run <pipeline run name> --supplier <supplier name> [--verbose]

Options:

  --chip-design The microarray chip design (see the snpsets.ini file)
  --config      Load database configuration from a user-defined .ini file.
                Optional, defaults to $HOME/.npg/genotyping.ini
  --dbfile      The SQLite database file. If not supplied, defaults to the
                value given in the configuration .ini file.
  --help        Display help.
  --input       The sample manifest file. Optional, defaults to STDIN.
  --namespace   The namespace for the imported sample names.
  --run         The pipeline run name in the database which will be created
                 or added to.
  --supplier    The name of the sample supplier.
  --verbose     Print messages while processing. Optional.

=head1 DESCRIPTION

Adds all of the samples described in tab-delimited manifest file to a
named pipeline run (a collection of samples to be analysed
together). Several sets may be added to the same pipeline run by
running this program multiple times on the same SQLite database.

Samples from different suppliers may have the same sample name by
chance. The use of a namespace enables these samples to be
distinguished while preserving their original names.

Samples using different Infinium chip designs may not be mixed within
the same run.

The --run and --namespace arguments must be at least 4 characters in
length and may contains only letters, numbers, hypens, underscores and
dots.

The manifest file must contain 6 fields per line, delimited by tab
characters. The fields are:

  Sample name
  Sample gender code (using the gender controlled vocabulary from genders.ini)
  Beadchip number
  Absolute path to the GTC format data file
  Absolute path to the green channel IDAT format data file
  Absolute path to the red channel IDAT format data file

The beadchip number may be omitted, in which case an attempt will be
made to infer it from the GTC file name. For this to be successful, GTC file
name must start with the approriate ten-digit beadchip code (this is the
case unless the file has been renamed).

Both IDAT file paths may be omitted. If only one is present, this is
considered an error.

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
