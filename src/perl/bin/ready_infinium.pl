#!/software/bin/perl

use utf8;

package main;

use warnings;
use strict;
use Getopt::Long;
use Log::Log4perl qw(:easy);
use Pod::Usage;

use WTSI::Genotyping;
use WTSI::Genotyping::Database::Pipeline;
use WTSI::Genotyping::Database::Infinium;
use WTSI::Genotyping::Database::Warehouse;
use WTSI::Genotyping::Database::SNP;

our $AUTOCALL_PASS = 'Pass';
our $WTSI_NAMESPACE = 'wtsi';
our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";
our $ID_REGEX = qr/^[A-Za-z0-9-._]{4,}$/;

Log::Log4perl->easy_init($ERROR);

run() unless caller();

sub run {
  my $config;
  my $dbfile;
  my $force;
  my $project_name;
  my $run_name;
  my $maximum;
  my $namespace;
  my $supplier_name;
  my $verbose;

  GetOptions('config=s' => \$config,
             'dbfile=s'=> \$dbfile,
             'force' => \$force,
             'help' => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'run=s' => \$run_name,
             'maximum=i' => \$maximum,
             'namespace=s' => \$namespace,
             'project=s' => \$project_name,
             'supplier=s' => \$supplier_name,
             'verbose' => \$verbose);

  $config ||= $DEFAULT_INI;
  $namespace ||= $WTSI_NAMESPACE;

  unless ($project_name) {
    pod2usage(-msg => "A --project argument is required\n", -exitval => 2);
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

  my $ifdb = WTSI::Genotyping::Database::Infinium->new
    (name   => 'infinium',
     inifile =>  $config)->connect(RaiseError => 1);

  my $ssdb = WTSI::Genotyping::Database::Warehouse->new
    (name   => 'sequencescape_warehouse',
     inifile =>  $config)->connect(RaiseError => 1);

  my $snpdb = WTSI::Genotyping::Database::SNP->new
    (name   => 'snp',
     inifile => $config)->connect(RaiseError => 1);

  my @chip_designs = $ifdb->find_project_chip_design($project_name);
  unless (@chip_designs) {
    die "Invalid chip design '$chip_design'. Valid designs are: [" .
      join(", ", map { $_->name } $pipedb->snpset->all) . "]\n";
  }

  if (scalar @chip_designs > 1) {
    unless ($force) {
      die ">1 chip design found for project '$project_title': [" .
        join(", ", @chip_designs) . "]";
    }
  }

  my $snpset = $pipedb->snpset->find({name => $chip_design});
  my $infinium = $pipedb->method->find({name => 'Infinium'});
  my $autocall = $pipedb->method->find({name => 'Autocall'});
  my $supplied = $pipedb->method->find({name => 'Supplied'});
  my $autocall_pass = $pipedb->state->find({name => 'autocall_pass'});
  my $autocall_fail = $pipedb->state->find({name => 'autocall_fail'});
  my $idat_unavailable = $pipedb->state->find({name => 'idat_unavailable'});
  my $gtc_unavailable = $pipedb->state->find({name => 'gtc_unavailable'});
  my $withdrawn = $pipedb->state->find({name => 'consent_withdrawn'});
  my $gender_na = $pipedb->gender->find({name => 'Not Available'});

  if ($pipedb->dataset->find({if_project => $project_name})) {
    die "Failed to load '$project_name'; it is present already.\n";
  }

  # This is here because SequenceScape is missing (!) some tracking
  # information. It enables the data to be imported without removing
  # NOT NULL UNIQUE constraints.
  my $num_untracked_samples = 0;
  my $num_consent_withdrawn_samples = 0;
  my $num_untracked_plates = 0;
  my %untracked_plates;

  $pipedb->in_transaction
    (sub {
       my $supplier = $pipedb->datasupplier->find_or_create
         ({name => $supplier_name,
           namespace => $namespace});
       my $run = $pipedb->piperun->find_or_create({name => $run_name});
       validate_snpset($run, $snpset);

       my $dataset = $run->add_to_datasets({if_project => $project_name,
                                            datasupplier => $supplier,
                                            snpset => $snpset});

       print_pre_report($supplier, $project_name, $namespace, $snpset)
         if $verbose;

       my %cache;
       my @samples;

     SAMPLE: foreach my $if_sample (@{$ifdb->find_project_samples
                                        ($project_name)}) {
         my $if_chip = $if_sample->{beadchip};
         my $grn_path = $if_sample->{idat_grn_path};
         my $red_path = $if_sample->{idat_red_path};
         my $gtc_path = $if_sample->{gtc_path};
         my $if_barcode = $if_sample->{'plate'};
         my $if_well = $if_sample->{'well'};
         my $if_name = $if_sample->{'sample'};
         my $if_status = $if_sample->{'status'};

         my $ss_plate;
         if (exists $cache{$if_sample->{'plate'}}) {
           $ss_plate = $cache{$if_sample->{'plate'}};
         }
         else {
           $ss_plate = $ssdb->find_infinium_plate($if_barcode);
         }

         my $address = $pipedb->address->find({label1 => $if_well});
         my $ss_sample = $ss_plate->{$address->label2};

         # Untracked
         my $ss_id = $ss_sample->{sanger_sample_id} ||
           sprintf("<NA identifier %d>", ++$num_untracked_samples);
         my $ss_barcode = $ss_sample->{barcode} ||
           $untracked_plates{$if_barcode};
         unless ($ss_barcode) {
           $untracked_plates{$if_barcode} = sprintf("<NA barcode %d>",
                                                    ++$num_untracked_plates);
           $ss_barcode = $untracked_plates{$if_barcode};
         }

         my $ss_gender = $ss_sample->{gender};
         my $ss_consent_withdrawn = $ss_sample->{consent_withdrawn};
         my $gender = $pipedb->gender->find({name => $ss_gender}) || $gender_na;
         my $state = $autocall_pass;
         my $sample = $dataset->add_to_samples({name => $if_name,
                                                sanger_sample_id => $ss_id,
                                                beadchip => $if_chip,
                                                include => 0});

         # If consent has been withdrawn, do not analyse and do not
         # look in SNP for Sequenom genotypes
         if ($ss_consent_withdrawn) {
           ++$num_consent_withdrawn_samples;
           $sample->add_to_genders($gender_na, {method => $supplied});
           $sample->add_to_states($withdrawn);
         }
         else {
           $sample->add_to_genders($gender, {method => $supplied});
         }

         my $autocall_state = $autocall_pass;
         unless ($if_status && $if_status eq $AUTOCALL_PASS) {
           $autocall_state = $autocall_fail;
         }
         $sample->add_to_states($autocall_state);

         my $plate = $pipedb->plate->find_or_create
           ({if_barcode => $if_barcode,
             ss_barcode => $ss_barcode});

         $plate->add_to_wells({address => $address,
                               sample  => $sample});
         $sample->add_to_results({method => $autocall,
                                  value => $gtc_path});
         $sample->add_to_results({method => $infinium,
                                  value => $grn_path});
         $sample->add_to_results({method => $infinium,
                                  value => $red_path});

         my $unix_gtc_path = $sample->gtc;
         unless (defined $unix_gtc_path and -e $unix_gtc_path) {
           $sample->add_to_states($gtc_unavailable);
         }

         my $unix_red_path = $sample->idat('red');
         my $unix_grn_path = $sample->idat('green');
         my $red_found = defined $unix_red_path and -e $unix_red_path;
         my $grn_found = defined $unix_grn_path and -e $unix_grn_path;
         unless ($red_found and $grn_found) {
           $sample->add_to_states($idat_unavailable);
         }

         $sample->include_from_state;
         $sample->update;

         unless ($ss_consent_withdrawn) {
           my $result = $sample->add_to_results({method => $infinium,
                                                 value => $gtc_path});
           push @samples, $sample;
         }

         last SAMPLE if defined $maximum && scalar @samples == $maximum;
       }

       unless (@samples) {
         print_post_report($pipedb, $project_name, $num_untracked_plates,
                           $num_consent_withdrawn_samples);
         die "Failed to find any samples for project '$project_name'\n";
       }

       $snpdb->insert_sequenom_calls($pipedb, \@samples);
     });

  print_post_report($pipedb, $project_name, $num_untracked_plates,
                    $num_consent_withdrawn_samples) if $verbose;

  return;
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
  my ($supplier, $project_name, $namespace, $snpset) = @_;
  print STDERR "Adding dataset for '$project_name':\n";
  print STDERR "  From '", $supplier->name, "'\n";
  print STDERR "  Into namespace '$namespace'\n";
  print STDERR "  Using '", $snpset->name, "'\n";

  return;
}

sub print_post_report {
  my ($pipedb, $project_name, $untracked, $unconsented) = @_;

  my $ds = $pipedb->dataset->find({if_project => $project_name});
  my $proj = $ds->if_project;
  my $num_samples = $ds->samples->count;

  my $num_plates = $pipedb->plate->search
    ({'dataset.if_project' => $project_name},
     {join => {wells => {sample => 'dataset'}}, distinct => 1})->count;

  my $num_calls = $pipedb->snp_result->search
    ({'dataset.if_project' => $project_name},
     {join => {result => {sample => 'dataset'}}})->count;

  print STDERR "Added dataset for '$proj':\n";
  print STDERR "  $num_plates plates ($untracked missing from Warehouse)\n";
  print STDERR "  $num_samples samples ($unconsented consent withdrawn)\n";
  print STDERR "  $num_calls Sequenom SNP calls\n";

  return;
}

__END__

=head1 NAME

ready_infinium

=head1 SYNOPSIS

ready_infinium [--config <database .ini file>] [--dbfile <SQLite file>] \
   [--force] [--namespace <sample namespace>] [--maximum <n>] \
   --project <project name> --run <pipeline run name> \
   --supplier <supplier name> [--verbose]

Options:

  --config    Load database configuration from a user-defined .ini file.
              Optional, defaults to $HOME/.npg/genotyping.ini
  --dbfile    The SQLite database file. If not supplied, defaults to the
              value given in the configuration .ini file.
  --force     Force the analysis when there are multiple chip designs in
              the project.
  --help      Display help.
  --maximum   Import samples up to a maximum number. Optional.
  --namespace The namespace for the imported sample names. Optional,
              defaults to 'wtsi'.
  --project   The name of the Infinium LIMS project to import.
  --run       The pipeline run name in the database which will be created
              or added to.
  --supplier  The name of the sample supplier.
  --verbose   Print messages while processing. Optional.

=head1 DESCRIPTION

Adds all of the 'Passed' samples from an Infinium LIMS project to a
named pipeline run (a collection of samples to be analysed
together). Several projects may be added to the same pipeline run by
running this program multiple times on the same SQLite database.

Samples that have had consent withdrawn will not be included.

Samples from different suppliers may have the same sample name by
chance. The use of a namespace enables these samples to be
distinguished while preserving their original names.

Projects using different Infinium chip designs may be mixed within
the same run only if the --force argument is used.

The --run and --namespace arguments must be at least 4 characters in
length and may contains only letters, numbers, hypens, underscores and
dots.

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
