#!/software/bin/perl

use utf8;

package main;

use warnings;
use strict;
use Config::IniFiles;
use Getopt::Long;
use List::AllUtils qw(uniq);
use Log::Log4perl;
use Log::Log4perl::Level;
use Pod::Usage;

use WTSI::NPG::Database::Warehouse;
use WTSI::NPG::Genotyping;
use WTSI::NPG::Genotyping::Database::Pipeline;
use WTSI::NPG::Genotyping::Database::Infinium;
use WTSI::NPG::Genotyping::Database::SNP;
use WTSI::NPG::Genotyping::Fluidigm::Subscriber;
use WTSI::NPG::Genotyping::SNPSet;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;
use WTSI::NPG::Utilities qw(user_session_log);

our $AUTOCALL_PASS = 'Pass';
our $WTSI_NAMESPACE = 'wtsi';
our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";
our $SNPSETS_INI = 'snpsets.ini';
our $ID_REGEX = qr/^[A-Za-z0-9-._]{4,}$/;

our $SEQUENOM = 'sequenom';
our $FLUIDIGM = 'fluidigm';

our $SEQUENOM_QC_DIR = '/nfs/srpipe_references/genotypes/';
our %SEQUENOM_QC_PLEX = (
     W30467 => $SEQUENOM_QC_DIR.'W30467_snp_set_info_1000Genomes.tsv',
     W34340 => $SEQUENOM_QC_DIR.'W34340_snp_set_info_1000Genomes.tsv',
     W35540 => $SEQUENOM_QC_DIR.'W35540_snp_set_info_1000Genomes.tsv'
);

our %QC_PLEX_DEFAULT = (
    $SEQUENOM => 'W30467',
    $FLUIDIGM => 'qc'
);

our $DEFAULT_REFERENCE_PATH = '/seq/fluidigm/multiplexes';
our $DEFAULT_DATA_PATH      = '/seq/fluidigm';

my $uid = `whoami`;
chomp($uid);
my $session_log = user_session_log($uid, 'ready_infinium');

my $embedded_conf = "
   log4perl.logger.npg.irods.publish = ERROR, A1, A2

   log4perl.appender.A1           = Log::Log4perl::Appender::Screen
   log4perl.appender.A1.utf8      = 1
   log4perl.appender.A1.layout    = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A1.layout.ConversionPattern = %d %p %m %n

   log4perl.appender.A2           = Log::Log4perl::Appender::File
   log4perl.appender.A2.filename  = $session_log
   log4perl.appender.A2.utf8      = 1
   log4perl.appender.A2.layout    = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A2.layout.ConversionPattern = %d %p %m %n
   log4perl.appender.A2.syswrite  = 1
";

my $log;

run() unless caller();

sub run {
  my $chip_design;
  my $config;
  my $dbfile;
  my $debug;
  my $log4perl_config;
  my $maximum;
  my $namespace;
  my $pl_config_dir;
  my $project_title;
  my $qc_platform;
  my $qc_plex;
  my $reference_path;
  my $run_name;
  my $supplier_name;
  my $verbose;

  GetOptions('chip-design=s'    => \$chip_design,
             'config=s'         => \$config,
             'dbfile=s'         => \$dbfile,
             'debug'            => \$debug,
             'help'             => sub { pod2usage(-verbose => 2,
                                                   -exitval => 0) },
             'logconf=s'        => \$log4perl_config,
             'maximum=i'        => \$maximum,
             'namespace=s'      => \$namespace,
             'pl-config-dir=s'  => \$pl_config_dir,
             'project=s'        => \$project_title,
             'qc-platform=s'    => \$qc_platform,
             'qc-plex=s'        => \$qc_plex,
             'reference-path=s' => \$reference_path,
             'run=s'            => \$run_name,
             'supplier=s'       => \$supplier_name,
             'verbose'          => \$verbose);

  $config         ||= $DEFAULT_INI;
  $pl_config_dir  ||= WTSI::NPG::Genotyping::config_dir();
  $namespace      ||= $WTSI_NAMESPACE;
  $reference_path ||= $DEFAULT_REFERENCE_PATH;;

  unless ($dbfile) {
    pod2usage(-msg => "A --dbfile argument is required\n", -exitval => 2);
  }
  unless ($project_title) {
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

  if ($log4perl_config) {
    Log::Log4perl::init($log4perl_config);
    $log = Log::Log4perl->get_logger('npg.irods.publish');
  }
  else {
    Log::Log4perl::init(\$embedded_conf);
    $log = Log::Log4perl->get_logger('npg.irods.publish');

    if ($verbose) {
      $log->level($INFO);
    }
    elsif ($debug) {
      $log->level($DEBUG);
    }
  }

  unless (-e $dbfile) {
      $log->logcroak("SQLite database file '$dbfile' does not exist");
  }

  if ($qc_platform) {
      if ($qc_plex) {
          $log->logcroak("Cannot specify both --qc-platform ",
                         "and --qc-plex options");
      } else {
          $qc_platform = lc $qc_platform;
          unless ($qc_platform eq $FLUIDIGM or $qc_platform eq $SEQUENOM) {
              pod2usage(-msg => "Invalid qc-platform '$qc_platform' " .
                        "expected one of [$FLUIDIGM, $SEQUENOM]\n",
                        -exitval => 2);
          }
          $log->info("Selected QC platform $qc_platform");
      }
  } elsif ($qc_plex) {
      $log->info("Selected QC plex $qc_plex");
  } else {
      $log->warn("No QC platform or plex selected. ",
                 "Proceeding without fetching QC data");
  }

  $log->info("Updating database '$dbfile' using config from $config");


  my @initargs = (name        => 'pipeline',
                  inifile     => $config,
                  dbfile      => $dbfile,
                  config_dir  => $pl_config_dir);

  my $pipedb = WTSI::NPG::Genotyping::Database::Pipeline->new
    (@initargs)->connect
      (RaiseError     => 1,
       sqlite_unicode => 1,
       on_connect_do  => 'PRAGMA foreign_keys = ON');

  my $ifdb = WTSI::NPG::Genotyping::Database::Infinium->new
    (name    => 'infinium',
     inifile => $config,
     logger  => $log)->connect(RaiseError => 1);

  my $ssdb = WTSI::NPG::Database::Warehouse->new
    (name    => 'sequencescape_warehouse',
     inifile => $config,
     logger  => $log)->connect(RaiseError           => 1,
                               mysql_enable_utf8    => 1,
                               mysql_auto_reconnect => 1);

  my @chip_designs = @{$ifdb->find_project_chip_design($project_title)};
  unless (@chip_designs) {
    $log->logcroak("Invalid chip design '",
                   $chip_design, "'. Valid designs are: [" ,
                   join(", ", map { $_->name } $pipedb->snpset->all), "]");
  }
  if ($chip_design) {
    unless (grep { /^$chip_design$/ } @chip_designs) {
        $log->logcroak("Invalid chip design '",
                       $chip_design, "'. Valid designs are: [ ",
                       join(", ", @chip_designs), "]");
    }
  }
  else {
    if (scalar @chip_designs > 1) {
        $log->logcroak("Found >1 chip design. Use the --chip_design ",
                       "argument to specify which one to use: [",
                       join(", ", @chip_designs), "]");
    }
    else {
      $chip_design = $chip_designs[0];
    }
  }

  my $snpset = $pipedb->snpset->find({name => $chip_design});
  unless ($snpset) {
    $log->logcroak("Chip design '$chip_design' is not configured for use. ",
                   "Configured are: [",
                   join(", ", map { $_->name } $pipedb->snpset->all), "]");
  }

  my $infinium = $pipedb->method->find({name => 'Infinium'});
  my $autocall = $pipedb->method->find({name => 'Autocall'});
  my $supplied = $pipedb->method->find({name => 'Supplied'});
  my $autocall_pass     = $pipedb->state->find({name => 'autocall_pass'});
  my $autocall_fail     = $pipedb->state->find({name => 'autocall_fail'});
  my $idat_unavailable  = $pipedb->state->find({name => 'idat_unavailable'});
  my $gtc_unavailable   = $pipedb->state->find({name => 'gtc_unavailable'});
  my $consent_withdrawn = $pipedb->state->find({name => 'consent_withdrawn'});
  my $gender_na = $pipedb->gender->find({name => 'Not Available'});

  if ($pipedb->dataset->find({if_project => $project_title})) {
    $log->logcroak("Failed to load '", $project_title,
                   "'; it is present already.");
  }

  # This is here because SequenceScape is missing (!) some tracking
  # information. It enables the data to be imported without removing
  # NOT NULL UNIQUE constraints.
  my $num_untracked_samples         = 0;
  my $num_consent_withdrawn_samples = 0;
  my $num_untracked_plates          = 0;
  my %untracked_plates;

  # chosen QC plex may not have been run for all samples
  my $samples_without_qc_calls = 0;

  $pipedb->in_transaction
    (sub {
       my $supplier = $pipedb->datasupplier->find_or_create
         ({name      => $supplier_name,
           namespace => $namespace});
       my $run = $pipedb->piperun->find_or_create({name => $run_name});
       validate_snpset($run, $snpset);

       my $dataset = $run->add_to_datasets({if_project   => $project_title,
                                            datasupplier => $supplier,
                                            snpset       => $snpset});

       print_pre_report($supplier, $project_title, $namespace, $snpset)
         if $verbose;

       # FIXME -- cache this in the database handle
       my %cache;
       my @samples;

     SAMPLE: foreach my $if_sample (@{$ifdb->find_project_samples
                                        ($project_title)}) {
         my $if_chip    = $if_sample->{beadchip};
         my $grn_path   = $if_sample->{idat_grn_path};
         my $red_path   = $if_sample->{idat_red_path};
         my $gtc_path   = $if_sample->{gtc_path};
         my $if_barcode = $if_sample->{'plate'};
         my $if_well    = $if_sample->{'well'};
         my $if_name    = $if_sample->{'sample'};
         my $if_status  = $if_sample->{'status'};
         my $if_rowcol  = $if_sample->{'beadchip_section'};

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

         my $ss_supply = $ss_sample->{supplier_name};
         $ss_supply ||= "";
         my $ss_cohort = $ss_sample->{cohort};
         $ss_cohort ||= "";

         my $ss_gender = $ss_sample->{gender};
         my $ss_consent_withdrawn = $ss_sample->{consent_withdrawn};
         my $gender = $pipedb->gender->find({name => $ss_gender}) || $gender_na;
         my $state = $autocall_pass;
         my $sample =
           $dataset->add_to_samples({name             => $if_name,
                                     sanger_sample_id => $ss_id,
                                     beadchip         => $if_chip,
                                     include          => 0,
                                     supplier_name    => $ss_supply,
                                     cohort           => $ss_cohort,
                                     rowcol           => $if_rowcol});

         # If consent has been withdrawn, do not analyse and do not
         # look in SNP for Sequenom genotypes
         if ($ss_consent_withdrawn) {
           ++$num_consent_withdrawn_samples;
           $sample->add_to_genders($gender_na, {method => $supplied});
           $sample->add_to_states($consent_withdrawn);
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
                                  value  => $gtc_path});
         $sample->add_to_results({method => $infinium,
                                  value  => $grn_path});
         $sample->add_to_results({method => $infinium,
                                  value  => $red_path});

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
                                                 value  => $gtc_path});
           push @samples, $sample;
         }

         last SAMPLE if defined $maximum && scalar @samples == $maximum;
       }
       unless (@samples) {
         print_post_report($pipedb, $project_title, $num_untracked_plates,
                           $num_consent_withdrawn_samples,
                           $samples_without_qc_calls );
         $log->logcroak("Failed to find any samples for project '",
                        $project_title, "'");
       }

       # need to know both QC platform and QC plex name
       if ($qc_platform) {
           $qc_plex = $QC_PLEX_DEFAULT{$qc_platform};
       } elsif ($qc_plex) {
           # QC platform is section title in snpsets.ini
           my $snpsets_ini_path = $pl_config_dir."/".$SNPSETS_INI;
           unless (-e $snpsets_ini_path) {
               $log->logcroak("Snpsets .ini path '", $snpsets_ini_path,
                              "' does not exist.");
           }
           $log->debug("Reading snpsets from ", $snpsets_ini_path);
           my $ini = Config::IniFiles->new(-file => $snpsets_ini_path);
           foreach my $sect ($ini->Sections) {
               foreach my $elt ($ini->val($sect, 'name')) {
                   if ($elt eq $qc_plex) {
                       $qc_platform = $sect;
                       last;
                   }
               }
           }
           unless ($qc_platform) {
               # script will die here if an unknown QC plex name is given
               $log->logcroak("Could not find QC platform for given QC plex '",
                              $qc_plex, "' in ", $snpsets_ini_path);
           }
       } else {
         $log->debug("Not inserting QC data; ",
                     "no QC platform or plex specified");
       }

       # retrieve QC calls for given platform and plex
       # if $qc_platform is false, continue without inserting calls
       my $inserted = 0;
       if ($qc_platform) {
           if ($qc_platform eq $SEQUENOM) {
               $log->debug("Inserting Sequenom QC data from SNP");

               my $snpdb = WTSI::NPG::Genotyping::Database::SNP->new
                   (name    => 'snp',
                    inifile => $config,
                    logger  => $log)->connect(RaiseError => 1);

               $inserted = insert_sequenom_calls($pipedb, $snpdb,
                                                 \@samples, $qc_plex);
           } elsif ($qc_platform eq $FLUIDIGM) {
               $log->debug("Inserting Fluidigm QC data from iRODS");
               my $irods = WTSI::NPG::iRODS->new;
               $inserted = insert_fluidigm_calls($pipedb, $irods,
                                                 \@samples, $qc_plex,
                                                 $reference_path, $log);
           } else {
               $log->logcroak("Unexpected QC platform '$qc_platform'");
           }
       }
       $samples_without_qc_calls = scalar(@samples) - $inserted;
     });

  print_post_report($pipedb, $project_title, $num_untracked_plates,
                    $num_consent_withdrawn_samples,
                    $samples_without_qc_calls ) if $verbose;

  return;
}

sub validate_snpset {
  my ($run, $snpset) = @_;

  unless ($run->validate_snpset($snpset)) {
    $log->logcroak("Cannot add this project to '", $run->name,
                   "'; design mismatch: '", $snpset->name,
                   "' cannot be added to existing designs [",
                   join(", ", map { $_->snpset->name } $run->datasets), "]");
  }

  return $snpset;
}

sub insert_fluidigm_calls {
  # returns the number of samples for which calls were inserted
  my ($pipedb, $irods, $samples, $qc_plex, $reference_path, $log) = @_;

  my $method = $pipedb->method->find({name => 'Fluidigm'});
  $method or $log->logcroak("The genotyping method 'Fluidigm' is ",
                            "not configured for use");
  my $snpset = $pipedb->snpset->find({name => $qc_plex});
  $snpset or $log->logcroak("The Fluidigm SNP set '", $qc_plex, "' is unknown");
  my $reference_name = 'Homo_sapiens (1000Genomes)';

  my $inserted = 0;
  my @sample_ids = uniq map { $_->sanger_sample_id } @$samples;

  my $subscriber = WTSI::NPG::Genotyping::Fluidigm::Subscriber->new
    (irods          => $irods,
     data_path      => $DEFAULT_DATA_PATH,
     reference_path => $reference_path,
     reference_name => $reference_name,
     snpset_name    => $snpset->name,
     logger         => $log);
  my $resultsets = $subscriber->get_assay_resultsets(\@sample_ids);

  foreach my $sample (@$samples) {
    my $sample_resultsets = $resultsets->{$sample->sanger_sample_id};
    my $num_resultsets = scalar @$sample_resultsets;
    if ($num_resultsets > 0) {
      my $raw_data = join q{; }, map { $_->str } @$sample_resultsets;
      my $result = $sample->add_to_results({method => $method,
                                            value  => $raw_data});

      my $calls = $subscriber->get_calls($sample_resultsets);

      # Make calls
      insert_qc_calls($pipedb, $snpset, $result, $calls);
      $inserted++;
    }
    else {
      $log->warn("Failed to find any Fluidigm results for '",
                 $sample->sanger_sample_id,  "', QC plex '", $qc_plex, "'");
    }
  }
  return $inserted;
}

sub insert_sequenom_calls {
  # returns the number of samples for which calls were inserted
  my ($pipedb, $snpdb, $samples, $qc_plex) = @_;

  my $method = $pipedb->method->find({name => 'Sequenom'});
  my $snpset = $pipedb->snpset->find({name => $qc_plex});

  $method or $log->logcroak("The genotyping method 'Sequenom' ",
                            "is not configured for use");
  $snpset or $log->logcroak("The Sequenom SNP set '$qc_plex' is unknown");

  my $sequenom_set = WTSI::NPG::Genotyping::SNPSet->new
    (name      => $qc_plex,
     file_name => $SEQUENOM_QC_PLEX{$qc_plex});

  my @sample_names;
  foreach my $sample (@$samples) {
    if ($sample->include and defined $sample->sanger_sample_id) {
      push @sample_names, $sample->sanger_sample_id;
    }
  }

  my $sequenom_results = $snpdb->find_sequenom_calls($sequenom_set,
                                                     \@sample_names);

  my $inserted = 0;
  foreach my $sample (@$samples) {
    my $calls = $sequenom_results->{$sample->sanger_sample_id};

    if ($calls && @$calls) {
      my $result = $sample->add_to_results({method => $method});
      insert_qc_calls($pipedb, $snpset, $result, $calls);
      $inserted++;
    }
    else {
      $log->warn("Failed to find any Sequenom results for sample '",
                 $sample->sanger_sample_id, "', QC plex '", $qc_plex, "'");
    }
  }
  return $inserted;
}

sub insert_qc_calls {
  my ($pipedb, $snpset, $result, $calls) = @_;

  foreach my $call (@$calls) {
    my $snp = $pipedb->snp->find_or_create
      ({name       => $call->snp->name,
        chromosome => $call->snp->chromosome,
        position   => $call->snp->position,
        snpset     => $snpset});

    $result->add_to_snp_results({snp   => $snp,
                                 value => $call->genotype});
  }

  return $result;
}

sub print_pre_report {
  my ($supplier, $project_title, $namespace, $snpset) = @_;
  my $report = "Adding dataset for '$project_title':\n".
      "  From supplier '".$supplier->name."'\n".
      "  Into namespace '".$namespace."'\n".
      "  Using snpset '".$snpset->name;
  $log->info($report);
  return;
}

sub print_post_report {
  my ($pipedb, $project_title, $untracked, $unconsented,
      $samples_without_qc) = @_;

  my $ds = $pipedb->dataset->find({if_project => $project_title});
  my $proj = $ds->if_project;
  my $num_samples = $ds->samples->count;

  my $num_plates = $pipedb->plate->search
    ({'dataset.if_project' => $project_title},
     {join => {wells => {sample => 'dataset'}}, distinct => 1})->count;

  my $num_calls = $pipedb->snp_result->search
    ({'dataset.if_project' => $project_title},
     {join => {result => {sample => 'dataset'}}})->count;

  my $report = "Added dataset for '$proj':\n".
      "  $num_plates plates ($untracked missing from Warehouse)\n".
      "  $num_samples samples ($unconsented consent withdrawn)\n".
      "  $num_calls QC SNP calls\n".
      "  $samples_without_qc samples without QC plex calls";

  $log->info($report);

  return;
}

__END__

=head1 NAME

ready_infinium

=head1 SYNOPSIS

ready_infinium [--config <database .ini file>] [--chip-design <name>] \
   [--namespace <sample namespace>] [--maximum <n>] \
   --dbfile <SQLite file> --project <project name> [--qc-platform <name>] \
   --run <pipeline run name> --supplier <supplier name> [--verbose]

Options:

  --chip-design   Explicitly state the chip design.
  --config        Load database configuration from a user-defined .ini file.
                  Optional, defaults to $HOME/.npg/genotyping.ini
  --dbfile        The SQLite database file. Required.
  --help          Display help.
  --maximum       Import samples up to a maximum number. Optional.
  --namespace     The namespace for the imported sample names. Optional,
                  defaults to 'wtsi'.
  --pl-config-dir Directory containing additional pipeline .ini files.
                  Optional.
  --project       The name of the Infinium LIMS project to import.
  --qc-platform   The QC genotyping platform. Fluidigm or Sequenom. Optional;
                  not compatible with --qc-plex.
  --qc-plex       Name of snpset used by the QC genotyping platform. The
                  snpset must be present in the SQLite database. Optional;
                  not compatible with --qc-platform.
  --run           The pipeline run name in the database which will be created
                  or added to.
  --supplier      The name of the sample supplier.
  --verbose       Print messages while processing. Optional.

=head1 DESCRIPTION

Adds all of the 'Passed' samples from an Infinium LIMS project to a
named pipeline run (a collection of samples to be analysed
together). Several projects may be added to the same pipeline run by
running this program multiple times on the same SQLite database.

Samples that have had consent withdrawn will not be included.

Samples from different suppliers may have the same sample name by
chance. The use of a namespace enables these samples to be
distinguished while preserving their original names.

Projects using mixed Infinium chip designs may be analysed by using the
--chip_design argument to state which design is to be used.

The --run and --namespace arguments must be at least 4 characters in
length and may contains only letters, numbers, hypens, underscores and
dots.

=head1 METHODS

None

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2012, 2013, 2014, 2015 Genome Research Limited. All
Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
