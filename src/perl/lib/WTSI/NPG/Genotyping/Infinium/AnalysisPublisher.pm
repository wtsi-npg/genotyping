
use utf8;

package WTSI::NPG::Genotyping::Infinium::AnalysisPublisher;

use File::Spec;
use Moose;

use WTSI::NPG::iRODS;
use WTSI::NPG::Publisher;

with 'WTSI::DNAP::Utilities::Loggable', 'WTSI::NPG::Accountable',
  'WTSI::NPG::Annotator', 'WTSI::NPG::Genotyping::Annotator';

our $DEFAULT_SAMPLE_ARCHIVE = '/archive/GAPI/gen/infinium';

has 'irods' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::iRODS',
   required => 1,
   default  => sub {
     return WTSI::NPG::iRODS->new;
   });

has 'publication_time' =>
  (is       => 'ro',
   isa      => 'DateTime',
   required => 1);

has 'analysis_directory' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1);

has 'sample_archive' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 0,
   default  => sub {
     return $DEFAULT_SAMPLE_ARCHIVE;
   });

has 'run_name' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1);

has 'pipe_db' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::Genotyping::Database::Pipeline',
   required => 1);

sub BUILD {
  my ($self) = @_;

  unless (-e $self->analysis_directory) {
    $self->logconfess("Analysis directory '", $self->analysis_directory,
                      "' does not exist");
  }
  unless (-d $self->analysis_directory) {
    $self->logconfess("Analysis directory '", $self->analysis_directory,
                      "' is not a directory");
  }

  # Make our irods handle use our logger by default
  $self->irods->logger($self->logger);
}

sub publish {
  my ($self, $publish_dest) = @_;

  defined $publish_dest or
    $self->logconfess('A defined publish_dest argument is required');

  $publish_dest eq '' and
    $self->logconfess('A non-empty publish_dest argument is required');

  $publish_dest = File::Spec->canonpath($publish_dest);
  $self->info("Publishing to '$publish_dest' using the sample archive in '",
              $self->sample_archive, "'");

  # Make a path based on the database file's MD5 to enable even distribution
  my $irods = $self->irods;
  my $pipedb = $self->pipe_db;
  my $hash_path = $irods->hash_path($pipedb->dbfile);
  my $target = File::Spec->catdir($publish_dest, $hash_path);

  my @dirs = grep { $_ ne '' } File::Spec->splitdir($self->analysis_directory);
  my $leaf_dir = pop @dirs;
  my $leaf_collection = File::Spec->catdir($target, $leaf_dir);

  if ($irods->list_collection($leaf_collection)) {
    $self->logcroak("An iRODS collection already exists at ",
                    "'$leaf_collection'. ",
                    "Please move or delete it before proceeding.");
  }

  $self->debug("Finding the project titles in the analysis database");
  my $run = $pipedb->piperun->find({name => $self->run_name});
  unless ($run) {
    $self->logcroak("The analysis database does not contain a run called '",
                    $self->run_name, "'");
  }

  my @project_titles;
  foreach my $dataset ($run->datasets) {
    push(@project_titles, $dataset->if_project);
  }

  unless (@project_titles) {
    $self->logcroak("The analysis database contained no data for run '",
                    $self->run_name, "'")
  }

  my $analysis_coll;
  my $analysis_uuid;
  my $num_projects = 0;
  my $num_samples = 0;
  my $num_objects = 0;

  eval {
    my @analysis_meta;
    push(@analysis_meta, $self->make_analysis_metadata(\@project_titles));
    push(@analysis_meta, $self->make_creation_metadata($self->affiliation_uri,
                                                       $self->publication_time,
                                                       $self->accountee_uri));
    unless ($irods->list_collection($target)) {
      $irods->add_collection($target);
    }

    my $coll_path = $irods->put_collection($self->analysis_directory, $target);
    $analysis_coll = WTSI::NPG::iRODS::Collection->new($irods, $coll_path);

    $self->info("Created new collection '", $analysis_coll->str, "'");

    my @uuid_meta = grep { $_->[0] eq $self->analysis_uuid_attr }
      @analysis_meta;
    $analysis_uuid = $uuid_meta[0]->[1];

    if ($analysis_uuid) {
      $self->info("Publishing new analysis with UUID ''$analysis_uuid");
    }
    else {
      $self->logconfess("Failed to find the new analysis_uuid in metadata: [",
                        join ", ", @uuid_meta, "]");
    }

    foreach my $project_title (@project_titles) {
      # Find the samples included at the analysis stage
      my %included_samples = $self->_make_included_sample_table($project_title);

      my $num_included = scalar keys %included_samples;
      if ($num_included == 0) {
        $self->logcroak("There were no samples marked for inclusion in the ",
                        "pipeline database. Aborting.");
      }

      my %studies_seen;

      foreach my $included_sample_name (sort keys %included_samples) {
        my $sample = $included_samples{$included_sample_name};

        my @sample_objects = $irods->find_objects_by_meta
          ($self->sample_archive,
           [$self->dcterms_title_attr             => $project_title],
           [$self->infinium_beadchip_attr         => $sample->beadchip],
           [$self->infinium_beadchip_section_attr => $sample->rowcol]);

        unless (@sample_objects) {
          $self->logconfess("Failed to find data in iRODS in sample archive '",
                            $self->sample_archive, "' for sample ",
                            "'$included_sample_name' in project ",
                            "'$project_title'");
        }

        # Should be triplets of 1x gtc plus 2x idat files for each sample
        foreach my $sample_object (@sample_objects) {
          my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $sample_object);

          # Xref analysis to sample studies
          my @studies = map { $_->{value} }
            $obj->find_in_metadata($self->study_id_attr);

          if (@studies) {
            $self->debug("Sample '$included_sample_name' has metadata for ",
                         "studies [", join(", ", @studies), "]");

            foreach my $study (@studies) {
              unless (exists $studies_seen{$study}) {
                push(@analysis_meta, [$self->study_id_attr => $study]);
                $studies_seen{$study}++;
              }
            }
          }
          else {
            $self->logconfess("Failed to find a study_id in iRODS for sample ",
                              "'$included_sample_name' data object ",
                              "'", $obj->str, "' in project '$project_title'");
          }

          # Xref samples to analysis UUID
          $obj->add_avu($self->analysis_uuid_attr, $analysis_uuid);
          ++$num_objects;
        }

        ++$num_samples;

        $self->info("Cross-referenced $num_samples/$num_included samples ",
                    "in project '$project_title'")
      }

      ++$num_projects;
    }

    foreach my $m (@analysis_meta) {
      my ($attribute, $value, $units) = @$m;
      $analysis_coll->add_avu($attribute, $value, $units);
    }

    my @groups = $analysis_coll->expected_groups;
    $analysis_coll->set_content_permissions('read', @groups);
  };

  if ($@) {
    $self->error("Failed to publish: ", $@);
    undef $analysis_uuid;
  }
  else {
    $self->info("Published '", $self->analysis_directory, "' to '",
                $analysis_coll->str, "' and cross-referenced $num_objects ",
                "data objects for $num_samples samples in ",
                "$num_projects projects");
  }

  return $analysis_uuid;
}

# Find samples marked as excluded during the analysis, keyed by their
# name in the Infinium LIMS
sub _make_included_sample_table {
  my ($self, $project_title) = @_;

  my %sample_table;

  my @samples = $self->pipe_db->sample->search
    ({'piperun.name'       => $self->run_name,
      'dataset.if_project' => $project_title,
      'me.include'         => 1},
     {join => {dataset => 'piperun'}});

  foreach my $sample (@samples) {
    $sample_table{$sample->name} = $sample;
  }

  return %sample_table;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2013 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
