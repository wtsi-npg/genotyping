use utf8;

package WTSI::NPG::Genotyping::Subscription;

use Moose::Role;
use JSON;
use List::AllUtils qw(all natatime uniq);
use WTSI::NPG::Genotyping::Sequenom::AssayDataObject;
use WTSI::NPG::Genotyping::Sequenom::AssayResultSet;
use WTSI::NPG::Genotyping::VCF::ReferenceFinder;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::Metadata; # has attribute name constants

our $VERSION = '';

our $CHROMOSOME_JSON_ATTR = 'chromosome_json';
our $REF_GENOME_NAME_ATTR = 'reference_name';
our $SNPSET_VERSION_ATTR = 'snpset_version';

# The largest number of bind variables iRODS supports for 'IN'
# queries.
our $BATCH_QUERY_CHUNK_SIZE = 100;

with 'WTSI::DNAP::Utilities::Loggable';

requires 'platform_name'; # subclasses must implement this method

has 'callset' =>
  (is             => 'ro',
   isa            => 'Str',
   documentation  => 'Identifier for the callset read by the Subscriber; '.
                     'used to disambiguate results from multiple subscribers',
   lazy           => 1,
   default        => sub {
       my ($self) = @_;
       return $self->platform_name().'_'.$self->snpset_name;
   },
);

has 'data_path' =>
  (is            => 'ro',
   isa           => 'Str',
   required      => 1,
   default       => sub { return '/' },
   writer        => '_set_data_path',
   documentation => 'The iRODS path under which the input data are found');

has 'irods'      =>
  (is            => 'ro',
   isa           => 'WTSI::NPG::iRODS',
   required      => 1,
   default       => sub { return WTSI::NPG::iRODS->new },
   documentation => 'An iRODS handle');

has 'reference_path' =>
  (is            => 'ro',
   isa           => 'Str',
   required      => 1,
   default       => sub { return '/' },
   writer        => '_set_reference_path',
   documentation => 'The iRODS path under which the reference and ' .
                    'SNP set data are found');

has 'reference_name' =>
  (is            => 'ro',
   isa           => 'Str',
   required      => 1,
   documentation => 'The name of the reference on which the SNP set ' .
                    ' is defined e.g. "Homo sapiens (1000 Genomes)"');

has 'repository' =>
  (is             => 'ro',
   isa            => 'Maybe[Str]',
   default        => $ENV{NPG_REPOSITORY_ROOT},
   documentation  => 'Root directory containing NPG genome references',
);

has 'snpset_name' =>
  (is            => 'ro',
   isa           => 'Str',
   required      => 1,
   documentation => 'The name of the SNP set e.g. "W35961"');

has 'read_snpset_version' =>
  (is            => 'ro',
   isa           => 'Maybe[Str]',
   documentation => 'SNP set version used to read assay results');

has 'write_snpset_version' =>
  (is            => 'ro',
   isa           => 'Maybe[Str]',
   lazy          => 1,
   default       => sub {
       my ($self) = @_;
       return $self->read_snpset_version;
   },
   documentation => 'SNP set version used to write VCF output');

# non-input attributes

has 'read_snpset' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::Genotyping::SNPSet',
   required => 1,
   builder  => '_build_read_snpset',
   lazy     => 1,
   init_arg => undef,
   documentation => 'SNPSet for plex results input');

has 'write_snpset' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::Genotyping::SNPSet',
   required => 1,
   builder  => '_build_write_snpset',
   lazy     => 1,
   init_arg => undef,
   documentation => 'SNPSet for VCF output');

has 'read_snpset_data_object' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::iRODS::DataObject',
   required => 1,
   builder  => '_build_read_snpset_data_object',
   lazy     => 1,
   init_arg => undef,
   documentation => 'Data object to use for generating SNPSet '.
                    'and chromosome lengths');

has 'write_snpset_data_object' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::iRODS::DataObject',
   required => 1,
   builder  => '_build_write_snpset_data_object',
   lazy     => 1,
   init_arg => undef,
   documentation => 'Data object to use for VCF output; may or may not '.
                    'differ from input snpset data object');

has '_chromosome_lengths' =>
  (is       => 'ro',
   isa      => 'HashRef[Int]',
   required => 0,
   builder  => '_build_chromosome_lengths',
   lazy     => 1,
   init_arg => undef,
   documentation => 'Find chromosome lengths for the SNPSet in iRODS, '.
                    'to be used in VCF generation');

has '_plex_name_attr' =>
  (is            => 'ro',
   isa           => 'Str',
   init_arg      => undef,
   documentation => 'iRODS attribute for QC plex name; varies by plex type');


sub BUILD {
  my ($self) = @_;

  # Make our irods handle use our logger by default
  $self->irods->logger($self->logger);

  # Ensure that the iRODS path is absolute so that its zone can be
  # determined.
  my $abs_ref_path = $self->irods->absolute_path($self->reference_path);
  $self->_set_reference_path($abs_ref_path);

  my $abs_data_path = $self->irods->absolute_path($self->data_path);
  $self->_set_data_path($abs_data_path);

  if (!$self->snpset_name) {
      $self->logcroak("Must have a non-null snpset_name argument");
  } elsif (!$self->reference_name) {
      $self->logcroak("Must have a non-null reference_name argument");
  }

  # test repository directory
  # Moose does not appear to assign default until after BUILD is run
  my $test_repository;
  if (defined($self->repository)) {
      $test_repository = $self->repository;
  } else {
      $test_repository = $ENV{'NPG_REPOSITORY_ROOT'};
  }
  unless (defined($test_repository)) {
      $self->logcroak("Test respository for Subscription.pm not defined; ",
                      "need to specify the NPG_REPOSITORY_ROOT environment ",
                      "variable?");
  }
  unless (-d $test_repository) {
      $self->logcroak("Repository '", $test_repository,
                      "' does not exist or is not a directory");
  }

  # ensure that snpset attributes are valid
  # attributes are lazy; want to die at object creation time, not in a
  # subsequent method call
  my $read_data_obj = $self->read_snpset_data_object;
  my $write_data_obj = $self->write_snpset_data_object;

}


=head2 get_chromosome_lengths

  Arg [1]    : None
  Example    : my $lengths = $sub->get_chromosome_lengths();
  Description: Accessor method for the chromosome lengths attribute. Used
               to get input for a VCF::AssayResultParser constructor.
  Returntype : HashRef[Int]

=cut

sub get_chromosome_lengths {
    my ($self) = @_;
    return $self->_chromosome_lengths;
}


=head2 find_object_paths

  Arg [1]    : ArrayRef[Str] of sample identifiers
  Arg [2..]  : Optional specs for iRODS metadata query
  Example    : my @object_paths = $sub->find_object_paths();
  Description: Find paths for data objects in iRODS corresponding to the
               given sample identifiers.
  Returntype : Array[Str]

=cut

sub find_object_paths {
    my ($self, $sample_identifiers, @query_specs) = @_;

    defined $sample_identifiers or
        $self->logconfess('A defined sample_identifiers ',
                          'argument is required');
    ref $sample_identifiers eq 'ARRAY' or
        $self->logconfess('The sample_identifiers argument ',
                          'must be an ArrayRef');
    _are_unique($sample_identifiers) or
        $self->logconfess('The sample_identifiers argument contained ',
                          'duplicate values: [',
                          join(q{, }, @$sample_identifiers), ']');

    my $num_samples = scalar @$sample_identifiers;
    my $chunk_size = $BATCH_QUERY_CHUNK_SIZE;

    $self->debug("Getting results for $num_samples samples in chunks of ",
                 $chunk_size);

    my @obj_paths;
    my $iter = natatime $chunk_size, @$sample_identifiers;
    while (my @ids = $iter->()) {
        my @id_obj_paths = $self->irods->find_objects_by_meta
            ($self->data_path,
             [$self->_plex_name_attr => $self->snpset_name],
             [$DCTERMS_IDENTIFIER => \@ids, 'in'], @query_specs);
        push @obj_paths, @id_obj_paths;
    }
    return @obj_paths;
}


=head2 find_resultsets_index

  Arg [1]    : ArrayRef[AssayResultSet] of QC plex AssayResultSets
  Arg [2]    : ArrayRef[Str] of sample identifiers
  Example    : my $ri = $sub->find_resultsets_index($resultsets, $sample_ids);
  Description: Index the results by sample identifier. The identifier is
               guaranteed to be present in the metadata because it was in the
               search criteria. No assumptions can be made about the order in
               which iRODS has returned the results, with respect to the
               arguments of the 'IN' clause.
  Returntype : HashRef[ArrayRef[AssayResultSet]]

=cut

sub find_resultsets_index {
    my ($self, $resultsets, $sample_identifiers) = @_;
    my %resultsets_index;
    foreach my $sample_identifier (@$sample_identifiers) {
        unless (exists $resultsets_index{$sample_identifier}) {
            $resultsets_index{$sample_identifier} = [];
        }
        my @sample_resultsets = grep {
            $_->data_object->get_avu($DCTERMS_IDENTIFIER,
                                     $sample_identifier) } @{$resultsets};
        $self->debug("Found ", scalar @sample_resultsets, " resultsets for ",
                     "sample '$sample_identifier'");
        # Sanity check that the contents are consistent
        my @sample_names = map { $_->canonical_sample_id } @sample_resultsets;
        unless (all { $_ eq $sample_names[0] } @sample_names) {
            $self->logconfess("The resultsets found for sample AVU value ",
                              "'$sample_identifier'",
                              ": did not all have the same ",
                              "sample name in their data: [",
                              join(q{, }, @sample_names), "]");
        }
        push @{$resultsets_index{$sample_identifier}}, @sample_resultsets;
    }
    return \%resultsets_index;
}


=head2 vcf_metadata_from_irods

  Arg [1]    : ArrayRef[DataObject] of iRODS DataObjects
  Example    : my $vcf_meta = $sub->vcf_metadata_from_irods($objects);
  Description: Retrieve iRODS metadata for the given DataObjects and use to
               construct VCF metadata.
  Returntype : HashRef[ArrayRef[Str]]

=cut

sub vcf_metadata_from_irods {
    my ($self, $data_objects) = @_;
    my %vcf_meta;
    $vcf_meta{'callset_name'} = [$self->callset, ];
    foreach my $obj (@{$data_objects}) { # check iRODS metadata
        my @obj_meta = @{$obj->metadata};
        foreach my $pair (@obj_meta) {
            my $key = $pair->{'attribute'};
            my $val = $pair->{'value'};
            if ($key eq 'fluidigm_plex') {
                push @{ $vcf_meta{'plex_type'} }, 'fluidigm';
                push @{ $vcf_meta{'plex_name'} }, $val;
            } elsif ($key eq 'sequenom_plex') {
                push @{ $vcf_meta{'plex_type'} }, 'sequenom';
                push @{ $vcf_meta{'plex_name'} }, $val;
            }
            elsif ($key eq 'reference') {
                my $rf = WTSI::NPG::Genotyping::VCF::ReferenceFinder->new(
                    reference_genome => $val,
                    repository => $self->repository,
                );
                push @{ $vcf_meta{'reference'} }, $rf->get_reference_uri();
            }
        }
    }
    foreach my $key (keys %vcf_meta) {
        my @values = @{$vcf_meta{$key}};
        $vcf_meta{$key} = [ uniq @values ];
    }
    return \%vcf_meta;
}


sub _are_unique {
  my ($args) = @_;

  return scalar @$args == uniq @$args;
}

sub _build_chromosome_lengths {
    my ($self) = @_;
    my $snp_obj = $self->read_snpset_data_object;
    my $chromosome_lengths;
    my @avus = $snp_obj->find_in_metadata($CHROMOSOME_JSON_ATTR);
    if (scalar(@avus)==0) {
        $self->logwarn("Snpset iRODS data object has no value for attribute ",
                      "$CHROMOSOME_JSON_ATTR, returning undef");
    } elsif (scalar(@avus)==1) {
        my %avu = %{ shift(@avus) };
        my $chromosome_json = $avu{'value'};
        my $data_object = WTSI::NPG::iRODS::DataObject->new
            ($self->irods, $chromosome_json);
        $chromosome_lengths = decode_json($data_object->slurp());
    } else {
        $self->logcroak("Cannot have more than one $CHROMOSOME_JSON_ATTR ",
                        "value in iRODS metadata for SNP set file");
    }
    return $chromosome_lengths;
}

sub _build_read_snpset {
    my ($self) = @_;
    return WTSI::NPG::Genotyping::SNPSet->new(
        $self->read_snpset_data_object);
}

sub _build_write_snpset {
    my ($self) = @_;
    return WTSI::NPG::Genotyping::SNPSet->new(
        $self->write_snpset_data_object);
}

sub _build_read_snpset_data_object {
   my ($self) = @_;
   return $self->_find_snpset_data_object(
       $self->reference_path,
       $self->reference_name,
       $self->snpset_name,
       $self->read_snpset_version
   );
}

sub _build_write_snpset_data_object {
   my ($self) = @_;
   my $write_data_obj;
   if (defined($self->read_snpset_version) &&
           defined($self->write_snpset_version) &&
           $self->read_snpset_version ne $self->write_snpset_version) {
       $write_data_obj = $self->_find_snpset_data_object(
           $self->reference_path,
           $self->reference_name,
           $self->snpset_name,
           $self->write_snpset_version
       );
   } else {
       $write_data_obj = $self->read_snpset_data_object;
   }
   return $write_data_obj;
}

sub _find_snpset_data_object {
    my ($self, $ref_path, $ref_name, $snpset_name, $snpset_version) = @_;
    unless ($ref_path && $ref_name && $snpset_name) {
        $self->logcroak("Missing argument(s) to find_snpset_data_object");
    }
    my @imeta_args =
       ($ref_path,
        [$self->_plex_name_attr  => $snpset_name],
        [$REF_GENOME_NAME_ATTR   => $ref_name],
    );
    if (defined($snpset_version)) {
        push @imeta_args, [$SNPSET_VERSION_ATTR => $snpset_version];
    }
    my @obj_paths = $self->irods->find_objects_by_meta(@imeta_args);
    my $num_snpsets = scalar @obj_paths;
    unless (defined($snpset_version)) {
        $snpset_version = '(not supplied)';
    }
    if ($num_snpsets > 1) {
        $self->logconfess("The SNP set query for SNP set '$snpset_name' ",
                          "and reference '$ref_name' ",
                          "under reference path '", $ref_path,
                          "', with snpset version ", $snpset_version,
                          " was not specific enough; $num_snpsets SNP sets ",
                          "were returned: [", join(', ',  @obj_paths), "]");
   } elsif ($num_snpsets == 0) {
        $self->logconfess("The SNP set query for SNP set '$snpset_name' ",
                          "and reference '$ref_name' ",
                          "under reference path '", $ref_path,
                          "', with snpset version ", $snpset_version,
                          " did not return any SNP sets");
   }
    my $path = shift @obj_paths;
    return WTSI::NPG::iRODS::DataObject->new($self->irods, $path);
}

no Moose;

1;

__END__

=head1 NAME

Subscription

=head1 DESCRIPTION


Shared attributes and methods for Subscriber classes to retrieve Sequenom or Fluidigm data from iRODS.

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2015, 2016 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
