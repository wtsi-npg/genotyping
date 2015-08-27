
package WTSI::NPG::Genotyping::Sequenom::Subscriber;

use Moose;
use List::AllUtils qw(all natatime);

use WTSI::NPG::Genotyping::Sequenom::AssayDataObject;
use WTSI::NPG::Genotyping::Sequenom::AssayResultSet;
use WTSI::NPG::iRODS;

our $VERSION = '';

# The largest number of bind variables iRODS supports for 'IN'
# queries.
our $BATCH_QUERY_CHUNK_SIZE = 100;

with 'WTSI::DNAP::Utilities::Loggable', 'WTSI::NPG::Annotation',
  'WTSI::NPG::Genotyping::Annotation', 'WTSI::NPG::Genotyping::Subscription';

has '_plex_name_attr' =>
  (is            => 'ro',
   isa           => 'Str',
   init_arg      => undef,
   default       => sub {
       my ($self) = @_;
       return $self->sequenom_plex_name_attr;
   },
   lazy          => 1,
   documentation => 'iRODS attribute for QC plex name');


=head2 get_assay_resultsets

  Arg [1]    : ArrayRef[Str] sample identifier (dcterms:identifier)
  Arg [n]    : Optional additional query specs as ArrayRefs.

  Example    : $sub->get_assay_resultsets(['0123456789'], [study => 12345]);
  Description: Fetch assay result sets by SNP set, sample and other optional
               criteria.
  Returntype : HashRef[ArrayRef[
                 WTSI::NPG::Genotyping::Sequenom::AssayResultSet]] indexed
               by sample identifier. Each ArrayRef will usually contain one
               item, but may contain more if multiple assays match the
               search criteria.

=cut

sub get_assay_resultsets {
  my ($self, $sample_identifiers, @query_specs) = @_;

  defined $sample_identifiers or
    $self->logconfess('A defined sample_identifiers argument is required');
  ref $sample_identifiers eq 'ARRAY' or
    $self->logconfess('The sample_identifiers argument must be an ArrayRef');
  _are_unique($sample_identifiers) or
    $self->logconfess('The sample_identifiers argument contained duplicate ',
                      'values: [', join(q{, }, @$sample_identifiers), ']');

  my $num_samples = scalar @$sample_identifiers;
  my $chunk_size = $BATCH_QUERY_CHUNK_SIZE;

  $self->debug("Getting results for $num_samples samples in chunks of ",
               $chunk_size);

  my @obj_paths;
  my $iter = natatime $chunk_size, @$sample_identifiers;
  while (my @ids = $iter->()) {
    my @id_obj_paths = $self->irods->find_objects_by_meta
      ($self->data_path,
       [$self->sequenom_plex_name_attr => $self->snpset_name],
       [$self->dcterms_identifier_attr => \@ids, 'in'], @query_specs);
    push @obj_paths, @id_obj_paths;
  }

  my @resultsets = map {
    WTSI::NPG::Genotyping::Sequenom::AssayResultSet->new
        (WTSI::NPG::Genotyping::Sequenom::AssayDataObject->new
         ($self->irods, $_));
  } @obj_paths;

  # Index the results by sample identifier. The identifier is
  # guaranteed to be present in the metadata because it was in the
  # search criteria. No assumptions can be made about the order in
  # which iRODS has returned the results, with respect to the
  # arguments of the 'IN' clause.
  my %resultsets_index;
  foreach my $sample_identifier (@$sample_identifiers) {
    unless (exists $resultsets_index{$sample_identifier}) {
      $resultsets_index{$sample_identifier} = [];
    }

    my @sample_resultsets = grep {
      $_->data_object->get_avu($self->dcterms_identifier_attr,
                               $sample_identifier) } @resultsets;

    $self->debug("Found ", scalar @sample_resultsets, " resultsets for ",
                 "sample '$sample_identifier'");

    # Sanity check that the contents are consistent
    my @sample_names = map { $_->canonical_sample_id } @sample_resultsets;
    unless (all { $_ eq $sample_names[0] } @sample_names) {
      $self->logconfess("The resultsets found for sample AVU value ",
                        "'$sample_identifier': did not all have the same ",
                        "sample name in their data: [",
                        join(q{, }, @sample_names), "]");
    }

    push @{$resultsets_index{$sample_identifier}}, @sample_resultsets;
  }

  return \%resultsets_index;
}


=head2 get_assay_resultsets_and_vcf_metadata

  Arg [1]    : ArrayRef[Str] sample identifier (dcterms:identifier)
  Arg [n]    : Optional additional query specs as ArrayRefs.

  Example    : $sub->get_assay_resultsets(['0123456789'], [study => 12345]);
  Description: Fetch assay result sets by SNP set, sample and other optional
               criteria. Also finds associated VCF metadata.
  Returntype : - HashRef[ArrayRef[
                  WTSI::NPG::Genotyping::Sequenom::AssayResultSet]] indexed
               by sample identifier. Each ArrayRef will usually contain one
               item, but may contain more if multiple assays match the
               search criteria.
               - HashRef[ArrayRef[Str]] containing VCF metadata.

=cut

sub get_assay_resultsets_and_vcf_metadata {
  my ($self, $sample_identifiers, @query_specs) = @_;

  my @obj_paths =
      $self->_find_object_paths($sample_identifiers, @query_specs);

  # generate array of AssayDataObjects
  # use to construct indexed AssayResultSets *and* find metadata
  my @data_objects = map {
      WTSI::NPG::Genotyping::Sequenom::AssayDataObject->new
            ($self->irods, $_);
  } @obj_paths;
  my @resultsets = map {
      WTSI::NPG::Genotyping::Sequenom::AssayResultSet->new($_);
  } @data_objects;
  my $resultsets_index =
      $self->_find_resultsets_index(\@resultsets, $sample_identifiers);
  my $vcf_meta = $self->_vcf_metadata_from_irods(\@data_objects);
  return ($resultsets_index, $vcf_meta);
}


__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::Sequenom::Subscriber - An iRODS data retriever
for Sequenom results.

Analagous to WTSI::NPG::Genotyping::Fluidigm::Subscriber. Both are used
to retrieve results and write to VCF in ready_qc_calls.pl. Both classes
implement a get_assay_resultsets method.

=head1 SYNOPSIS

  my $subscriber = WTSI::NPG::Genotyping::Sequenom::Subscriber->new
    (irods          => $irods,
     data_path      => '/seq',
     reference_path => '/seq',
     reference_name => 'Homo_sapiens (1000Genomes)',
     snpset_name    => 'qc');

=head1 DESCRIPTION

This class provides methods for retrieving Sequenom results for
specific samples from iRODS.

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
