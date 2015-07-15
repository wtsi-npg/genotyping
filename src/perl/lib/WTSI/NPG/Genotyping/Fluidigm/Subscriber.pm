
package WTSI::NPG::Genotyping::Fluidigm::Subscriber;

use Moose;
use List::AllUtils qw(all natatime reduce uniq);
use Try::Tiny;

use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::Fluidigm::AssayDataObject;
use WTSI::NPG::Genotyping::Fluidigm::AssayResultSet;
use WTSI::NPG::Genotyping::SNP;
use WTSI::NPG::Genotyping::SNPSet;
use WTSI::NPG::iRODS;

our $VERSION = '';

# TODO Remove duplication of $NO_CALL_GENOTYPE in AssayResult.pm
our $NO_CALL_GENOTYPE = 'NN';

# The largest number of bind variables iRODS supports for 'IN'
# queries.
our $BATCH_QUERY_CHUNK_SIZE = 100;

with 'WTSI::DNAP::Utilities::Loggable', 'WTSI::NPG::Annotation',
  'WTSI::NPG::Genotyping::Annotation';

has 'data_path' =>
  (is            => 'ro',
   isa           => 'Str',
   required      => 1,
   default       => sub { return '/' },
   writer        => '_set_data_path',
   documentation => 'The iRODS path under which the raw data are found');

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

has 'snpset_name' =>
  (is            => 'ro',
   isa           => 'Str',
   required      => 1,
   documentation => 'The name of the SNP set e.g. "qc"');

has '_snpset' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::Genotyping::SNPSet',
   required => 1,
   builder  => '_build_snpset',
   lazy     => 1,
   init_arg => undef);

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
}

=head2 get_assay_resultset

  Arg [1] : Str sample identifier (dcterms:identifier)
  Arg [n] : Optional additional query specs as ArrayRefs.
  Example : $sub->get_assay_resultset('qc', '0123456789',
                                      [study => 12345]);
  Description: Fetch an assay result by SNP set, sample and other optional
               criteria. Raises an error if the query finds >1 result set.
  Returntype : WTSI::NPG::Genotyping::Fluidigm::AssayResultSet

=cut

sub get_assay_resultset {
    my ($self, $sample_identifier, @query_specs) = @_;
    my $resultsets = $self->get_assay_resultsets
      ([$sample_identifier], @query_specs);

    my $num_samples = scalar keys %$resultsets;
    if ($num_samples > 1) {
      $self->logconfess("The assay results query returned data for >1 ",
                        "sample: [", join(q{, }, keys %$resultsets, "]"));
    }

    my @resultsets = @{$resultsets->{$sample_identifier}};
    my $num_resultsets = scalar @resultsets;
    if ($num_resultsets > 1) {
      $self->logconfess("The assay results query was not specific enough; ",
                        "$num_resultsets result sets were returned: [",
                        join(q{, }, map { $_->str } @resultsets), "]");
    }

    return shift @resultsets;
}

=head2 get_assay_resultsets

  Arg [1]    : ArrayRef[Str] sample identifier (dcterms:identifier)
  Arg [n]    : Optional additional query specs as ArrayRefs.

  Example    : $sub->get_assay_resultsets(['0123456789'], [study => 12345]);
  Description: Fetch assay result sets by SNP set, sample and other optional
               criteria.
  Returntype : HashRef[ArrayRef[
                 WTSI::NPG::Genotyping::Fluidigm::AssayResultSet]] indexed
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
       [$self->fluidigm_plex_name_attr => $self->snpset_name],
       [$self->dcterms_identifier_attr => \@ids, 'in'], @query_specs);
    push @obj_paths, @id_obj_paths;
  }

  my @resultsets = map {
    WTSI::NPG::Genotyping::Fluidigm::AssayResultSet->new
        (WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new
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
    my @sample_names = map { $_->sample_name } @sample_resultsets;
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

=head2 get_calls

  Arg [1]    : ArrayRef[WTSI::NPG::Genotyping::Fluidigm::AssayResultSet]
  Example    : $sub->get_calls($resultsets);
  Description: Get call objects given the assay results of a single sample.
               Usually there will be only one AssayResultSet. However,
               if the same experiment has been repeated, there may be more.
               Only one call is reported for seach SNP. This is achieved by
               checking that the results for each SNP are concordant across
               the results. If any results disagree, then a warning is emitted
               and a no-call is reported. When a call is compared to a no-call,
               the result of the successful call is accepted.

               This method raises an error if is is asked to compare call
               data from different samples.

  Returntype : ArrayRef[WTSI::NPG::Genotyping::Call]

=cut

sub get_calls {
  my ($self, $assay_resultsets) = @_;

  defined $assay_resultsets or
    $self->logconfess('A defined assay_resultsets argument is required');
  ref $assay_resultsets eq 'ARRAY' or
    $self->logconfess('The assay_resultsets argument must be an ArrayRef');

  my @resultsets = @{$assay_resultsets};
  my $num_resultsets = scalar @resultsets;

  my @sample_names = map { $_->sample_name } @resultsets;
  unless (all { $_ eq $sample_names[0] } @sample_names) {
    $self->logconfess('The assay_resultsets must belong to the same sample: [',
                      join(q{, }, @sample_names, ']'));
  }
  my $sample_name = $sample_names[0];

  my @calls;
  # Want to merge the calls for each result, by assay. i.e. The
  # S01-A01 assay is merged only with another experiment's S01-A01
  # assay, even if other assays are measuring the same SNP as
  # S01-A01.
  my @sizes = map { $_->size } @resultsets;
  unless (all { $_ == $sizes[0] } @sizes) {
    $self->logconfess("Failed to merge $num_resultsets Fluidigm ",
                      "resultsets for sample '$sample_name' ",
                      "because they are different sizes: [",
                      join(q{, }, @sizes), "]");
  }

  my @repeated_calls;
  foreach my $assay_address (@{$resultsets[0]->assay_addresses}) {
    push @repeated_calls,
      $self->_build_calls_at($assay_address, \@resultsets);
  }

  foreach my $calls (@repeated_calls) {
    if (@$calls) {
      my $call;
      try {
        $call = reduce { $a->merge($b) } @$calls;
      } catch {
        my $snp = $calls->[0]->snp;
        my @genotypes = map { $_->genotype } @$calls;

        $self->logwarn("Cannot merge Fluidigm calls [",
                       join(q{, }, @genotypes), "] for sample ",
                       "'$sample_name', SNP '", $snp->name,
                       "', defaulting to no-call: ", $_);
        $call = WTSI::NPG::Genotyping::Call->new
          (genotype => $NO_CALL_GENOTYPE,
           snp      => $snp,
           is_call  => 0);
      };

      push @calls, $call;
    }
  }

  return \@calls;
}

sub _build_snpset {
   my ($self) = @_;

   my $snpset_name = $self->snpset_name;
   my $reference_name = $self->reference_name;

   my @obj_paths = $self->irods->find_objects_by_meta
     ($self->reference_path,
      [$self->fluidigm_plex_name_attr    => $snpset_name],
      [$self->reference_genome_name_attr => $reference_name]);

   my $num_snpsets = scalar @obj_paths;
   if ($num_snpsets > 1) {
     $self->logconfess("The SNP set query for SNP set '$snpset_name' ",
                       "and reference '$reference_name' ",
                       "under reference path '", $self->reference_path,
                       "' was not specific enough; $num_snpsets SNP sets ",
                       "were returned: [", join(', ',  @obj_paths), "]");
   }

   my $path = shift @obj_paths;
   my $obj = WTSI::NPG::iRODS::DataObject->new($self->irods, $path);

   return WTSI::NPG::Genotyping::SNPSet->new($obj);
}

sub _build_calls_at {
  my ($self, $assay_address, $resultsets) = @_;

  $self->debug("Collecting ", scalar @$resultsets, " Fluidigm results ",
               "at address '$assay_address'");

  my @calls;
  foreach my $resultset (@$resultsets) {
    my $result = $resultset->result_at($assay_address);
    if ($result->is_control) {
      $self->trace("Skipping Fluidigm control at '$assay_address'");
    }
    else {
      $self->trace("Adding fluidigm result at '$assay_address'");

      my $snp = $self->_snpset->named_snp($result->snp_assayed);
      push @calls,
        WTSI::NPG::Genotyping::Call->new(snp      => $snp,
                                         genotype => $result->canonical_call,
                                         is_call  => $result->is_call);
    }
  }

  $self->debug("Found ", scalar @calls,
               " Fluidigm calls at address '$assay_address'");

  return \@calls;
}

sub _are_unique {
  my ($args) = @_;

  return scalar @$args == uniq @$args;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::Fluidigm::Subscriber - An iRODS data retriever
for Fluidigm results.

=head1 SYNOPSIS

  my $subscriber = WTSI::NPG::Genotyping::Fluidigm::Subscriber->new
    (irods          => $irods,
     data_path      => '/seq',
     reference_path => '/seq',
     reference_name => 'Homo_sapiens (1000Genomes)',
     snpset_name    => 'qc');

=head1 DESCRIPTION

This class provides methods for retrieving Fluidigm results for
specific samples from iRODS.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2014, 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
