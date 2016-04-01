
package WTSI::NPG::Genotyping::Fluidigm::Subscriber;

use Moose;
use JSON;
use List::AllUtils qw(all reduce uniq);
use Try::Tiny;

use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::Fluidigm::AssayDataObject;
use WTSI::NPG::Genotyping::Fluidigm::AssayResultSet;
use WTSI::NPG::Genotyping::SNP;
use WTSI::NPG::Genotyping::SNPSet;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::Metadata; # has attribute name constants

our $VERSION = '';

# TODO Remove duplication of $NO_CALL_GENOTYPE in AssayResult.pm
our $NO_CALL_GENOTYPE = 'NN';

our $CHROMOSOME_JSON_ATTR = 'chromosome_json';

with 'WTSI::DNAP::Utilities::Loggable', 'WTSI::NPG::Genotyping::Subscription';

has '_plex_name_attr' =>
  (is            => 'ro',
   isa           => 'Str',
   init_arg      => undef,
   default       => sub {
       my ($self) = @_;
       return $FLUIDIGM_PLEX_NAME;
   },
   lazy          => 1,
   documentation => 'iRODS attribute for QC plex name');


=head2 get_assay_resultsets_and_vcf_metadata

  Arg [1]    : ArrayRef[Str] sample identifier (dcterms:identifier)
  Arg [n]    : Optional additional query specs as ArrayRefs.

  Example    : $sub->get_assay_resultsets(['0123456789'], [study => 12345]);
  Description: Fetch assay result sets by SNP set, sample and other optional
               criteria. Also finds associated VCF metadata.
  Returntype : - HashRef[ArrayRef[
                  WTSI::NPG::Genotyping::Fluidigm::AssayResultSet]] indexed
               by sample identifier. Each ArrayRef will usually contain one
               item, but may contain more if multiple assays match the
               search criteria.
               - HashRef[ArrayRef[Str]] containing VCF metadata.

=cut

sub get_assay_resultsets_and_vcf_metadata {
  my ($self, $sample_identifiers, @query_specs) = @_;

  my @obj_paths =
      $self->find_object_paths($sample_identifiers, @query_specs);

  # generate array of AssayDataObjects
  # use to construct indexed AssayResultSets *and* find metadata
  my @data_objects = map {
      WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new
            ($self->irods, $_);
  } @obj_paths;
  my @resultsets = map {
      WTSI::NPG::Genotyping::Fluidigm::AssayResultSet->new($_);
  } @data_objects;
  my $resultsets_index =
      $self->find_resultsets_index(\@resultsets, $sample_identifiers);
  my $vcf_meta = $self->vcf_metadata_from_irods(\@data_objects);
  return ($resultsets_index, $vcf_meta);
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

  my @sample_names = map { $_->canonical_sample_id } @resultsets;
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


=head2 platform_name

  Arg [1] : None
  Example : my $name = $sub->platform_name();
  Description: Return an identifier string for the genotyping platform;
               in this case, 'fluidigm'. Used to construct a default
               callset name in the Subscription role.
  Returntype : Str

=cut

sub platform_name {
    return 'fluidigm';
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

      my $snp = $self->read_snpset->named_snp($result->snp_assayed);
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

Keith James <kdj@sanger.ac.uk>, Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2014, 2015, 2016 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
