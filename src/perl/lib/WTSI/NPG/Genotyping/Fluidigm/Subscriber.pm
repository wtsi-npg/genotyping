
use utf8;

package WTSI::NPG::Genotyping::Fluidigm::Subscriber;

use Moose;

use List::AllUtils qw(all reduce);
use Try::Tiny;

use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::Fluidigm::AssayDataObject;
use WTSI::NPG::Genotyping::Fluidigm::AssayResultSet;
use WTSI::NPG::Genotyping::SNP;
use WTSI::NPG::Genotyping::SNPSet;
use WTSI::NPG::iRODS;

with 'WTSI::DNAP::Utilities::Loggable', 'WTSI::NPG::Annotation',
  'WTSI::NPG::Genotyping::Annotation';

has 'data_path' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1,
   default  => sub { return '/' },
   writer   => '_set_data_path');

has 'irods' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::iRODS',
   required => 1,
   default  => sub {
     return WTSI::NPG::iRODS->new;
   });

has 'reference_path' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1,
   default  => sub { return '/' },
   writer   => '_set_reference_path');

has 'snpsets_cache' =>
  (is       => 'ro',
   isa      => 'HashRef[WTSI::NPG::Genotyping::SNPSet]',
   required => 1,
   default  => sub { return {} },
   init_arg => undef);

# TODO Remove duplication of $NO_CALL_GENOTYPE in AssayResult.pm
our $NO_CALL_GENOTYPE = 'NN';

our $RECORD_SEPARATOR = chr(30); # Use ASCII 30 as non-printing separator

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

=head2 get_snpset

  Arg [1]    : Str snpset name e.g. 'qc'
  Arg [2]    : Str reference name e.g. 'Homo sapiens (1000 Genomes)'

  Example    : $set = $subscriber->get_snpset('qc', $reference_name)
  Description: Make a new SNPSet from data in iRODS.
  Returntype : WTSI::NPG::Genotyping::SNPSet

=cut

sub get_snpset {
   my ($self, $snpset_name, $reference_name) = @_;

   $snpset_name or $self->logconfess('The snpset_name argument was empty');
   $reference_name or
     $self->logconfess('The reference_name argument was empty');

   my $cache_key = $snpset_name . $RECORD_SEPARATOR . $reference_name;
   if (exists $self->snpsets_cache->{$cache_key}) {
     $self->debug("Found SNP set '$snpset_name' and reference ",
                  "'$reference_name' in the cache");
   }
   else {
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

     $self->snpsets_cache->{$cache_key} =
       WTSI::NPG::Genotyping::SNPSet->new($obj);
   }

   return $self->snpsets_cache->{$cache_key};
}

=head2 get_assay_resultset

  Arg [1] : Str SNP set name e.g. 'qc'
  Arg [2] : Str sample identifier (dcterms:identifier)
  Arg [n] : Optional additional query specs as ArrayRefs.
  Example : $sub->get_assay_resultset('qc', '0123456789',
                                      [study => 12345]);
  Description: Fetch an assay result by SNP set and sample. Raises an error
               if the query finds >1 result set.
  Returntype : WTSI::NPG::Genotyping::Fluidigm::AssayResultSet

=cut

sub get_assay_resultset {
    my ($self, $snpset_name, $sample_identifier, @query_specs) = @_;
    my @resultsets = $self->get_assay_resultsets
        ($snpset_name, $sample_identifier, @query_specs);
    my $num_resultsets = scalar @resultsets;
    if ($num_resultsets > 1) {
        $self->logconfess("The assay results query was not specific enough; ",
                          "$num_resultsets result sets were returned: [",
                          join(', ', map { $_->str } @resultsets), "]");
    }
    return shift @resultsets;
}


=head2 get_assay_resultsets

  Arg [1]    : Str SNP set name e.g. 'qc'
  Arg [2]    : Str sample identifier (dcterms:identifier)
  Arg [n]    : Optional additional query specs as ArrayRefs.

  Example    : $sub->get_assay_resultsets('qc', '0123456789',
                                          [study => 12345]);
  Description: Fetch assay results by SNP set and sample.
  Returntype : Array of WTSI::NPG::Genotyping::Fluidigm::AssayResultSet

=cut

sub get_assay_resultsets {
  my ($self, $snpset_name, $sample_identifier, @query_specs) = @_;

  $snpset_name or $self->logconfess('The snpset_name argument was empty');
  $sample_identifier or
    $self->logconfess('The sample_identifier argument was empty');

  $self->debug("Finding Fluidigm results for sample '$sample_identifier' ",
               "with plex '$snpset_name'");

  my @obj_paths = $self->irods->find_objects_by_meta
    ($self->data_path,
     [$self->fluidigm_plex_name_attr => $snpset_name],
     [$self->dcterms_identifier_attr => $sample_identifier], @query_specs);

  my @resultsets;
  foreach my $obj_path (@obj_paths) {
    my $obj = WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new
      ($self->irods, $obj_path);
    push @resultsets,
      WTSI::NPG::Genotyping::Fluidigm::AssayResultSet->new($obj);
  }

  return @resultsets;
}

=head2 get_calls

  Arg [1]    : Str reference name e.g. 'Homo sapiens (1000 Genomes)'
  Arg [2]    : Str snpset name e.g. 'qc'
  Arg [3]    : Str sample identifier (dcterms:identifier)
  Arg [n]    : Optional additional query specs as ArrayRefs.

  Example    : $sub->get_calls('Homo sapiens (1000 Genomes)', 'qc',
                               '0123456789', [study => 12345]);
  Description: Get genotype calls mapped to the specified reference for
               a specified SNP multiplex and sample.
  Returntype : ArrayRef[WTSI::NPG::Genotyping::Call]

=cut

sub get_calls {
  my ($self, $reference_name, $snpset_name, $sample_identifier,
      @query_specs) = @_;
  $self->debug("Finding a Fluidigm resultset for sample '$sample_identifier' ",
               "using SNP set '$snpset_name' on reference '$reference_name'");
  my @resultsets = $self->get_assay_resultsets
    ($snpset_name, $sample_identifier, @query_specs);

  my $num_resultsets = scalar @resultsets;

  my @calls;
  if ($num_resultsets == 0) {
    $self->debug("Failed to find a Fluidigm resultset for sample ",
                 "'$sample_identifier' using SNP set '$snpset_name' on ",
                 "reference '$reference_name'");
  }
  else {
    $self->info("Found $num_resultsets Fluidigm resultsets for sample ",
                "'$sample_identifier' using SNP set '$snpset_name' on ",
                "reference '$reference_name'");

    # Want to merge the calls for each result, by assay. i.e. The
    # S01-A01 assay is merged only with another experiment's S01-A01
    # assay, even if other assays are measuring the same SNP as
    # S01-A01.
    my $snpset = $self->get_snpset($snpset_name, $reference_name);

    my @sizes = map { $_->size } @resultsets;
    unless (all { $_ == $sizes[0] } @sizes) {
      $self->logconfess("Failed to merge $num_resultsets Fluidigm resultsets ",
                        "for sample '$sample_identifier' using SNP set ",
                        "'$snpset_name' because they are different sizes: [",
                        join(', ', @sizes), "]");
    }

    my @repeated_calls;
    foreach my $assay_address (@{$resultsets[0]->assay_addresses}) {
      push @repeated_calls,
        $self->_build_calls_at($assay_address, $snpset, \@resultsets);
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
                         join(', ', @genotypes), "] for sample ",
                         "'$sample_identifier', SNP '", $snp->name,
                         "', defaulting to no-call: ", $_);
          $call = WTSI::NPG::Genotyping::Call->new
            (genotype => $NO_CALL_GENOTYPE,
             snp      => $snp,
             is_call  => 0);
        };

        push @calls, $call;
      }
    }
  }

  return \@calls;
}

sub _build_calls_at {
  my ($self, $assay_address, $snpset, $resultsets) = @_;

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

      my $snp = $snpset->named_snp($result->snp_assayed);
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
    (irods => $irods);


=head1 DESCRIPTION

This class provides methods for retrieving Fluidigm results for
specific samples from iRODS.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2014 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
