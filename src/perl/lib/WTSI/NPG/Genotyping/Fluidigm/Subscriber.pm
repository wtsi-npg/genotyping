
use utf8;

package WTSI::NPG::Genotyping::Fluidigm::Subscriber;

use Moose;

use WTSI::NPG::iRODS;

with 'WTSI::NPG::Loggable';

has 'irods' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::iRODS',
   required => 1,
   default  => sub {
     return WTSI::NPG::iRODS->new;
   });

sub BUILD {
  my ($self) = @_;

  # Make our irods handle use our logger by default
  $self->irods->logger($self->logger);
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

  my @obj_paths = $self->irods->find_objects_by_meta
    ('/',
     [fluidigm_plex        => $snpset_name],
     ['dcterms:identifier' => $sample_identifier], @query_specs);

  my @resultsets;
  foreach my $obj_path (@obj_paths) {
    my $obj = WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new
      ($self->irods, $obj_path);
    push @resultsets,
      WTSI::NPG::Genotyping::Fluidigm::AssayResultSet->new($obj);
  }

  return @resultsets;
}

=head2 get_assay_resultset

  Arg [1]    : Str SNP set name e.g. 'qc'
  Arg [2]    : Str sample identifier (dcterms:identifier)
  Arg [n]    : Optional additional query specs as ArrayRefs.

  Example    : $sub->get_assay_resultset('qc', '0123456789',
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
                      "$num_resultsets result sets were returned");
  }

  return shift @resultsets;
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
