use utf8;

package WTSI::NPG::Genotyping::Subscription;

use Moose::Role;
use JSON;
use List::AllUtils qw(uniq);
use WTSI::NPG::Genotyping::Sequenom::AssayDataObject;
use WTSI::NPG::Genotyping::Sequenom::AssayResultSet;
use WTSI::NPG::iRODS;

our $VERSION = '';

our $CHROMOSOME_JSON_ATTR = 'chromosome_json';

with 'WTSI::DNAP::Utilities::Loggable';

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
   documentation => 'The name of the SNP set e.g. "W35961"');

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

has '_snpset' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::Genotyping::SNPSet',
   required => 1,
   builder  => '_build_snpset',
   lazy     => 1,
   init_arg => undef);

has '_snpset_data_object' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::iRODS::DataObject',
   required => 1,
   builder  => '_build_snpset_data_object',
   lazy     => 1,
   init_arg => undef,
   documentation => 'Data object to use for generating SNPSet '.
                    'and chromosome lengths');

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


=head2 get_snpset

  Arg [1]    : None
  Example    : my $snpset = $sub->get_snpset();
  Description: Accessor method for the snpset attribute. Used to get a
               snpset for input to a VCF::AssayResultParser constructor.
  Returntype : WTSI::NPG::Genotyping::SNPSet

=cut

sub get_snpset {
    my ($self) = @_;
    return $self->_snpset;
}


sub _are_unique {
  my ($args) = @_;

  return scalar @$args == uniq @$args;
}

sub _build_chromosome_lengths {
    my ($self) = @_;
    my $snp_obj = $self->_snpset_data_object;
    my $chromosome_lengths;
    my @avus = $snp_obj->find_in_metadata($CHROMOSOME_JSON_ATTR);
    if (scalar(@avus)==0) {
        $self->logwarn("No value found for snpset attribute ",
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

sub _build_snpset {
   my ($self) = @_;
   return WTSI::NPG::Genotyping::SNPSet->new($self->_snpset_data_object);
}

sub _build_snpset_data_object {
   my ($self) = @_;

   my $snpset_name = $self->snpset_name;
   my $reference_name = $self->reference_name;

   my @obj_paths = $self->irods->find_objects_by_meta
     ($self->reference_path,
      [$self->_plex_name_attr            => $snpset_name],
      [$self->reference_genome_name_attr => $reference_name]);

   my $num_snpsets = scalar @obj_paths;
   if ($num_snpsets > 1) {
       $self->logconfess("The SNP set query for SNP set '$snpset_name' ",
                         "and reference '$reference_name' ",
                         "under reference path '", $self->reference_path,
                         "' was not specific enough; $num_snpsets SNP sets ",
                         "were returned: [", join(', ',  @obj_paths), "]");
   } elsif ($num_snpsets == 0) {
        $self->logconfess("The SNP set query for SNP set '$snpset_name' ",
                         "and reference '$reference_name' ",
                         "under reference path '", $self->reference_path,
                         "' did not return any SNP sets");
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

Copyright (c) 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
