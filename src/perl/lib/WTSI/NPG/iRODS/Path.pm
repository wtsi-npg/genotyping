
use utf8;

package WTSI::NPG::iRODS::Path;

use JSON;
use File::Spec;
use Moose::Role;

use WTSI::NPG::Metadata qw($STUDY_ID_META_KEY);
use WTSI::NPG::iRODS;

with 'WTSI::NPG::Loggable', 'WTSI::NPG::Annotatable';

has 'collection' =>
  (is        => 'ro',
   isa       => 'Str',
   required  => 1,
   lazy      => 1,
   default   => '.',
   predicate => 'has_collection');

has 'irods' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::iRODS',
   required => 1);

has 'metadata' => (is        => 'rw',
                   isa       => 'ArrayRef',
                   predicate => 'has_metadata',
                   clearer   => 'clear_metadata');

around BUILDARGS => sub {
  my ($orig, $class, @args) = @_;

  if (@args == 2 && ref $args[0] eq 'WTSI::NPG::iRODS') {
    return $class->$orig(irods      => $args[0],
                         collection => $args[1]);
  }
  else {
    return $class->$orig(@_);
  }
};

sub BUILD {
  my ($self) = @_;

  # Make our logger be the iRODS logger by default
  $self->logger($self->irods->logger);
}

around 'metadata' => sub {
   my ($orig, $self, @args) = @_;

   my @sorted = sort { $a->{attribute} cmp $b->{attribute} ||
                       $a->{value}     cmp $b->{value}     ||
                       $a->{units}     cmp $b->{units} } @{$self->$orig(@args)};
   return \@sorted;
};

=head2 get_avu

  Arg [1]    : attribute
  Arg [2]    : value (optional)
  Arg [2]    : units (optional)

  Example    : $path->get_avu('foo')
  Description: Return a single matching AVU. If multiple candidate AVUs
               match the arguments, an error is raised.
  Returntype : HashRef

=cut

sub get_avu {
  my ($self, $attribute, $value, $units) = @_;
  $attribute or $self->logcroak("An attribute argument is required");

  my @exists = $self->find_in_metadata($attribute, $value, $units);

  my $avu;
  if (@exists) {
    if (scalar @exists == 1) {
      $avu = $exists[0];
    }
    else {
      $value ||= '';
      $units ||= '';

      my $fn = sub {
        my $avu = shift;

        return sprintf("{'%s', '%s', '%s'}", $avu->{attribute}, $avu->{value},
                       $avu->{units});
      };

      my $matched = join ", ", map { $fn->($_) } @exists;

      $self->logconfess("Failed to get a single AVU matching ",
                        "{'$attribute', '$value', '$units'}: ",
                        "matched [$matched]");
    }
  }

  return $avu;
}

=head2 find_in_metadata

  Arg [1]    : attribute
  Arg [2]    : value (optional)
  Arg [2]    : units (optional)

  Example    : my @avus = $path->get_find_in_metadata('foo')
  Description: Return all matching AVUs
  Returntype : Array

=cut

sub find_in_metadata {
  my ($self, $attribute, $value, $units) = @_;
  $attribute or $self->logcroak("An attribute argument is required");

  my @meta = @{$self->metadata};
  my @exists;

  if ($value && $units) {
    @exists = grep { $_->{attribute} eq $attribute &&
                     $_->{value}     eq $value &&
                     $_->{units}     eq $units } @meta;
  }
  elsif ($value) {
    @exists = grep { $_->{attribute} eq $attribute &&
                     $_->{value}     eq $value } @meta;
  }
  else {
    @exists = grep { $_->{attribute} eq $attribute } @meta;
  }

  return @exists;
}

=head2 expected_irods_groups

  Arg [1]    : None

  Example    : @groups = $path->expected_irods_groups
  Description: Return an array of iRODS group names given metadata containing
               >=1 study_id under the key $STUDY_ID_META_KEY
  Returntype : Array

=cut

sub expected_irods_groups {
  my ($self) = @_;

  my @ss_study_avus = $self->find_in_metadata($STUDY_ID_META_KEY);
  unless (@ss_study_avus) {
    $self->logwarn("Did not find any study information in metadata");
  }

  my @groups;
  foreach my $avu (@ss_study_avus) {
    my $study_id = $avu->{value};
    my $group = $self->irods->make_group_name($study_id);
    push(@groups, $group);
  }

  return @groups;
}

sub meta_str {
  my ($self) = @_;

  return $self->json;
}

=head2 meta_json

  Arg [1]    : None

  Example    : $json = $path->meta_json
  Description: Return all metadata as UTF-8 encoded JSON.
  Returntype : Str

=cut

sub meta_json {
  my ($self) = @_;

  return JSON->new->utf8->encode($self->metadata);
}

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::iRODS::Path - The base class for representing iRODS
collections and data objects.

=head1 DESCRIPTION

Represents the features common to all iRODS paths; the collection and
the metadata.

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
