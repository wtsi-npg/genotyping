use utf8;

package WTSI::NPG::iRODS::Storable;

use Moose::Role;

has 'file_name' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 0);

has 'data_object' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::iRODS::DataObject',
   required => 0);

after 'BUILD' => sub {
  my ($self) = @_;

  unless ($self->data_object or $self->file_name) {
    $self->logconfess("Neither data_object nor file_name ",
                      "arguments were supplied to the constructor");
  }

  if ($self->data_object and $self->file_name) {
    $self->logconfess("Both data_object '", $self->data_object->str,
                      "' and file_name '", $self->file_name,
                      "' arguments were supplied to the constructor");
  }

  if ($self->data_object) {
    $self->data_object->is_present or
      $self->logconfess("Data object ", $self->data_object->absolute->str,
                        " is not present");
  }

  if ($self->file_name) {
    unless (-e $self->file_name) {
      $self->logconfess("File ", $self->file_name,
                        " is not present");
    }
  }
};

sub BUILD { }

sub str {
  my ($self) = @_;

  if ($self->data_object) {
    return $self->data_object->str;
  }
  else {
    return $self->file_name
  }
}

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::iRODS::Storable - A file which may be either local or
stored in iRODS.

=head1 DESCRIPTION

Represents a data file which may be either present on a local
filesystem, or stored in iRODS as a data object.

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
