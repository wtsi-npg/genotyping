
package WTSI::NPG::Addressable;

use Moose::Role;

with 'WTSI::NPG::Loggable';

has 'size' => (is => 'ro', isa => 'Int', required => 1,
               builder => '_build_size', lazy => 1);

has 'addresses' => (is => 'ro', isa => 'ArrayRef[Str]', required => 1,
                    builder => '_build_addresses', lazy => 1);

has 'content' => (is => 'ro', isa => 'HashRef',
                  default => sub { {} },
                  writer => '_write_content');

sub lookup {
  my ($self, $address) = @_;

  defined $address or
    $self->logconfess("A defined address argument is required");

  exists $self->content->{$address} or
    $self->logcroak("Unknown address '$address'");

  return $self->content->{$address};
}

sub _build_size {
  my ($self) = @_;

  return scalar keys %{$self->content};
}

sub _build_addresses {
  my ($self) = @_;

  return [sort keys %{$self->content}];
}

1;

no Moose;

__END__

=head1 NAME

Addressable - a role which provides a lookup table of contents keyed
on strings.

=head1 DESCRIPTION

Provides a content hash and methods to report its size and return it,
its addresses (keys) and content by address.

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
