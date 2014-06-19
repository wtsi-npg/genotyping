
use utf8;

package WTSI::NPG::iRODS::Communicator;

use Moose;

with 'WTSI::NPG::Startable', 'WTSI::NPG::JSONCodec';

sub communicate {
  my ($self, $spec) = @_;

  my $json = $self->encode($spec);
  ${$self->stdin} .= $json;
  ${$self->stderr} = '';

  $self->debug("Sending JSON spec $json to ", $self->executable);

  my $response;

  eval {
    # baton sends JSON responses on a single line
    $self->harness->pump until ${$self->stdout} =~ m{[\r\n]$};
    $response = $self->decode(${$self->stdout});
    ${$self->stdout} = '';
  };

  if ($@) {
    $self->error("JSON parse error on: '", ${$self->stdout}, "': ", $@);
  }

  defined $response or
    $self->logconfess("Failed to get a response from JSON spec '$json'");

  $self->debug("Got a response of ", $self->encode($response));

  return $response;
}

sub validate_response {
  my ($self, $response) = @_;

  # Valid responses are a HashRef or an ArrayRef (in the event of
  # listing a collection).

  my $rtype = ref $response;
  unless ($rtype eq 'HASH' or $rtype eq 'ARRAY') {
    $self->logconfess("Failed to get a HashRef or ArrayRef response; ",
                      "got $rtype");
  }

  return $self;
}

sub report_error {
  my ($self, $response) = @_;

  if (ref $response eq 'HASH' and exists $response->{error}) {
    $self->logconfess($response->{error}->{message}, " Error code: ",
                      $response->{error}->{code});
  }

  return $self;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::iRODS::Communicable

=head1 DESCRIPTION

A client that lists iRODS metadata as JSON.

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
