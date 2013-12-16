
package WTSI::NPG::Accountable;

use Moose::Role;
use URI;

has 'accountee_uid' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1,
   lazy     => 1,
   default  => sub {
     my $uid = `whoami`;
     chomp $uid;
     return $uid;
   });

has 'affiliation_uri' =>
  (is       => 'ro',
   isa      => 'URI',
   required => 1,
   lazy     => 1,
   default  => sub {
     my $uri = URI->new("http:");
     $uri->host('www.sanger.ac.uk');

     return $uri;
   });

=head2 accountee_uri

  Arg [1]    : None

  Example    : my $uri = $obj->accountee_uri;
  Description: Return the LDAP URI of the accountable user.
  Returntype : URI

=cut

sub accountee_uri {
  my ($self) = @_;

  my $uid = $self->accountee_uid;
  my $uri = URI->new("ldap:");
  $uri->host('ldap.internal.sanger.ac.uk');
  $uri->dn('ou=people,dc=sanger,dc=ac,dc=uk');
  $uri->attributes('title');
  $uri->scope('sub');
  $uri->filter("(uid=$uid)");

  return $uri;
}

=head2 accountee_name

  Arg [1]    : None

  Example    : my $name = $obj->accountee_name;
  Description: Return the LDAP name of the accountable user.
  Returntype : Str

=cut

sub accountee_name {
  my ($self) = @_;

  my $uri = $self->accountee_uri;
  my $ldap = Net::LDAP->new($uri->host) or
    $self->logcroak("LDAP connection failed: ", $@);

  my $msg = $ldap->bind;
  $msg->code && $self->logcroak($msg->error);

  $msg = $ldap->search(base   => "ou=people,dc=sanger,dc=ac,dc=uk",
                       filter => $uri->filter);
  $msg->code && $self->logcroak($msg->error);

  my ($name) = ($msg->entries)[0]->get('cn');

  $ldap->unbind;
  $self->logcroak("Failed to find $uri in LDAP") unless $name;

  return $name;
}

no Moose;

1;

__END__

=head1 NAME

Accountable - a role which identifies an agent (the Accountee, a
person or program) who carried out an operation.

=head1 DESCRIPTION

Provides accessors for getting the username of an agent, its WTSI LDAP
URI and its common name as given on the WTSI LDAP server.

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
