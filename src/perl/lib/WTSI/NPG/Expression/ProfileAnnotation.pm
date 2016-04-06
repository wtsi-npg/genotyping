use utf8;

package WTSI::NPG::Expression::ProfileAnnotation;

use Moose;

our $VERSION = '';

with 'WTSI::DNAP::Utilities::Loggable', 'WTSI::NPG::iRODS::Storable';

around BUILDARGS => sub {
  my ($orig, $class, @args) = @_;

  # Permit a DataObject as an anonymous argument mapping to data_object
  # Permit a Str as an anonymous argument mapping to file_name
  if (@args == 1 and ref $args[0] eq 'WTSI::NPG::iRODS::DataObject') {
    return $class->$orig(data_object => $args[0]);
  }
  elsif (@args == 1 and !ref $args[0]) {
    return $class->$orig(file_name => $args[0]);
  }
  else {
    return $class->$orig(@_);
  }
};

sub is_valid {
  my ($self) = @_;

  my $fh = $self->_open_content;

  my $stanza  = 0;
  my $version = 0;
  my $profile = 0;
  my $target  = 0;

  my $line_num = 0;
  while (my $line = <$fh>) {
    chomp $line;

    if ($line_num == 0 and $line =~ m{^\[Header\]}msx) {
      $stanza = 1;
    }

    if ($line_num == 1 and $line =~ m{^GSGX\sVersion\s+1.9.0}msx) {
      $version = 1;
    }

    if ($line_num == 7 and $line !~ m{^\[Control\sProbe\sProfile\]}msx) {
      $profile = 1;
    }

    if ($line_num == 8 and $line =~ m{^TargetID}msx) {
      $target = 1;
    }

    $line_num++;

    last if $line_num > 8;
  }

  close $fh or $self->logwarn("Failed to close a file handle to ", $self->str);


  $self->debug("Tested '", $self->str, "' for validity as a ",
               "Profile Annotation: stanza: $stanza, version: $version, ",
               "profile: $profile, target: $target. Lines read: $line_num");

  return ($stanza && $version && $profile && $target);
}

sub _open_content {
  my ($self) = @_;

  my $fh;

  if ($self->data_object) {
    my $content = $self->data_object->slurp;
    open $fh, '<', \$content
      or $self->logconfess("Failed to open content string for reading: $!");
  }
  elsif ($self->file_name) {
    open $fh, '<:encoding(utf8)', $self->file_name or
      $self->$self->logconfess("Failed to open file '", $self->file_name,
                               "' for reading: $!");
  }

  return $fh;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2013, 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
