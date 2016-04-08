use utf8;

package WTSI::NPG::Expression::SampleProbeProfile;

use Moose;

use WTSI::NPG::Utilities qw(trim);

our $VERSION = '';

with 'WTSI::DNAP::Utilities::Loggable', 'WTSI::NPG::iRODS::Storable';

our $NORMALISATION_HEADER_PROPERTY = 'Normalization';

has 'normalisation_method' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1,
   builder  => '_build_normalisation_method',
   lazy     => 1);

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

  my $version = 0;
  my $norm    = 0;
  my $content = 0;

  my $line_num = 0;
  while (my $line = <$fh>) {
    chomp $line;

    if ($line_num == 0 and
        $line =~ m{^Illumina\sInc.\sGenomeStudio\sversion}msx) {
      $version = 1;
    }

    if ($line_num == 1 and $line =~ m{^Normalization\s=}msx) {
      $norm = 1;
    }

    if ($line_num == 2 and $line =~ m{^Array\sContent\s=}msx) {
      $content = 1;
    }

    $line_num++;

    last if $line_num > 2;
  }

  close $fh or $self->logwarn("Failed to close a file handle to ", $self->str);

  $self->debug("Tested '", $self->str, "' for validity as a ",
               "Sample Probe Profile: version: $version, norm: $norm, ",
               "content: $content. Lines read: $line_num");

  return ($version && $norm && $content);
}

sub _build_normalisation_method {
  my ($self) = @_;

  my $fh = $self->_open_content;

  my $platform = <$fh>;
  chomp $platform;
  unless ($platform =~ m{^Illumina\sInc.\sGenomeStudio\sversion\s1.9.0}msx) {
    $self->warn("The Illumina platform '$platform' differs from the expected ",
                "value of 'Illumina Inc. GenomeStudio version 1.9.0'.");
  }

  my $normalisation = <$fh>;
  chomp $normalisation;
  my ($prop, $method) = map { trim($_) } split /=/msx, $normalisation;

  unless (defined $prop   &&
          defined $method &&
          $prop eq $NORMALISATION_HEADER_PROPERTY) {
    $self->logcroak("Failed to determine the normalisation method for '",
                    $self->str, "'; the header property ",
                    "'$NORMALISATION_HEADER_PROPERTY' was not present ",
                    "where expected.")
  }

  close $fh or $self->logwarn("Failed to close a file handle to ", $self->str);

  return $method;
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
