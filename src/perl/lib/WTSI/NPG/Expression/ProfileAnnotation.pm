use utf8;

package WTSI::NPG::Expression::ProfileAnnotation;

use Moose;

with 'WTSI::DNAP::Utilities::Loggable', 'WTSI::NPG::Expression::Annotation',
  'WTSI::NPG::iRODS::Storable';

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

    if ($line_num == 0 and $line =~ m{^\[Header\]}) {
      $stanza = 1;
    }

    if ($line_num == 1 and $line =~ m{^GSGX Version\s+1.9.0}) {
      $version = 1;
    }

    if ($line_num == 7 and $line !~ m{^\[Control Probe Profile\]}) {
      $profile = 1;
    }

    if ($line_num == 8 and $line =~ m{^TargetID}) {
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
