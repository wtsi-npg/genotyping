use utf8;

package WTSI::NPG::Expression::ProfileAnnotation;

use Moose;

use WTSI::NPG::Expression::ProfileAnnotationHint;

with 'WTSI::NPG::Loggable', 'WTSI::NPG::iRODS::Guessable',
  'WTSI::NPG::Expression::Annotation';

has '+hint' => (default => sub {
                  WTSI::NPG::Expression::ProfileAnnotationHint->new;
                });

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

__PACKAGE__->meta->make_immutable;

no Moose;

1;
