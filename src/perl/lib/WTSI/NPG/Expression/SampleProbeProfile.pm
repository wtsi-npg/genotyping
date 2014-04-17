use utf8;

package WTSI::NPG::Expression::SampleProbeProfile;

use Moose;

use WTSI::NPG::Expression::ProfileHint;
use WTSI::NPG::Utilities qw(trim);

with 'WTSI::NPG::Loggable', 'WTSI::NPG::iRODS::Guessable',
  'WTSI::NPG::Expression::Annotation';

our $PLATFORM = 'Illumina Inc. GenomeStudio version 1.9.0';
our $NORMALISATION_HEADER_PROPERTY = 'Normalization';

has '+hint' => (default => sub { WTSI::NPG::Expression::ProfileHint->new });

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

sub _build_normalisation_method {
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

  my $platform = <$fh>;
  chomp $platform;
  unless ($platform =~ m{^$PLATFORM}) {
    $self->warn("The Illumina platform '$platform' differs from the expected ",
                "value of '$PLATFORM'.");
  }

  my $normalisation = <$fh>;
  chomp $normalisation;
  my ($prop, $method) = map { trim($_) } split /=/, $normalisation;

  unless (defined $prop   &&
          defined $method &&
          $prop eq $NORMALISATION_HEADER_PROPERTY) {
    $self->logcroak("Failed to determine the normalisation method for '",
                    $self->str, "'; the header property ",
                    "'$NORMALISATION_HEADER_PROPERTY' was not present ",
                    "where expected.")
  }

  close $fh or $self->logwarn("Failed to close a string handle");

  return $method;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;
