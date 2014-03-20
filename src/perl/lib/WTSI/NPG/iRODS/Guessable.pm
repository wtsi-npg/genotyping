use utf8;

package WTSI::NPG::iRODS::Guessable;

use List::AllUtils qw(none);
use Moose::Role;

with 'WTSI::NPG::Loggable', 'WTSI::NPG::iRODS::Storable';

has 'hints' =>
  (is       => 'ro',
   isa      => 'ArrayRef[WTSI::NPG::iRODS::FileHint]',
   required => 1,
   lazy     => 1,
   default  => sub { [] });

has 'hint' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::iRODS::FileHint',
   required => 0);

sub BUILD {
  my ($self) = @_;

  # If no hints were provided as initargs, at least ensure that our
  # own hint is in the list to be used for guessing.
  if ($self->hint) {
    $self->add_hint($self->hint);
  }
}

sub add_hint {
  my ($self, $hint) = @_;

  defined $hint or $self->logconfess('A defined hint argument is required');

  if (none { $_->name eq $hint->name } @{$self->hints}) {
    push @{$self->hints}, $hint;
  }

  return $self;
}

sub guess {
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

  my $line_num = 0;
  my %hint_counts;
  my %guesses;

  while (my $line = <$fh>) {
    foreach my $hint (@{$self->hints}) {
      my ($name, $continue) = $hint->test($line, $line_num);
      if ($name) {
        $hint_counts{$name}++;
        $self->debug("Hint '", $self->str, " may be '$name' at line $line_num");

        if ($hint_counts{$name} == $hint->num_criteria) {
          $guesses{$name} = $hint->num_criteria;
          $self->debug("Guessed '", $self->str,
                       "' to be '$name' at line $line_num using ",
                       $hint->num_criteria, " criteria");
        }
      }

      last unless $continue;
    }

    $line_num++;
  }

  close $fh or $self->logwarn("Failed to close handle");

  my $guess = '';

  my @guesses = keys %guesses;

  if (scalar @guesses == 1) {
    $guess = shift @guesses;
  }

  $self->debug("Guessed '", $self->str,
               "' to be '$guess' for hints [",
               join(', ', map { $_->name } @{$self->hints}), "]");

  return $guess eq $self->hint->name;
}

no Moose;

1;
