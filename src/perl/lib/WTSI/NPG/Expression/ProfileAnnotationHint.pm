use utf8;

package WTSI::NPG::Expression::ProfileAnnotationHint;

use Moose;

with 'WTSI::NPG::iRODS::FileHint';

sub name {
  return 'profile_annotation';
}

sub num_criteria {
  return 3;
}

sub test {
  my ($self, $line, $line_num) = @_;

  my $hint    = '';
  my $continue = 0;

  if ($line_num == 0 and $line =~ m{^\[Header\]}) {
    $hint = $self->name;
    $continue = 1;
  }
  elsif ($line_num == 1 and $line =~ m{^GSGX Version\s+1.9.0}) {
    $hint = $self->name;
    $continue = 1;
  }
  elsif ($line_num == 8 and $line =~ m{^TargetID}) {
    $hint = $self->name;
    $continue = 0;
  }

  return ($hint, $continue);
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;
