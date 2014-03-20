use utf8;

package WTSI::NPG::Expression::ProfileHint;

use Moose;

with 'WTSI::NPG::iRODS::FileHint';

sub name {
  return 'genome_studio_profile';
}

sub num_criteria {
  return 2;
}

sub test {
  my ($self, $line, $line_num) = @_;

  my $hint    = '';
  my $continue = 0;

  if ($line_num == 0 and $line =~ m{^Illumina Inc.\sGenomeStudio\sversion}) {
    $hint = $self->name;
    $continue = 1;
  }
  elsif ($line_num == 1 and $line =~ m{^Normalization =}) {
    $hint = $self->name;
    $continue = 0;
  }

  return ($hint, $continue);
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;
