use utf8;

package WTSI::NPG::Genotyping::SNP;

use Moose;

has 'name'       => (is => 'ro', isa => 'Str', required => 1);
has 'ref_allele' => (is => 'ro', isa => 'Str', required => 1);
has 'alt_allele' => (is => 'ro', isa => 'Str', required => 1);
has 'chromosome' => (is => 'ro', isa => 'Str', required => 1);
has 'position'   => (is => 'ro', isa => 'Int', required => 1);
has 'strand'     => (is => 'ro', isa => 'Str', required => 1);
has 'str'        => (is => 'ro', isa => 'Str', required => 1);

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__
