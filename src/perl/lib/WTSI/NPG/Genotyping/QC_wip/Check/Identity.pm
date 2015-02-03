
package WTSI::NPG::Genotyping::QC_wip::Check::Identity;

use Moose;

use plink_binary;

use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::SNP;

use WTSI::NPG::Genotyping::Types qw(:all);

with 'WTSI::DNAP::Utilities::Loggable';

has 'plink' =>
  (is      => 'ro',
   isa     => 'plink_binary::plink_binary',
   required => 1);

has 'snpset'  =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::Genotyping::SNPSet',
   required => 1);

has 'min_shared_snps' =>
  (is      => 'rw',
   isa     => 'Int',
   default => 8);

has 'swap_threshold' =>
  (is      => 'ro',
   isa     => 'Num',
   default => 0.9);

has 'pass_threshold' => # minimum similarity for metric pass
  (is      => 'rw',
   isa     => 'Num',
   default => 0.9);

sub get_num_samples {
  my ($self) = @_;

  return $self->plink->{"individuals"}->size;
}

sub get_sample_names {
  my ($self) = @_;

  my @names;
  for (my $i = 0; $i < $self->get_num_samples; $i++) {
    push @names, $self->plink->{'individuals'}->get($i)->{'name'};
  }

  return \@names;
}

sub get_shared_snp_names {
  my ($self) = @_;

  my %plex_snp_table;
  foreach my $name ($self->snpset->snp_names) {
    $plex_snp_table{$name}++;
  }

  my @shared_names;
  my $num_plink_snps = $self->plink->{'snps'}->size;
  for (my $i = 0; $i < $num_plink_snps; $i++) {
    my $name = $self->plink->{'snps'}->get($i)->{'name'};
    my $converted = _from_illumina_snp_name($name);

    if (exists $plex_snp_table{$converted}) {
      push @shared_names, $converted;
    }
  }

  return \@shared_names;
}

sub get_plink_calls {
  my ($self) = @_;

  my $genotypes = new plink_binary::vectorstr;
  my $snp = new plink_binary::snp;

  my %shared_snp_names;
  foreach my $name (@{$self->get_shared_snp_names}) {
    $shared_snp_names{$name}++;
  }

  my @calls;
  while ($self->plink->next_snp($snp, $genotypes)) {
    my $illumina_name = $snp->{'name'};
    my $real_name     = _from_illumina_snp_name($illumina_name);

    foreach my $name ($illumina_name, $real_name) {
      $self->debug("Checking for SNP '$name' in [",
                   join(', ', keys %shared_snp_names), "]");

      for (my $i = 0; $i < $genotypes->size; $i++) {
        if (exists $shared_snp_names{$name}) {
          my $call = WTSI::NPG::Genotyping::Call->new
            (snp      => $self->snpset->named_snp($name),
             genotype => $genotypes->get($i));

          $self->debug("Plink call: ", $call->str);
          push @calls, $call;
        }
      }
    }
  }

  return \@calls;
}


sub _from_illumina_snp_name {
  my ($name) = @_;

  my ($prefix, $body) = $name =~ m{^(exm-)?(.*)};

  return $body;
}

no Moose;

1;
