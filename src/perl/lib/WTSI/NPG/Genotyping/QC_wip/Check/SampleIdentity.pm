
package WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentity;

# class to represent identity for a single sample

use Moose;
use JSON;
use List::Util qw(max);

use WTSI::NPG::Genotyping::Call;

use Data::Dumper; # TODO remove when development is stable

with 'WTSI::DNAP::Utilities::Loggable';

# arguments: unpaired (sample, qc) calls
# do pairing and evaluate match/mismatch
# container for match, mismatch, identity, missing, failed, sample

# required input arguments

has 'sample_name' =>
    (is  => 'ro',
     isa => 'Str');

has 'snpset' =>
    (is       => 'ro',
     isa      => 'WTSI::NPG::Genotyping::SNPSet',
     required => 1);

has 'production_calls' =>
    (is       => 'ro',
     isa      => 'ArrayRef[WTSI::NPG::Genotyping::Call]',
     required => 1);

has 'qc_calls' =>
    (is       => 'ro',
     isa      => 'ArrayRef[WTSI::NPG::Genotyping::Call]',
     required => 1);

# optional arguments

has 'pass_threshold' =>
    (is      => 'ro',
     isa     => 'Num',
     default => 0.9);

has 'missing' => # sample not present in QC data
    (is       => 'ro',
     isa      => 'Bool',
     default  => 0);

has 'omitted' => # sample present in QC data, but insufficient shared SNPs
    (is       => 'ro',
     isa      => 'Bool',
     default  => 0);

# non-input attributes

has 'paired_calls' =>
     (is  => 'rw',
      isa => 'ArrayRef[HashRef[WTSI::NPG::Genotyping::Call]]');

has 'identity' =>
    (is  => 'rw',
     isa => 'Num');

has 'failed' =>
    (is  => 'rw',
     isa => 'Bool');

our $QC_KEY = 'qc';
our $PROD_KEY = 'production';

sub BUILD {
    my ($self) = @_;
    $self->paired_calls($self->_pair_sample_calls($self->production_calls,
                                                  $self->qc_calls));
    if (scalar($self->paired_calls) > 0) {
        # argument lists may be empty if sample has no QC data
        my $match = sub {
            my $pair = shift;
            return $pair->{$QC_KEY}->equivalent($pair->{$PROD_KEY});
        };
        my @matches = grep {  $match->($_) } @{$self->get_paired_calls()};
        my $identity = scalar @matches / scalar @{$self->get_paired_calls()};
        $self->identity($identity);
        $self->failed($identity < $self->pass_threshold);
    } else {
        $self->identity(undef);
        $self->failed(undef);
    }
}

# cross-check with another SampleIdentity object
# compare this object's production calls to other's QC calls, and vice versa
# use to detect possible sample swaps, by pairwise comparison of failed samples
sub find_swap_metric {
    my ($self, $other) = @_;
    return $self->_sample_swap_metric($self->get_paired_calls(),
                                            $other->get_paired_calls());
}

sub get_paired_calls {
  my ($self) = @_;
  return $self->paired_calls;
}

sub is_omitted {
    my $self = (@_);
    return $self->omitted;
}

sub is_missing {
    my $self = (@_);
    return $self->missing;
}

# convert to a data structure which can be represented in JSON format
sub to_json_spec {
    my ($self) = @_;
    my %spec = (identity => $self->identity,
                missing  => $self->missing,
                omitted  => $self->omitted,
                failed   => $self->failed);
    my %genotypes;
    foreach my $pair (@{$self->get_paired_calls()}) {
        my $qc_call = $pair->{$QC_KEY};
        my $pd_call = $pair->{$PROD_KEY};
        my $qc_snp = $qc_call->snp->name;
        my $pd_snp = $pd_call->snp->name;

        unless ($qc_snp eq $pd_snp) {
            $self->logconfess("Invalid matched SNPs: QC: ",
                              $qc_snp, ", sample: ", $pd_snp);
        }
        if (exists($genotypes{$qc_snp})) {
            $self->logconfess("Multiple calls for SNP $qc_snp");
        }
        $genotypes{$qc_snp} = [$qc_call->genotype, $pd_call->genotype];
    }
    $spec{genotypes} = \%genotypes;
  return \%spec;
}

# was in Identity.pm
sub _pair_sample_calls {
  my ($self, $production_calls, $qc_calls) = @_;

  defined $production_calls or
    $self->logconfess('A defined production_calls argument is required');
  defined $qc_calls or
    $self->logconfess('A defined qc_calls argument is required');

  my @pairs;
  foreach my $qc_call (@$qc_calls) {
      unless ($self->snpset->contains_snp($qc_call->snp->name)) {
          $self->logconfess("Invalid QC call for comparison; its SNP '",
                            $qc_call->snp->name, "' is not in the SNP set ",
                            "being compared");
      }
      foreach my $production_call (@$production_calls) {
          if ($qc_call->snp->name eq $production_call->snp->name) {
              push @pairs, {$QC_KEY   => $qc_call,
                            $PROD_KEY => $production_call};
          }
      }
  }

  $self->debug("Paired ", scalar @pairs, " calls for '",
               $self->sample_name, "'");

  return \@pairs;
}

# Evaluate a cross-check metric on sample & QC calls for pairs of samples
# Use to warn of possible sample swaps
# Let r_AB = rate of matching calls between (Sample_A, QC_B)
# we may have r_AB != r_BA, so define pairwise metric as max(r_AB, r_BA)
sub _sample_swap_metric {
    my ($self, $pairs_ref_A, $pairs_ref_B) = @_;
    my @pairs_A = @{$pairs_ref_A};
    my @pairs_B = @{$pairs_ref_B};
    my $total_snps = scalar(@pairs_A);
    if ($total_snps != scalar(@pairs_B)) {
        $self->logcroak("Call pair argument lists are of different lengths");
    }
    my @match = (0,0);
    for (my $i=0;$i<$total_snps;$i++) {
        # compare results on both samples for the ith snp
        if ($pairs_A[$i]{$QC_KEY}->snp->name ne
                $pairs_B[$i]{$QC_KEY}->snp->name) {
            $self->logcroak("Mismatched SNP identities for ",
                            "sample swap metric");
        }
        if ($pairs_A[$i]{$QC_KEY}->equivalent($pairs_B[$i]{$PROD_KEY})) {
            $match[0]++;
        }
        if ($pairs_A[$i]{$PROD_KEY}->equivalent($pairs_B[$i]{$QC_KEY})) {
            $match[1]++;
        }
    }
    return max(@match)/$total_snps;
}


no Moose;

1;
