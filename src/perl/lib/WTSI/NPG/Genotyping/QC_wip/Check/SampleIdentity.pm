
package WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentity;

# class to represent identity for a single sample

use Moose;
use JSON;
use List::Util qw(max);

use WTSI::NPG::Genotyping::Call;

with 'WTSI::DNAP::Utilities::Loggable';

# arguments: unpaired (sample, qc) calls
# do pairing and evaluate match/mismatch
# container for match, mismatch, identity, missing, failed, sample

# required input arguments

has 'sample_name' =>
    (is       => 'ro',
     isa      => 'Str',
     required => 1,);

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

has 'snp_threshold' =>
  (is            => 'ro',
   isa           => 'Int',
   required      => 1,
   documentation => 'The minimum number of shared SNPs, below which ' .
                    'the result is declared omitted.');

# optional arguments

has 'pass_threshold' =>
    (is       => 'ro',
     isa      => 'Num',
     required => 1,
     default  => 0.9);

# non-input attributes

has 'paired_calls' =>
     (is       => 'ro',
      isa      => 'ArrayRef[HashRef[WTSI::NPG::Genotyping::Call]]',
      required => 1,
      builder  => '_pair_sample_calls',
      lazy     => 1);

has 'identity' =>
    (is       => 'rw',
     isa      => 'Num',
     required => 1,
     builder  => '_fraction_of_pairs_matching',
     lazy     => 1);

our $QC_KEY = 'qc';
our $PROD_KEY = 'production';

=head2 assayed

  Arg [1]    : None

  Example    : my $assayed = $si->assayed
  Description: Return true if the sample identity result is from assay,
               rather than a null result due to missing data.
  Returntype : Int

=cut

sub assayed {
  my ($self) = @_;

  return !($self->missing || $self->omitted);
}

=head2 failed

  Arg [1]    : None

  Example    : my $failed = $si->failed
  Description: Return true if the sample identity result indicates failure
               due to the paired calls matching at a level greater than
               the permitted threshold.

  Returntype : Int

=cut

sub failed {
  my ($self) = @_;

  if ($self->assayed) {
    return $self->identity < $self->pass_threshold ? 1 : 0;
  }
  else {
    $self->logconfess("Cannot determine the identity pass/fail state of ",
                      $self->sample_name, " because it has not been assayed");
  }
}

sub omitted {
  my ($self) = @_;
  my $num_qc_calls     = scalar @{$self->qc_calls};
  my $num_paired_calls = scalar @{$self->paired_calls};

  # 'omitted' means that there were QC calls, but too few overlap SNPs
  # overlap with the production calls to perform a test
  return (!$self->missing && $num_paired_calls < $self->snp_threshold) ? 1 : 0;
}

sub missing {
  my ($self) = @_;

  # QC calls were missing
  return scalar @{$self->qc_calls} ? 0 : 1;
}

# cross-check with another SampleIdentity object
# compare this object's production calls to other's QC calls, and vice versa
# use to detect possible sample swaps, by pairwise comparison of failed samples
sub find_swap_metric {
  my ($self, $other) = @_;

  my $metric = $self->_sample_swap_metric($self->paired_calls,
                                          $other->paired_calls);

  $self->debug("Swap metric for ", $self->str, " vs. ",
               $other->str, " is $metric");

  return $metric;
}

# convert to a data structure which can be represented in JSON format
sub to_json_spec {
    my ($self) = @_;
    my %spec = (sample_name => $self->sample_name,
                identity    => $self->identity,
                missing     => $self->missing,
                omitted     => $self->omitted,
                failed      => $self->assayed ? $self->failed : undef);

    my %genotypes;
    foreach my $pair (@{$self->paired_calls}) {
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

sub str {
  my ($self) = @_;

  my $failed_str = $self->assayed ? 'undef' : $self->failed;

  return sprintf "%s identity: %0.2f, missing: %d, omitted: %d, failed: %s",
    $self->sample_name, $self->identity, $self->missing, $self->omitted,
    $failed_str;
}

sub _pair_sample_calls {
  my ($self) = @_;

  my $production_calls = $self->production_calls;
  my $qc_calls         = $self->qc_calls;

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

# Builder for the identity attribute
sub _fraction_of_pairs_matching {
  my ($self) = @_;

  my $frac_matching = 0;
  my @paired_calls = @{$self->paired_calls};
  if (@paired_calls) {
    my $match = sub {
      my $pair = shift;
      return $pair->{$QC_KEY}->equivalent($pair->{$PROD_KEY});
    };

    my @matches = grep { $match->($_) } @paired_calls;

    $frac_matching = scalar @matches / scalar @paired_calls;

    $self->debug("######## ", $self->sample_name, ": ",
                 scalar @matches, "/",
                 scalar @paired_calls, " = $frac_matching");
  }

  return $frac_matching;
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
                            "sample swap metric: ",
                            $pairs_A[$i]{$QC_KEY}->snp->name,
                            " vs. ", $pairs_B[$i]{$QC_KEY}->snp->name);
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
