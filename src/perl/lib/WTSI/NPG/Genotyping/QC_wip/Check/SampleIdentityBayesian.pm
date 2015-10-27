package WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentityBayesian;

# class to do Bayesian identity computation for a single sample
# Similar to older SampleIdentity class, but metrics are Bayesian probabilities, not simple match/mismatch rates
# Input is a production call (Infinium) and one or more QC calls (Fluidigm/Sequenom) for each SNP in QC plex. If Infinium call and at least one QC call are both non-null, do the Bayesian identity calculation.

use Moose;
use JSON;
use Math::BigFloat;

use WTSI::NPG::Genotyping::Call;

our $VERSION = '';

with 'WTSI::DNAP::Utilities::Loggable';

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

# optional argument

has 'pass_threshold' =>
    (is       => 'ro',
     isa      => 'Num',
     required => 1,
     default  => 0.85);

# Bayesian model parameters

has 'equivalent_calls_probability' => # ECP
    (is            => 'ro',
     isa           => 'HashRef[Num]',
     lazy          => 1,
     builder       => '_build_ecp',
     documentation => 'Probability of equivalent genotype calls on distinct '.
         'samples, for each SNP');

has 'expected_error_rate' => # XER
    (is            => 'ro',
     isa           => 'Num',
     default       => 0.01,
     documentation => 'Expected rate of experimental error; determines '.
         'probability of non-equivalent calls on identical samples');

has 'sample_mismatch_prior' => # SMP
   (is            => 'ro',
    isa           => 'Num',
    default       => 0.01,
    documentation => 'Prior probability of a non-identical sample');


has 'ecp_default' =>
    (is       => 'ro',
     isa      => 'Num',
     required => 1,
     default => 0.40625, # het 50%, maf 25%
     documentation => 'Default probability of equivalent calls for a '.
         'given SNP on distinct samples',
    );

# non-input attributes

has 'concordance' =>
    (is       => 'ro',
     isa      => 'Num',
     builder  => '_build_concordance',
     init_arg => undef,
     lazy     => 1,
     documentation => 'Concordance: Fraction of QC calls equivalent '.
         'to production calls, ignoring any no-calls.');

has 'identity' =>
    (is       => 'ro',
     isa      => 'Num',
     builder  => '_build_identity',
     init_arg => undef,
     lazy     => 1,
     documentation => 'The Bayesian identity metric: Probability that '.
         'production and QC calls derive from the same sample');

has 'production_calls_by_snp' =>
    (is       => 'ro',
     isa      => 'HashRef[WTSI::NPG::Genotyping::Call]',
     builder  => '_build_production_hash',
     init_arg => undef,
     lazy     => 1,
     documentation => 'Production Call objects indexed by SNP name');


has 'qc_calls_by_snp' =>
    (is       => 'ro',
     isa      => 'HashRef[ArrayRef[WTSI::NPG::Genotyping::Call]]',
     builder  => '_build_qc_hash',
     init_arg => undef,
     lazy     => 1,
     documentation => 'ArrayRefs of QC Call objects indexed by SNP name');

## TODO get rid of production_calls and qc_calls attributes?

=head2 assayed

  Arg [1]    : None

  Example    : my $assayed = $si->assayed
  Description: Return true if the sample identity result is from assay,
               rather than a null result due to missing data.
  Returntype : Int

=cut

sub assayed {
  my ($self) = @_;

  return !($self->missing);
}


=head2 failed

  Arg [1]    : None

  Example    : my $failed = $si->failed
  Description: Return true if the sample identity result indicates failure
               due to posterior probability of sample identity greater than
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

=head2 missing

  Arg [1]    : None

  Example    : my $failed = $si->failed
  Description: Return true if the set of QC calls is empty

  Returntype : Int

=cut

sub missing {
  my ($self) = @_;
  # QC calls were missing
  return scalar @{$self->qc_calls} ? 0 : 1;
}

# find swap metric; run identity computation with QC calls of other sample_id object. Swap metric S(A, B) = Probability QC calls from B and production calls from A are from the same sample. May have S(A,B) != S(B,A). Compute both and find the *minimum* probability before checking against swap threshold?

# cross-check with another SampleIdentity object
# compare this object's production calls to other's QC calls, and vice versa
# use to detect possible sample swaps, by pairwise comparison of failed samples


=head2 swap_metric

  Arg [1]    : WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentityBayesian

  Example    : my $swap = $si->swap_metric($other_si)
  Description: Evaluate the swap metric on two sample identity objects,
A and B. Swap occurs if there is high probability that production calls on A
and QC calls on B originate from the same biological sample. Let p_A, q_A be
production, QC calls on A; similarly for p_B, q_B. Let identity metric be
I(p,q) = Pr(identity|p,q). Swap metric is the *maximum* of I(p_A, q_B)
and I(p_B, q_A). Prior probability of mismatch should be higher than for
standard QC comparison (we are doing pairwise comparisons on all failed
samples, most of which will be non-equivalent).
  Returntype : Num

=cut

sub swap_metric {
    my ($self, $other, $prior) = @_;
    unless (defined($prior) && $prior >= 0 && $prior <= 1) {
        $self->logcroak("Must supply a prior mismatch probability ",
                        "between 0 and 1 to swap metric");
    }
    my $id_0 = $self->_id_metric($self->production_calls_by_snp,
                                 $other->qc_calls_by_snp,
                                 $prior,
                             );
    my $id_1 = $self->_id_metric($other->production_calls_by_snp,
                                 $self->qc_calls_by_snp,
                                 $prior,
                             );
    # return *maximum* probability of identity
    if ($id_0 > $id_1) { return $id_0; }
    else { return $id_1; }
}

# convert to a data structure which can be represented in JSON format
sub to_json_spec {
    my ($self) = @_;
    my %spec = (sample_name => $self->sample_name,
                identity    => sprintf("%.4f", $self->identity),
                missing     => $self->missing,
                failed      => $self->assayed ? $self->failed : undef,
                concordance => sprintf("%.4f", $self->concordance));
    my %genotypes;
    # for each SNP, record production call and (zero or more) QC calls
    foreach my $snp (@{$self->snpset->snps}) {
        my $snp_name = $snp->name;
        my $production_call = $self->production_calls_by_snp->{$snp_name};
        my $qc_calls = $self->qc_calls_by_snp->{$snp_name};
        if (defined($production_call) && defined($qc_calls)) {
            my @qc_genotypes;
            foreach my $call_q (@{$qc_calls}) {
                push @qc_genotypes, $call_q->genotype;
            }
            $genotypes{$snp_name} = {
                'production' => $production_call->genotype,
                'qc' => \@qc_genotypes,
            }
        } else {
            $self->info("Call data not found for SNP '", $snp_name,
                        "', sample '", $self->sample_name, "'");
        }
    }
    $spec{genotypes} = \%genotypes;
    return \%spec;
}

sub _build_concordance {
    my ($self) = @_;
    my $n_total = 0;
    my $k_total = 0;
    foreach my $snp_name (keys %{$self->production_calls_by_snp}) {
        my $call_p = $self->production_calls_by_snp->{$snp_name};
        my $calls_q = $self->qc_calls_by_snp->{$snp_name};
        my ($n, $k) = $self->_count_calls($call_p, $calls_q);
        $n_total += $n;
        $k_total += $k;
    }
    my $concordance = 0;
    if ($n_total > 0) { $concordance = $k_total / $n_total; }
    return $concordance;
}

sub _build_identity {
    my ($self) = @_;
    if ($self->missing) {
        return 0;
    } else {
        return $self->_id_metric($self->production_calls_by_snp,
                                 $self->qc_calls_by_snp,
                             );
    }
}

sub _build_production_hash {
    my ($self) = @_;
    my %grouped;
    foreach my $call (@{$self->production_calls}) {
        my $snp_name = $call->snp->name;
        if (defined($grouped{$snp_name})) {
            # TODO support for multiple production calls?
            $self->logcroak("More than one production call for variant '",
                            $snp_name, "'");
        } else {
            $grouped{$snp_name} = $call;
        }
    }
    return \%grouped;
}

sub _build_qc_hash {
    my ($self) = @_;
    my %grouped;
    foreach my $call (@{$self->qc_calls}) {
        my $snp_name = $call->snp->name;
        push @{$grouped{$snp_name}}, $call;
    }
    return \%grouped;
}

# default builder for the 'equivalent calls probability' hash
sub _build_ecp {
    # possible calls: AA, Aa, aa. A = major allele, a = minor allele
    # assume heterozygosity = 50%, MAF=25%
    # so Pr(AA)=0.375, Pr(Aa)=0.5, Pr(aa)=0.125
    my ($self) = @_;
    #my $p = (0.5*0.75)**2 + 0.5**2 + (0.5*0.25)**2; # 0.40625
    my %ecp;
    foreach my $snp (@{$self->snpset->snps}) {
        $ecp{$snp->name} = $self->ecp_default;
    }
    return \%ecp;
}

sub _count_calls {
    # count matching and total non-null pairs of production/QC calls
    my ($self, $call_p, $calls_q) = @_;
    my $n = 0; # total non-null pairs
    my $k = 0; # total equivalent, non-null pairs
    if (defined($call_p) && defined($calls_q)) {
        my $snp = $call_p->snp;
        foreach my $call_q (@{$calls_q}) {
            if (!$snp->equals($call_q->snp)) {
                $self->logcroak("Non-equivalent SNPs in identity ",
                                "count for production SNP '",
                                $snp->name, "'");
            } elsif (!$call_q->is_call) { # QC call is null
                next;
            } elsif ($call_p->equivalent($call_q)) {
                $k++;
            }
            $n++;
        }
    }
    return ($n, $k);
}


# find Pr(identity|calls)
# probability that production/QC calls derive from the same sample
# Pr(H0|D) = (Pr(D|H0) * Pr(H0)) / Pr(D)
# Pr(D) = 1/(Pr(D|H0)*Pr(H0) + Pr(D|H1)*Pr(H1))
# Pr(D|H0)*Pr(H0) = "identity score"
# Pr(D|H1)*Pr(H1) = "non-identity score"
# $calls_p, $calls_q = production, qc call hashes respectively
sub _id_metric {
    my ($self, $calls_by_snp_p, $calls_by_snp_q, $prior) = @_;
    $prior ||= $self->sample_mismatch_prior;
    my $id_score = 1 - $prior;
    my $non_id_score = $prior;
    my $total_calls = 0;
    foreach my $snp (@{$self->snpset->snps}) {
        my ($n, $k) = $self->_count_calls($calls_by_snp_p->{$snp->name},
                                          $calls_by_snp_q->{$snp->name});
        if ($n==0) { next; }
        $total_calls += $n;
        my $call_p = $calls_by_snp_p->{$snp->name};
        if (!defined($call_p)) {
            $self->info("No production call for SNP '", $snp->name, "'");
            next;
        }
        my $calls_q = $calls_by_snp_q->{$snp->name};
        my $id_score_snp = $self->_identity_score_snp($n, $k);
        $id_score = $id_score * $id_score_snp;
        my $nid_score_snp = $self->_non_identity_score_snp($n, $k,
                                                           $snp->name);
        $non_id_score = $non_id_score * $nid_score_snp;

        unless (defined($id_score_snp) && defined($nid_score_snp)) {
            $self->logcroak("Undefined score value");
        }
    }
    my $id_metric;
    if ($total_calls == 0) {
        # record identity as zero (instead of the prior probability)
        $id_metric = 0;
    } else {
        $id_metric = $id_score / ($id_score + $non_id_score);
    }
    return $id_metric;
}

# find the (un-normalised) probability, Pr(d|H0)
# See comments for _non_identity_score_snp
sub _identity_score_snp {
  my ($self, $n, $k) = @_;
  my $p = 1 - $self->expected_error_rate;
  return $self->_binomial($p, $n, $k);
}

# find the (un-normalised) probability, Pr(d|H1), for a given SNP
# take the product across all SNPs to find Pr(D|H1)
# Pr(d|H1) may vary according to allele frequency of each SNP
# D = all SNP calls, d = single SNP call
# H0 = identical samples, H1 = non-identical samples
# inputs: production call, QC calls
# output: Pr(d|H1), or undef if no non-null calls to compare
#
sub _non_identity_score_snp {
    my ($self, $n, $k, $snp_name) = @_;
    my $p = $self->equivalent_calls_probability->{$snp_name};
    unless (defined($p)) {
        $self->logcroak("Probability of equivalent calls on distinct ",
                        "samples not defined for SNP '", $snp_name, "'");
    }
    return $self->_binomial($p, $n, $k);
}

# Pr(D|H_j)*Pr(H_j) = "(non-)identity score"
# inputs: probability of equivalence, total calls, equivalent calls
sub _binomial {
    # what is probability of k equivalent calls in n production/qc pairs?
    # use binomial distribution for k successes in n trials
    # "bnok" = "binomial n over k" function
    # Pr(X=k) = bnok(n,k) * p^k * (1 - p)^(n-k)
    # argument $p = Pr(equivalent calls)
    my ($self, $p, $n, $k) = @_;
    # convert counts to BigFloat objects to compute (n choose k)
    my $k_bf = Math::BigFloat->new($k);
    my $n_bf = Math::BigFloat->new($n);
    $n_bf->bnok($k_bf); # NB BigFloat methods modify the object in place
    my $nok = $n_bf->bstr(); # (n choose k) as regular Perl scalar
    # could compute result as BigFloat, but is such high precision needed?
    my $result = $nok * ($p**$k) * ((1-$p)**($n-$k));
    return $result;
}



__PACKAGE__->meta->make_immutable;

no Moose;

1;
