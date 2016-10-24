
package WTSI::NPG::Genotyping::QC::BayesianIdentity::Simulator;

use Moose;

use MooseX::Types::Moose qw(Int);

use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::QC::BayesianIdentity::SampleMetric;
use WTSI::NPG::Genotyping::SNPSet;
use WTSI::NPG::Genotyping::Types qw(:all);

our $VERSION = '';

with 'WTSI::DNAP::Utilities::Loggable';

# required arguments

has 'calls' =>
    (is            => 'ro',
     isa           => 'ArrayRef[WTSI::NPG::Genotyping::Call]',
     required      => 1,
     documentation => 'ArrayRef of QC Call objects to generate simulated data'
 );

has 'snpset' =>
    (is            => 'ro',
     isa           => 'WTSI::NPG::Genotyping::SNPSet',
     required      => 1,
     documentation => 'SNPSet for creation of SampleMetric '.
         'objects. Must include all SNPs in the "calls" attribute.');

# optional argument

has 'pass_threshold' =>
    (is            => 'ro',
     isa           => 'Maybe[Num]',
     documentation => 'Minimum posterior probability of identity for '.
         'sample pass');

# optional params for identity calculation
# passed to SampleMetric constructor
# no default values; instead use defaults of SampleMetric class

has 'equivalent_calls_probability' => # ECP
    (is            => 'ro',
     isa           => 'Maybe[HashRef[Num]]',
     documentation => 'Probability of equivalent genotype calls on distinct '.
         'samples, for each SNP');

has 'expected_error_rate' => # XER
    (is            => 'ro',
     isa           => 'Maybe[Num]',
     documentation => 'Expected rate of experimental error; determines '.
         'probability of non-equivalent calls on identical samples');

has 'sample_mismatch_prior' => # SMP
   (is            => 'ro',
    isa           => 'Maybe[Num]',
    documentation => 'Prior probability of a non-identical sample');

# non-input arguments

has 'total_calls' =>
    (is           => 'ro',
     isa          => 'Int',
     lazy         => 1,
     default      => sub { my ($self) = @_; return scalar @{$self->calls} },
     init_arg      => undef,
 );

has '_identity_params' =>
    (is            => 'ro',
     isa           => 'HashRef',
     lazy          => 1,
     builder       => '_build_identity_params',
     init_arg      => undef,
 );

our $DUMMY_SAMPLE_NAME = 'dummy_sample';

# class to generate simulated results for the Bayesian identity check
# create fake production and QC calls
# record concordance and identity to generate plots



=head2 find_identity_vary_ecp

  Arg [1]    : Maybe[Num]
  Arg [2]    : Maybe[Num]
  Arg [3]    : Maybe[Int]

  Example    : $results = $simulator->find_identity_vary_ecp(0, 0.05, 20);
  Description: Find concordance and identity for different values of the
               Equivalent Calls Probability (ECP) parameter. The arguments
               control the range of ECP to be used.
  Returntype : ArrayRef[ArrayRef[Num]]

=cut


sub find_identity_vary_ecp {
    # vary equivalent calls probability (ecp)
    # probability of equivalent calls on different samples
    my ($self, $start, $incr, $total) = @_;
    $start ||= 0;
    $incr ||= 0.05;
    $total ||= 20;
    $self->info("ECP: Start = $start, increment = $incr, total = $total");
    my $ecps = $self->_generate_variable_list($start, $incr, $total);
    my $params = $self->_identity_params;
    my @results;
    my $i = $start;
    foreach my $ecp (@{$ecps}) {
        $params->{ecp_default} = $ecp;
        my $equivalent = 0;
        while ($equivalent <= $self->total_calls) {
            my $id = $self->_find_identity($self->calls, $params, $equivalent,
                                           $self->total_calls);
            my $concord = $equivalent / $self->total_calls;
            push @results, [$ecp, $concord, $id];
            $equivalent++;
        }
        $i += $incr;
    }
    return \@results;
}


=head2 find_identity_vary_qcr

  Arg [1]    : Maybe[Num]
  Arg [2]    : Maybe[Num]
  Arg [3]    : Maybe[Int]

  Example    : $results = $simulator->find_identity_vary_qcr(1, 1, 4);
  Description: Find concordance and identity for different values of the
               QC Runs (QCR) parameter. The arguments control the range of
               QCR to be used.
  Returntype : ArrayRef[ArrayRef[Num]]

=cut


sub find_identity_vary_qcr {
    # vary number of (identical) QC runs
    my ($self, $start, $incr, $total) = @_;
    $start ||= 1;
    $incr ||= 1;
    $total ||= 4;
    $self->info("QCR: Start = $start, increment = $incr, total = $total");
    unless (is_Int($start) && is_Int($incr) && is_Int($total)) {
        $self->logcroak("Number of QC runs must be an integer");
    }
    my $min = 1;
    my $max = $start + $incr*$total + 1;
    my $qc_totals = $self->_generate_variable_list($start, $incr,
                                                   $total, $min, $max);
    my @results;
    foreach my $qc_total (@{$qc_totals}) {
        my $equivalent = 0;
        while ($equivalent <= $self->total_calls) {
            my $id = $self->_find_identity($self->calls,
                                           $self->_identity_params,
                                           $equivalent,
                                           $self->total_calls,
                                           $qc_total);
            my $concord = $equivalent / $self->total_calls;
            push @results, [$qc_total, $concord, $id];
            $equivalent++;
        }
    }
    return \@results;
}


=head2 find_identity_vary_qcs

  Arg [1]    : Maybe[Num]
  Arg [2]    : Maybe[Num]
  Arg [3]    : Maybe[Int]

  Example    : $results = $simulator->find_identity_vary_qcs(4, 1, 21);
  Description: Find concordance and identity for different values of the
               total QC SNPs (QCS) parameter. The arguments control the
               range of QCS to be used.
  Returntype : ArrayRef[ArrayRef[Num]]

=cut

sub find_identity_vary_qcs {
    # vary the number of QC SNP calls
    my ($self, $start, $incr, $total) = @_;
    $start ||= 4;
    $incr ||= 1;
    $total ||= 21;
    $self->info("QCS: Start = $start, increment = $incr, total = $total");
    unless (is_Int($start) && is_Int($incr) && is_Int($total)) {
        $self->logcroak("Number of QC SNPs must be an integer");
    }
    my $min = 1;
    my $max = $self->total_calls; # QC SNPs cannot exceed total calls
    my $qcs_list = $self->_generate_variable_list($start, $incr,
                                                  $total, $min, $max);
    my @results;
    foreach my $qcs (@{$qcs_list}) {
        my $equivalent = 0;
        while ($equivalent <= $qcs) {
            my $id = $self->_find_identity($self->calls,
                                           $self->_identity_params,
                                           $equivalent,
                                           $qcs);
            my $concord = $equivalent / $qcs;
            push @results, [$qcs, $concord, $id];
            $equivalent++;
        }
    }
    return \@results;
}


=head2 find_identity_vary_smp

  Arg [1]    : Maybe[Num]
  Arg [2]    : Maybe[Num]
  Arg [3]    : Maybe[Int]

  Example    : $results = $simulator->find_identity_vary_smp(0.01, 0.05, 20);
  Description: Find concordance and identity for different values of the
               Sample Mismatch Prior (SMP) parameter, ie. the Bayesian prior
               probability of non-equivalent samples. The arguments control
               the range of SMP to be used.
  Returntype : ArrayRef[ArrayRef[Num]]

=cut

sub find_identity_vary_smp {
    # vary the Sample Mismatch Prior (SMP) parameter
    # Bayesian prior probability of non-equivalent samples
    my ($self, $start, $incr, $total) = @_;
    $start ||= 0.01;
    $incr ||= 0.05;
    $total ||= 20;
    $self->info("SMP: Start = $start, increment = $incr, total = $total");
    my $smp_list = $self->_generate_variable_list($start, $incr, $total);
    my $params = $self->_identity_params;
    my @results;
    foreach my $smp (@{$smp_list}) {
        $params->{sample_mismatch_prior} = $smp;
        my $equivalent = 0;
        while ($equivalent <= $self->total_calls) {
            my $id = $self->_find_identity($self->calls, $params, $equivalent,
                                           $self->total_calls);
            my $concord = $equivalent / $self->total_calls;
            push @results, [$smp, $concord, $id];
            $equivalent++;
        }
    }
    return \@results;
}


=head2 find_identity_vary_xer

  Arg [1]    : Maybe[Num]
  Arg [2]    : Maybe[Num]
  Arg [3]    : Maybe[Int]

  Example    : $results = $simulator->find_identity_vary_xer(0.01, 0.01, 20);
  Description: Find concordance and identity for different values of the
               Expected Error Rate (XER) parameter. The arguments control
               the range of XER to be used.
  Returntype : ArrayRef[ArrayRef[Num]]

=cut

sub find_identity_vary_xer {
    # vary the expected error rate (XER)
    # error = probability of non-equivalent calls on the same sample
    my ($self, $start, $incr, $total) = @_;
    $start ||= 0.01;
    $incr ||= 0.01;
    $total ||= 20;
    $self->info("XER: Start = $start, increment = $incr, total = $total");
    my $xer_list = $self->_generate_variable_list($start, $incr, $total);
    my $params = $self->_identity_params;
    my @results;
    foreach my $xer (@{$xer_list}) {
        $params->{expected_error_rate} = $xer;
        my $equivalent = 0;
        while ($equivalent <= $self->total_calls) {
            my $id = $self->_find_identity($self->calls, $params, $equivalent,
                                           $self->total_calls);
            my $concord = $equivalent / $self->total_calls;
            push @results, [$xer, $concord, $id];
            $equivalent++;
        }
    }
    return \@results;
}

sub _build_identity_params {
    # build a generic params hash for SampleMetric construction
    # production_calls and qc_calls are specified for each simulation type
    my ($self) = @_;
    my %params;
    $params{'sample_name'} = $DUMMY_SAMPLE_NAME;
    if (defined($self->pass_threshold)) {
        args{'pass_threshold'} = $self->pass_threshold;
    }
    if (defined($self->equivalent_calls_probability)) {
        $params{'equivalent_calls_probability'} =
            $self->equivalent_calls_probability;
    }
    if (defined($self->expected_error_rate)) {
        $params{'expected_error_rate'} = $self->expected_error_rate;
    }
    if (defined($self->sample_mismatch_prior)) {
        $params{'sample_mismatch_prior'} = $self->sample_mismatch_prior;
    }
    if (defined($params{'snpset'})) {
        $self->logcroak("Cannot supply a snpset in identity_params_input ",
                        "attribute; must provide separately in the snpset ",
                        "attribute.");
    }
    $params{'snpset'} = $self->snpset;
    return \%params;
}

sub _find_identity {
    # 'workhorse' method to evaluate the identity metric with given inputs
    # $calls = arrayref of Call objects
    # $params = hashref of params for SampleMetric object creation
    my ($self, $calls, $params, $equivalent, $total, $qc_total, $maf) = @_;
    $qc_total ||= 1;
    $maf ||= 0.25;
    my ($calls_p, $calls_q) = $self->_generate_call_subsets($calls,
                                                            $equivalent,
                                                            $total,
                                                            $qc_total,
                                                            $maf,
                                                        );
    my %args = (production_calls => $calls_p,
                qc_calls         => $calls_q,
            );
    foreach my $key (keys %{$params}) {
        $args{$key} = $params->{$key};
    }
    my $sib = WTSI::NPG::Genotyping::QC::BayesianIdentity::SampleMetric->new(\%args);
    return $sib->identity;
}

sub _generate_call_subsets {
    my ($self, $raw_calls, $equivalent, $total, $qc_total, $maf) = @_;
    $qc_total ||= 1; # total QC calls per SNP
    my @raw_calls = @{$raw_calls};
    if ($equivalent > $total) {
        $self->logcroak("Number of equivalent calls cannot be greater ",
                        "than total");
    } elsif ($total > scalar(@raw_calls)) {
        $self->logcroak("Total number of calls cannot be greater than ",
                        "size of raw call set");
    }
    my @production_calls = @raw_calls[0..$total-1];
    my @qc_calls = ();
    my $i = 0;
    foreach my $call (@production_calls) {
        my $qc_call;
        if ($i < $equivalent) {
            $qc_call = $call;
        } else {
            # change to non-equivalent genotype for QC call
            $qc_call = $self->_flip_genotype($call, $maf);
            if ($call->equivalent($qc_call)) {
                $self->logcroak("Flipped call should not be equivalent!");
            }
        }
        push @qc_calls, $qc_call;
        my $j = 0;
        while ($j < $qc_total - 1) { # add more identical QC calls, if needed
            push @qc_calls, $qc_call->clone();
            $j++;
        }
        $i++;
    }
    return (\@production_calls, \@qc_calls);
}

sub _flip_genotype {
    # given a call, create a new one with non-equivalent genotype on same SNP
    # preserve overall heterozygosity: So het->hom, hom->het
    # Use MAF to randomly choose between major/minor het
    # if no-call, return an identical no-call
    # preserve qscore (if any)
    my ($self, $call, $maf) = @_;
    $maf ||= 0.25;
    my $snp = $call->snp;
    my $new_genotype;
    my $is_call = 1;
    if (!($call->is_call)) {
        $new_genotype = 'NN';
        $is_call = 0;
    } elsif ($call->is_homozygous || $call->is_homozygous_complement) {
        # create heterozygous new call
        $new_genotype = $snp->ref_allele.$snp->alt_allele;
    } elsif ($call->is_heterozygous || $call->is_heterozygous_complement) {
        # create homozygous new call, random choice using MAF
        if (rand() < $maf) {
            $new_genotype = $snp->alt_allele.$snp->alt_allele;
        } else {
            $new_genotype = $snp->ref_allele.$snp->ref_allele;
        }
    } else {
        $self->logcroak("Input call is not a no-call, homozygote ",
                       "or heterozygote: ", $call->str());
    }
    my %args = (
        snp      => $snp,
        genotype => $new_genotype,
        is_call  => $is_call,
    );
    if (defined($call->qscore)) { $args{'qscore'} = $call->qscore; }
    return WTSI::NPG::Genotyping::Call->new(\%args);
}

sub _generate_variable_list {
    # generate a list of values for simulation input
    # $start, $incr, $total used to generate list
    # $min, $max are minimum, maximum permitted values
    # (eg. probabilities must be between 0 and 1)
    my ($self, $start, $incr, $total, $min, $max) = @_;
    $min ||= 0;
    $max ||= 1;
    if ($start < $min) {
        $self->logcroak("Starting value cannot be less than minimum of ",
                        $min);
    }
    if ($incr < 0) {
        $self->logcroak("Simulation variable increment cannot be negative");
    }
    my @values;
    my $value = $start;
    for (my $i=0;$i<$total;$i++) {
        if ($value > $max) {
            $self->logcroak("Simulation variable cannot be greater than ",
                            "maximum of ", $max);
        }
        push @values, $value;
        $value += $incr;
    }
    return \@values;
}


no Moose;

1;
