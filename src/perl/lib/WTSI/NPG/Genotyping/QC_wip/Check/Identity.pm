
package WTSI::NPG::Genotyping::QC_wip::Check::Identity;

use JSON;
use Moose;
use List::Util qw/max/;

use plink_binary;

use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::SNP;
use WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentity;

use WTSI::NPG::Genotyping::Types qw(:all);

with 'WTSI::DNAP::Utilities::Loggable';

has 'plink_path' =>
  (is      => 'ro',
   isa     => 'Str',
   required => 1);

# represents the snpset used in the QC plex
# typically this is a subset of the Plink snpset
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

has 'sample_plink_calls' =>
  (is       => 'rw',
   isa      => 'HashRef[ArrayRef[WTSI::NPG::Genotyping::Call]]',
   required => 0);


sub BUILD {
    my ($self) = @_;
    # read Plink data into an attribute for later reference
    $self->sample_plink_calls($self->get_production_calls());
}

=head2 get_num_samples

  Arg [1]    : None

  Example    : my $n = $check->get_num_samples;
  Description: Return number of samples in the Plink data.
  Returntype : Int

=cut

sub get_num_samples {
  my ($self) = @_;

  my $plink = plink_binary::plink_binary->new($self->plink_path);

  return $plink->{"individuals"}->size;
}

=head2 get_sample_names

  Arg [1]    : None

  Example    : my @names = @{$check->get_num_samples};
  Description: Return the names of the samples in the Plink data, in
               their original order.
  Returntype : ArrayRef[Str]

=cut

sub get_sample_names {
  my ($self) = @_;

  my $plink = plink_binary::plink_binary->new($self->plink_path);

  my @names;
  for (my $i = 0; $i < $self->get_num_samples; $i++) {
    push @names, $plink->{'individuals'}->get($i)->{'name'};
  }

  return \@names;
}


=head2 pairwise_swap_check

  Arg [1]    : ArrayRef[WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentity]

  Example    : my $comparison = pairwise_swap_check($results_ref);

  Description: Pairwise comparison of the given samples to look for possible
               swaps. Warning of a swap occurs if similarity between
               (calls_i, qc_j) for i != j is greater than
               $self->swap_threshold. Typically, the input samples have
               failed the standard identity metric.
  Returntype : ArrayRef[ArrayRef[Str]]

=cut

sub pairwise_swap_check {
    my ($self, $id_results) = @_;
    my $total_warnings = 0;
    my @comparison = ();
    for (my $i=0;$i<@{$id_results};$i++) {
        for (my $j=0;$j<$i;$j++) {
            my $similarity =
                $id_results->[$i]->find_swap_metric($id_results->[$j]);
            my $warning = 0;
            if ($similarity >= $self->swap_threshold) {
                $warning = 1;
                $total_warnings++;
            }
            my @row = ($id_results->[$i]->get_sample_name(),
                       $id_results->[$j]->get_sample_name(),
                       $similarity,
                       $warning);
            push(@comparison, [@row]);
        }
    }
    if ($total_warnings > 0) {
        $self->warn("Warning of possible sample swap for ", $total_warnings,
                    " of ", scalar(@comparison), " pairs of failed samples.");
    }
    return \@comparison;
}

=head2 find_identity

  Arg [1]     : HashRef[ArrayRef[WTSI::NPG::Genotyping::Call]]

  Returntype  : ArrayRef[WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentity]

  Description : Find identity results for all samples. Input is a hash of
                arrays of Call objects, indexed by sample name.

=cut

# based on old count_all_matches method
# should bail and write empty output if insufficient shared SNPs present

sub find_identity {
    my ($self, $qc_calls_by_sample) = @_;
    defined $qc_calls_by_sample or
        $self->logconfess('Must have a defined qc_calls_by_sample argument');
    my @id_results = ();
    my %missing = ();
    foreach my $sample_name (@{$self->get_sample_names()}) {
        my $qc_calls = $qc_calls_by_sample->{$sample_name};
        if (defined($qc_calls)) {
            my $production_calls = $self->sample_plink_calls->{$sample_name};
            my $result;
            if (scalar(@{$production_calls}) >= $self->min_shared_snps) {
                $result =
                    WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentity->new(
                        sample_name      => $sample_name,
                        snpset           => $self->snpset,
                        production_calls => $production_calls,
                        qc_calls         => $qc_calls,
                        pass_threshold   => $self->pass_threshold,
                    );
            } else {
                # insufficient shared SNPs; by convention, result is empty
                $result =
                    WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentity->new(
                        sample_name      => $sample_name,
                        snpset           => $self->snpset,
                        production_calls => [],
                        qc_calls         => [],
                        pass_threshold   => $self->pass_threshold,
                        omitted          => 1,
                    );
            }
            push(@id_results, $result);
        } else {
            $missing{$sample_name} = 1;
        }
    }
    # now construct empty results for samples missing from QC data (if any)
    # by convention, these are appended at the end of the results array
    foreach my $sample_name (@{$self->get_sample_names()}) {
        if ($missing{$sample_name}) {
            my $result =
                WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentity->new(
                    sample_name      => $sample_name,
                    snpset           => $self->snpset,
                    production_calls => [],
                    qc_calls         => [],
                    pass_threshold   => $self->pass_threshold,
                    missing          => 1);
            push(@id_results, $result);
        }
    }
    return \@id_results;
}

=head2 run_identity_checks

  Arg [1]     : ArrayRef[HashRef[WTSI::NPG::Genotyping::Call]]

  Returntype  : (ArrayRef[
                  WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentity
                 ],
                 ArrayRef[ArrayRef[Str]])

  Description : Run the identity check on each sample, then carry out
                cross-check on failed samples to detect swaps. Return
                results of both checks.

=cut

sub run_identity_checks {
    my ($self, $qc_call_sets) = @_;
    my $identity_results = $self->find_identity($qc_call_sets);
    my $failed = $self->_get_failed_results($identity_results);
    my $swap_evaluation = $self->pairwise_swap_check($failed);
    return ($identity_results, $swap_evaluation);
}

=head2 run_identity_checks_json_spec

  Arg [1]     : ArrayRef[HashRef[WTSI::NPG::Genotyping::Call]]

  Returntype  : HashRef

  Description : Run identity checks on all samples, returning a data structure
                compatible with JSON output. Intended as a 'main' method to
                run from a command-line script.

=cut

sub run_identity_checks_json_spec {
  my ($self, $qc_call_sets) = @_;
  my ($identity_results, $swap_evaluation) =
      $self->run_identity_checks($qc_call_sets);
  my %spec;
  my @id_json_spec = ();
  foreach my $id_result (@{$identity_results}) {
      push(@id_json_spec, $id_result->to_json_spec());
  }
  $spec{'identity'} = \@id_json_spec;
  $spec{'swap'} = $swap_evaluation;
  return \%spec;
}


sub _from_illumina_snp_name {
  my ($name) = @_;

  my ($prefix, $body) = $name =~ m{^(exm-)?(.*)};

  return $body;
}

# Return the Plink calls for all samples, indexed by sample
# name. The calls retain the order of the Plink dataset.
# Calls are only returned for SNPs in the QC plex, as
# represented in $self->snpset.
#  Returntype : HashRef[Str] of sample names, where values are
#               ArrayRef[WTSI::NPG::Genotyping::Call]

sub get_production_calls {
  my ($self) = @_;

  my $plink = plink_binary::plink_binary->new($self->plink_path);
  my $genotypes = new plink_binary::vectorstr;
  my $snp = new plink_binary::snp;

  my %shared_snp_index;
  foreach my $name (@{$self->get_shared_snp_names}) {
    $shared_snp_index{$name}++;
  }

  my $sample_names = $self->get_sample_names;

  my %sample_calls_index;
  while ($plink->next_snp($snp, $genotypes)) {
    my $illumina_name = $snp->{'name'};
    my $snp_name      = _from_illumina_snp_name($illumina_name);
    if ($snp_name ne $illumina_name) {
      $self->debug("Converted Illumina SNP name '$illumina_name' to ",
                   "'$snp_name'");
    }

    $self->debug("Checking for SNP '$snp_name' in [",
                 join(', ', keys %shared_snp_index), "]");

    for (my $i = 0; $i < $genotypes->size; $i++) {
      if (exists $shared_snp_index{$snp_name}) {
        my $call = WTSI::NPG::Genotyping::Call->new
          (snp      => $self->snpset->named_snp($snp_name),
           genotype => $genotypes->get($i));

        my $sample_name = $sample_names->[$i];
        unless (exists $sample_calls_index{$sample_name}) {
          $sample_calls_index{$sample_name} = [];
        }

        $self->debug("Plink call: ", $call->str);
        push @{$sample_calls_index{$sample_name}}, $call;
      }
      else {
        $self->debug("Skipping ", $snp_name);
      }
    }
  }

  return \%sample_calls_index;
}

# get failed WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentity results
# only returns results confirmed as failed
# 'missing' and 'omitted' results have undefined pass/fail status

sub _get_failed_results {
    my ($self, $id_results) = @_;
    my @failed;
    foreach my $result (@{$id_results}) {
        if ($result->{'failed'}) {
            push(@failed, $result);
        }
    }
    return \@failed;
}


# Return the names of SNPs common to both the Plink data
# and the quality control SNPset, in their original order
# in the Plink data.

# This method ignores the "exm-" prefix added by Illumina
# to the real (dbSNP) SNP names.

# Returntype : ArrayRef[Str]

sub get_shared_snp_names {
  my ($self) = @_;

  my %plex_snp_index;
  foreach my $name ($self->snpset->snp_names) {
    $plex_snp_index{$name}++;
  }

  my $plink = plink_binary::plink_binary->new($self->plink_path);

  my @shared_names;
  my $num_plink_snps = $plink->{'snps'}->size;
  for (my $i = 0; $i < $num_plink_snps; $i++) {
    my $name = $plink->{'snps'}->get($i)->{'name'};
    my $converted = _from_illumina_snp_name($name);

    if (exists $plex_snp_index{$converted}) {
      push @shared_names, $converted;
    }
  }

  return \@shared_names;
}


no Moose;

1;
