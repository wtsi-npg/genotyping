
package WTSI::NPG::Genotyping::QC_wip::Check::Identity;

use JSON;
use Moose;
use List::Util qw/max/;

use plink_binary;

use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::SNP;
use WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentityBayesian;

use WTSI::NPG::Genotyping::Types qw(:all);

our $VERSION = '';

with 'WTSI::DNAP::Utilities::Loggable';

has 'plink_path' =>
  (is      => 'ro',
   isa     => 'Str',
   required => 1);

has 'snpset'  =>
  (is            => 'ro',
   isa           => 'WTSI::NPG::Genotyping::SNPSet',
   required      => 1,
   documentation => 'SNPs used in the QC assay (or assays). May be a '.
                    'union of multiple QC snpsets. Must be a subset of the '.
                    'Plink snpset for the identity check to take place');

# pass/fail thresholds

has 'swap_threshold' =>
  (is            => 'ro',
   isa           => 'Num',
   required      => 1,
   default       => 0.5,
   documentation => 'Minimum cross-identity for swap warning');

has 'pass_threshold' =>
  (is            => 'ro',
   isa           => 'Maybe[Num]',
   documentation => 'Minimum identity for metric pass');

# Bayesian model parameters, for SampleIdentityBayseian object

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

has 'ecp_default' =>
    (is       => 'ro',
     isa      => 'Maybe[Num]',
     documentation => 'Default probability of equivalent calls for a '.
         'given SNP on distinct samples',
    );

# non-input attributes

has 'num_samples' =>
  (is       => 'ro',
   isa      => 'Int',
   init_arg => undef,
   builder  => '_read_num_samples',
   lazy     => 1);

has 'sample_names' =>
  (is       => 'ro',
   isa      => 'ArrayRef[Str]',
   init_arg => undef,
   builder  => '_read_sample_names',
   lazy     => 1);

has 'shared_snp_names' =>
  (is       => 'ro',
   isa      => 'ArrayRef[Str]',
   init_arg => undef,
   builder  => '_read_shared_snp_names',
   lazy     => 1);

has 'production_calls' =>
  (is       => 'ro',
   isa      => 'HashRef[ArrayRef[WTSI::NPG::Genotyping::Call]]',
   init_arg => undef,
   builder  => '_read_production_calls',
   lazy     => 1);

=head2 find_identity

  Arg [1]     : HashRef[ArrayRef[WTSI::NPG::Genotyping::Call]]

  Returntype  : ArrayRef[WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentityBayesian]

  Description : Find identity results for all samples. Input is a hash of
                arrays of Call objects, indexed by sample name.

=cut

sub find_identity {
    my ($self, $qc_calls_by_sample) = @_;
    defined $qc_calls_by_sample or
        $self->logconfess('Must have a defined qc_calls_by_sample argument');
    $self->debug("Calculating identity with QC calls");
    my @id_results;
    my %missing;
    my $total_samples = scalar @{$self->sample_names};
    my $i = 0;
    foreach my $sample_name (@{$self->sample_names}) {
        $i++;
        $self->debug("Finding identity for '", $sample_name, "', sample ",
                     $i, " of ", $total_samples);
        my $qc_calls = $qc_calls_by_sample->{$sample_name};
        if (defined($qc_calls)) {
            my %args = %{$self->_get_sample_args($sample_name, $qc_calls)};
            my $result =
                WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentityBayesian->
                      new(%args);
            push @id_results, $result;
        } else {
            $missing{$sample_name} = 1;
        }
    }
    # now construct empty results for any samples missing from QC data
    # by convention, these are appended at the end of the results array
    $self->debug("Inserting empty results for missing samples");
    my $smp = $self->sample_mismatch_prior;
    foreach my $sample_name (@{$self->sample_names}) {
        if ($missing{$sample_name}) {
            my $args = $self->_get_sample_args($sample_name);
            my $result =
                WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentityBayesian->
                      new($args);
            push @id_results, $result;
        }
      }
    $self->debug("Finished calculating identity metric.");
    return \@id_results;
}

=head2 pairwise_swap_check

  Arg [1]    : ArrayRef[
                 WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentityBayesian
               ]
  Arg [2]    : Maybe[Num]

  Example    : my $comparison = pairwise_swap_check($results_ref, $prior);

  Description: Pairwise comparison of the given samples to look for possible
               swaps. Warning of a swap occurs if similarity between
               (calls_i, qc_j) for i != j is greater than
               $self->swap_threshold. Typically, the input samples have
               failed the standard identity metric. Prior probability of
               match can be given as an argument, or a default value can be
               computed. Return value is a hashref containing detailed
               results, pass/fail totals and the prior used.
  Returntype : HashRef

=cut

# TODO typechecking on the prior probability? Should have 0 < prior < 1.
# swap_metric method considers both (i,j) and (j,i) so only need to run once

sub pairwise_swap_check {
    my ($self, $id_results, $prior) = @_;
    my @comparison;
    my $total_warnings = 0;
    my $total_results = scalar @{$id_results};
    my %warnings_by_sample;
    if ($total_results > 0) {
        $prior ||= 1 - (1/$total_results);
        for (my $i=0;$i<$total_results;$i++) {
            $self->debug("Doing pairwise swap check for sample ", $i+1,
                         " of ", $total_results);
            for (my $j=0;$j<$i;$j++) {
                my $similarity =
                    $id_results->[$i]->swap_metric($id_results->[$j],
                                                   $prior);
                my $warning = 0;
                if ($similarity >= $self->swap_threshold) {
                    $warning = 1;
                    $total_warnings++;
                    $warnings_by_sample{$id_results->[$i]->sample_name}++;
                    $warnings_by_sample{$id_results->[$j]->sample_name}++;
                }
                my @row = ($id_results->[$i]->sample_name,
                           $id_results->[$j]->sample_name,
                           sprintf("%.4f", $similarity),
                           $warning);
                push(@comparison, [@row]);
            }
        }
    }
    if ($total_warnings > 0) {
        $self->warn("Warning of possible sample swap for ", $total_warnings,
                    " of ", scalar(@comparison), " pairs of failed samples.");
    }
    my %swap_result = (
        prior => $prior,
        comparison => \@comparison,
        sample_warnings => \%warnings_by_sample,
        total_sample_warnings => scalar(keys %warnings_by_sample),
        total_samples_checked => $total_results,
    );
    return \%swap_result;
}

=head2 write_identity_results

  Arg [1]     : ArrayRef[HashRef[WTSI::NPG::Genotyping::Call]]
  Arg [2]     : Str, path for JSON output or '-' for STDOUT
  Arg [3]     : Str, path for CSV output

  Returntype  : None

  Description : Run identity checks on all samples. Write JSON and CSV
                output to the given paths.

=cut

sub write_identity_results {
    my ($self, $qc_calls, $json_path, $csv_path) = @_;
    my $identity_results = $self->find_identity($qc_calls);
    my $failed = $self->_get_failed_results($identity_results);
    my $swap_evaluation = $self->pairwise_swap_check($failed);
    # write JSON
    my $json_spec = $self->_results_to_json_spec($identity_results,
                                                 $swap_evaluation);
    my $out_json;
    if ($json_path ne '-') {
        open $out_json, ">", $json_path ||
            $self->logcroak("Cannot open JSON path '", $json_path, "'");
    } else {
        $out_json = \*STDOUT;
    }
    print $out_json encode_json($json_spec);
    if ($json_path ne '-') {
        close $out_json || $self->logcroak("Cannot open output path '",
                                           $json_path, "'");
    }
    # write CSV
    my $out_csv;
    open $out_csv, ">", $csv_path ||
        $self->logcroak("Cannot open CSV path '", $csv_path, "'");
    my $callset_names = $self->_find_callset_names($qc_calls);
    print $out_csv $self->_csv_header($callset_names)."\n";
    foreach my $result (@{$identity_results}) {
        print $out_csv $result->to_csv($callset_names)."\n";
    }
    close $out_csv ||
        $self->logcroak("Cannot close CSV path '", $csv_path, "'");
}

sub _csv_header {
    # return a header customised for the available callsets
    # always include:
    #   sample_name,assayed,identity,concordance,total_calls,valid,equivalent
    # for each callset (if more than one):
    #   concordance,total_calls,valid_calls,equivalent_calls
    my ($self, $callset_names) = @_;
    my @header_fields = qw(sample_name
                           status
                           identity
                           concordance
                           total_calls
                           valid_calls
                           equivalent_calls);
    my @suffixes = qw(total_calls valid_calls equivalent_calls);
    if (scalar @{$callset_names} > 1) {
        foreach my $callset_name (@{$callset_names}) {
            foreach my $suffix (@suffixes) {
                push @header_fields, $callset_name.":".$suffix;
            }
        }
    }
    return join(',', @header_fields);
}

# find distinct callset names, sorted in alphabetical order
# each call has a callset_name attribute
# use for breakdown of calls (eg. Sequenom vs. Fluidigm) in output
# input: hash of arrays of Call objects
sub _find_callset_names {
    my ($self, $qc_calls_by_sample) = @_;
    my %callset_names;
    foreach my $sample_name (keys %{$qc_calls_by_sample}) {
        foreach my $call (@{$qc_calls_by_sample->{$sample_name}}) {
            $callset_names{$call->callset_name} = 1;
        }
    }
    my @callset_names = sort keys %callset_names;
    return \@callset_names;
}


# =head2 _read_num_samples

#   Arg [1]    : None

#   Example    : my $n = $check->_read_num_samples;
#   Description: Return number of samples in the Plink data.
#   Returntype : Int

# =cut

sub _read_num_samples {
  my ($self) = @_;

  my $plink = plink_binary::plink_binary->new($self->plink_path);

  return $plink->{"individuals"}->size;
}

# =head2 _read_sample_names

#   Arg [1]    : None

#   Example    : my @names = @{$check->_read_sample_names};
#   Description: Return the names of the samples in the Plink data, in
#                their original order.
#   Returntype : ArrayRef[Str]

# =cut

sub _read_sample_names {
  my ($self) = @_;

  my $plink = plink_binary::plink_binary->new($self->plink_path);

  my @names;
  for (my $i = 0; $i < $self->num_samples; $i++) {
    push @names, $plink->{'individuals'}->get($i)->{'name'};
  }
  return \@names;
}

# =head2 _read_shared_snp_names

#   Arg [1]    : None

#   Example    : my @names = @{$check->_read_shared_snp_names};
#   Description: Return the names of SNPs common to both the Plink data
#                and the quality control SNPset, in their original order
#                in the Plink data.

#                This method ignores the "exm-" prefix added by Illumina
#                to the real (dbSNP) SNP names.
#   Returntype : ArrayRef[Str]

# =cut

sub _read_shared_snp_names {
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

# =head2 _read_production_calls

#   Arg [1]    : None
#   Example    : my $calls = $check->_read_production_calls;
#                my @calls = @{$calls->{$sample_name}};
#   Description: Return the production Plink calls for all samples
#                corresponding to the QC SNPs.These are indexed by
#                samplename. The calls retain the order of the Plink
#                dataset.
#   Returntype : HashRef[Str] of sample names, where values are
#                ArrayRef[WTSI::NPG::Genotyping::Call]

# =cut

sub _read_production_calls {
  my ($self) = @_;


  my $plink = plink_binary::plink_binary->new($self->plink_path);
  my $genotypes = plink_binary::vectorstr->new;
  my $snp = plink_binary::snp->new;

  my %shared_snp_index;
  foreach my $name (@{$self->shared_snp_names}) {
    $shared_snp_index{$name}++;
  }

  my $sample_names = $self->sample_names;
  my %prod_calls_index;
  # Initializing %prod_calls_index so that it returns an empty list
  # (not undef), if shared SNP set is empty for a given sample.
  # Samples with no shared SNPs appear as 'missing' in JSON output.
  foreach my $sample_name (@{$sample_names}) {
    $prod_calls_index{$sample_name} = [];
  }

  while ($plink->next_snp($snp, $genotypes)) {
    my $illumina_name = $snp->{'name'};
    my $snp_name      = _from_illumina_snp_name($illumina_name);
    if ($snp_name ne $illumina_name) {
      $self->debug("Converted Illumina SNP name '$illumina_name' to ",
                   "'$snp_name'");
    }

    $self->debug("Checking for SNP '$snp_name' in [",
                 join(', ', keys %shared_snp_index), "]");

    if (exists $shared_snp_index{$snp_name}) {
        $self->debug("SNP name '$snp_name' found.");
        for (my $i = 0; $i < $genotypes->size; $i++) {
            my $call = WTSI::NPG::Genotyping::Call->new
                (snp      => $self->snpset->named_snp($snp_name),
                 genotype => $genotypes->get($i));

            my $sample_name = $sample_names->[$i];

            $self->debug("Plink call for SNP '", $snp_name, "', sample '",
                         $sample_name, "' = ", $call->str);
            push @{$prod_calls_index{$sample_name}}, $call;
        }
    } else {
        $self->debug("SNP name '", $snp_name, "' not found; skipping.");
    }
  }
  return \%prod_calls_index;
}


sub _results_to_json_spec {

    my ($self, $identity_results, $swap_evaluation) = @_;
    my %spec;
    my %summary = (
        'failed' => 0,
        'missing' => 0,
    );
    my @id_json_spec = ();
    # get summary counts
    foreach my $id_result (@{$identity_results}) {
        push(@id_json_spec, $id_result->to_json_spec());
        if ($id_result->missing) { $summary{'missing'}++; }
        elsif ($id_result->failed) { $summary{'failed'}++; }
    }
    $summary{'total'} = scalar @id_json_spec;
    my $pass_rate = 0;
    my $assayed = $summary{'total'} - $summary{'missing'};
    if ($assayed > 0) {
        $pass_rate = 1 - $summary{'failed'}/$assayed;
    }
    $summary{'assayed_pass_rate'} = sprintf "%.4f", $pass_rate;
    # get params (may be using default values from sample ID object)
    my $result = $identity_results->[0];
    my $pass_threshold = $self->pass_threshold ||
        $result->pass_threshold;
    my $ecp = $self->equivalent_calls_probability ||
        $result->equivalent_calls_probability;
    my $xer = $self->expected_error_rate ||
        $result->expected_error_rate;
    my $smp = $self->sample_mismatch_prior ||
        $result->sample_mismatch_prior;
    my $consensus_ecp; # record if all SNPs have same ECP
    foreach my $snp_name (keys %{$ecp}) {
        if (!defined($consensus_ecp)) {
            $consensus_ecp = $ecp->{$snp_name};
        } elsif ($consensus_ecp != $ecp->{$snp_name}) {
            $consensus_ecp = undef;
            last;
        }
    }
    # create JSON spec object
    $spec{'identity'} = \@id_json_spec;
    $spec{'swap'} = $swap_evaluation;
    $spec{'summary'} = \%summary; # id total/failed/missing
    $spec{'params'} = {
        pass_threshold => $pass_threshold,
        swap_threshold => $self->swap_threshold,
        equivalent_calls_probability => $ecp,
        consensus_ecp => $consensus_ecp,
        expected_error_rate => $xer,
        sample_mismatch_prior => $smp,
    };
    return \%spec;
}


sub _from_illumina_snp_name {
  my ($name) = @_;

  my ($prefix, $body) = $name =~ m{^(exm-)?(.*)}msx;

  return $body;
}

# get failed WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentity results
# only returns results confirmed as failed
# 'missing' results have undefined pass/fail status

sub _get_failed_results {
  my ($self, $id_results) = @_;
  my @failed = grep { $_->assayed && $_->failed } @$id_results;
  my $total = scalar @failed;
  $self->debug("Found ", $total, " failed identity results.");
  return \@failed;
}

sub _get_sample_args {
    # get arguments to construct a SampleIdentityBayesian object
    # QC calls may be omitted, eg. for a sample missing from QC data
    my ($self, $sample_name, $qc_calls) = @_;
    $qc_calls ||= [];
    my $production_calls = $self->production_calls->{$sample_name};
    my %args = (
        logger                => $self->logger,
        sample_name           => $sample_name,
        snpset                => $self->snpset,
        production_calls      => $production_calls,
        qc_calls              => $qc_calls
    );
    # update with optional attributes (if any)
    if (defined($self->pass_threshold)) {
        $args{'pass_threshold'} = $self->pass_threshold;
    }
    if (defined($self->equivalent_calls_probability)) {
        $args{'equivalent_calls_probability'} =
            $self->equivalent_calls_probability;
    }
    if (defined($self->expected_error_rate)) {
        $args{'expected_error_rate'} = $self->expected_error_rate;
    }
    if (defined($self->sample_mismatch_prior)) {
        $args{'sample_mismatch_prior'} = $self->sample_mismatch_prior;
    }
    if (defined($self->ecp_default)) {
        $args{'ecp_default'} = $self->ecp_default;
    }
    return \%args;
}


no Moose;

1;
