
package WTSI::NPG::Genotyping::QC_wip::Check::IdentityResults;


# class to represent identity check results
# methods including to_json

use Moose;
use JSON;
use WTSI::NPG::Genotyping::Call;

with 'WTSI::DNAP::Utilities::Loggable';


# previous version was a complex data structure:
# returned by count_all_matches

#           Return an array of HashRefs whose keys and values are:

#                {match    => <ArrayRef[HashRef]>, # call pairs
#                 mismatch => <ArrayRef[HashRef]>, # call pairs
#                 identity => <Num>,
#                 missing  => <Bool>,
#                 failed   => <Bool>,
#                 sample   => <Str>}

# need to get failed results, as input to evaluate_sample_swaps
# convert to a JSON-compatible data structure for output

# populate an IdentityResults object 'on the fly' while doing identity check

# methods:
# - add_sample_result
# - delete_sample_result
# - get_failed_results
# - to_json_spec

has 'sample_names' =>
    (is       => 'rw',
     isa      => 'ArrayRef[Str]',
     default  => sub { [] } );

has 'sample_names_hash' =>
    (is       => 'rw',
     isa      => 'HashRef',
     default  => sub { {} } );

has 'results' =>
    (is       => 'rw',
     isa      => 'ArrayRef[HashRef]',
     default  => sub { [] });

sub BUILD {

    my ($self) = @_;

    if (scalar(@{$self->sample_names}) != scalar(@{$self->results})) {
        $self->logcroak("Argument lists of mismatched length: ",
                        scalar(@{$self->sample_names}), " names, ",
                        scalar(@{$self->results}), " results" );
    }

}

sub add_sample_result {
    my ($self, $sample, $result) = @_;
    # enforce uniqueness of sample names
    if ($self->sample_names_hash->{$sample}) {
        $self->logcroak("Sample name '$sample' already exists in result set");
    }
    $self->sample_names_hash->{$sample} = 1;
    push (@{$self->sample_names}, $sample);
    # sanity checking on result argument
    my @keys = qw/match mismatch identity missing failed/;
    foreach my $key (@keys) {
        unless (defined($result->{$key})) {
            $self->logcroak("Must have a defined value for result key $key");
        }
    }
    push (@{$self->results}, $result);
    return 1;
}

sub delete_sample_result {
    my ($self, $target) = @_;
    my $i = 0;
    my @sample_names = @{$self->sample_names};
    foreach my $sample (@sample_names) {
        if ($sample eq $target) {
            splice(@sample_names, $i, 1);
            last;
        } else {
            $i++;
        }
    }
    $self->sample_names(\@sample_names);
    splice(@{$self->results}, $i, 1);
    return 1;
}

sub get_failed_results {
    my ($self) = @_;
    my $failed_results =
        WTSI::NPG::Genotyping::QC_wip::Check::IdentityResults->new();
    my $i = 0;
    while ($i < $self->get_num_results) {
        if ($self->results->[$i]->{'failed'}) {
            $failed_results->add_sample_result($self->sample_names->[$i],
                                               $self->results->[$i]);
        }
        $i++;
    }
    return $failed_results;
}

sub get_num_results {
    my ($self) = @_;
    return scalar(@{$self->sample_names});
}

sub to_json_spec {
   my ($self) = @_;
   my @identity_results_json;
   my $i = 0;
   while ($i < $self->get_num_results) {
       my $result_spec = $self->_make_result_json_spec($self->results->[$i]);
       push(@identity_results_json, [ $self->sample_names->[$i],
                                      $result_spec] );
       $i++;
   }
   return \@identity_results_json;
}


# Convert the return value of _count_matches to a data structure that
# may be output as JSON.
sub _make_result_json_spec {
  my ($self, $match_result) = @_;

  my %genotypes;
  foreach my $pair (@{$match_result->{match}}, @{$match_result->{mismatch}}) {
    my $qc_call = $pair->{qc};
    my $sa_call = $pair->{sample};
    my $qc_snp = $qc_call->snp->name;
    my $sa_snp = $sa_call->snp->name;

    unless ($qc_snp eq $sa_snp) {
      $self->logconfess("Invalid matched SNPs: QC: $qc_snp, sample: $sa_snp");
    }
    if (exists($genotypes{$qc_snp})) {
      $self->logconfess("Multiple calls for SNP $qc_snp");
    }

    $genotypes{$qc_snp} = [$qc_call->genotype, $sa_call->genotype];
  }

  # Replace Calls with genotype strings
  my %spec = %$match_result;
  delete $spec{match};
  delete $spec{mismatch};
  $spec{genotypes} = \%genotypes;

  return \%spec;
}

no Moose;

1;
