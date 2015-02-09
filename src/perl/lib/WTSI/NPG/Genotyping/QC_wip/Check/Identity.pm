
package WTSI::NPG::Genotyping::QC_wip::Check::Identity;

use Moose;

use plink_binary;

use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::SNP;

use WTSI::NPG::Genotyping::Types qw(:all);

with 'WTSI::DNAP::Utilities::Loggable';

has 'plink_path' =>
  (is      => 'ro',
   isa     => 'Str',
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

=head2 get_shared_snp_names

  Arg [1]    : None

  Example    : my @names = @{$check->get_shared_snp_names};
  Description: Return the names of SNPs common to both the Plink data
               and the quality control SNPset, in their original order
               in the Plink data.

               This method ignores the "exm-" prefix added by Illumina
               to the real (dbSNP) SNP names.

  Returntype : ArrayRef[Str]

=cut

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

=head2 get_all_calls

  Arg [1]    : None

  Example    : my $all_calls = $check->get_all_calls;
               my @sample_calls = @{$all_calls->{$sample_name}};
  Description: Return the Plink calls for all samples, indexed by sample
               name. The calls retain the order of the Plink dataset.

  Returntype : HashRef[Str] of sample names, where values are
               ArrayRef[WTSI::NPG::Genotyping::Call]

=cut

sub get_all_calls {
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

=head2 get_sample_calls

  Arg [1]    : None

  Example    : my @sample_calls = @{$check->get_sample_calls($sample_name)};
  Description: Return the Plink calls for a samples.

  Returntype : ArrayRef[WTSI::NPG::Genotyping::Call]

=cut

sub get_sample_calls {
  my ($self, $sample_name) = @_;

  defined $sample_name or
    $self->logconfess('A defined sample_name argument is required');
  $sample_name or
    $self->logconfess('A non-empty sample_name argument is required');

  my $all_calls = $self->get_all_calls;
  exists $all_calls->{$sample_name} or
    $self->logconfess("There is no data for a sample named '$sample_name'");

  return $all_calls->{$sample_name};
}

=head2 pair_all_calls

  Arg [1]    : ArrayRef[HashRef]

  Example    : my @pairs = @{$check->pair_calls($sample_name, $qc_calls)};
  Description: Find the calls for given samples that correspond to the
               supplied QC calls (i.e. are for the same SNP).

               Each HashRef in the argument must have the following keys
               and values:

                {sample => <Str>,
                 calls  => <ArrayRef[WTSI::NPG::Genotyping::Call]>}

               (These are the arguments used by pair_sample_calls).

               Return an array of HashRefs whose keys and values are:

                {sample => <Str>,
                 pairs  => <ArrayRef[HashRef]>}

               (The values under the key 'pairs' correspond to the return
                values of pair_sample_calls).

  Returntype : ArrayRef[HashRef]

=cut


sub pair_all_calls {
  my ($self, $qc_call_sets) = @_;

  defined $qc_call_sets or
    $self->logconfess('A defined qc_call_sets argument is required');

  $self->debug("Pairing calls for ", scalar @$qc_call_sets, " samples");

  my @paired;
  foreach my $qc_call_set (@$qc_call_sets) {
    exists $qc_call_set->{calls} or
      $self->logconfess("A 'calls' key is required");
    exists $qc_call_set->{sample} or
      $self->logconfess("A 'sample' key is required");

    my $qc_calls    = $qc_call_set->{calls};
    my $sample_name = $qc_call_set->{sample};

    push @paired,
      {pairs  => $self->pair_sample_calls($sample_name, $qc_calls),
       sample => $sample_name};
  }

  $self->debug("Paired calls for ", scalar @paired, " samples");

  return \@paired;
}

=head2 pair_sample_calls

  Arg [1]    : Str sample name (in Plink data)
  Arg [2]    : ArrayRef[WTSI::NPG::Genotyping::Call] QC calls

  Example    : my @pairs = @{$check->pair_calls($sample_name, $qc_calls)};
  Description: Find the calls for a given sample that correspond to the
               supplied QC calls (i.e. are for the same SNP). Return an array
               pairs calls. Each pair is a HashRef whose keys and values are:

                {qc     => <WTSI::NPG::Genotyping::Call>,
                 sample => <WTSI::NPG::Genotyping::Call>}

               The results are in the same order as the supplied QC calls.

  Returntype : ArrayRef[HashRef]

=cut

sub pair_sample_calls {
  my ($self, $sample_name, $qc_calls) = @_;

  defined $sample_name or
    $self->logconfess('A defined sample_name argument is required');
  $sample_name or
    $self->logconfess('A non-empty sample_name argument is required');

  defined $qc_calls or
    $self->logconfess('A defined qc_calls argument is required');

  my $sample_calls = $self->get_sample_calls($sample_name);
  unless (defined $sample_calls) {
    $self->logconfess("No sample calls are present for '$sample_name'");
  }

  $self->debug("Pairing calls for '$sample_name'");

  my @pairs;
  foreach my $qc_call (@$qc_calls) {
    unless ($self->snpset->contains_snp($qc_call->snp->name)) {
      $self->logconfess("Invalid QC call for comparison; its SNP '",
                        $qc_call->snp->name, "' is not in the SNP set ",
                        "being compared");
    }

    foreach my $sample_call (@$sample_calls) {
      if ($qc_call->snp->name eq $sample_call->snp->name) {
        $self->debug("Comparing [", $qc_call->str, "] with [",
                     $sample_call->str, "]");

        push @pairs, {qc     => $qc_call,
                      sample => $sample_call};
      }
    }
  }

  $self->debug("Paired ", scalar @pairs, " calls for '$sample_name'");

  return \@pairs;
}

sub _from_illumina_snp_name {
  my ($name) = @_;

  my ($prefix, $body) = $name =~ m{^(exm-)?(.*)};

  return $body;
}

no Moose;

1;
