
use utf8;

package WTSI::NPG::Genotyping::VCF::VCFGtcheck;

use JSON;
use Log::Log4perl::Level;
use Moose;

with 'WTSI::NPG::Loggable';

# front-end for bcftools gtcheck function
# use to cross-check sample results in a single VCF file for consistency
# parse results and output in useful formats

our $MAX_DISCORDANCE_KEY = 'MAX_DISCORDANCE';
our $PAIRWISE_DISCORDANCE_KEY = 'PAIRWISE_DISCORDANCE';

has 'input' => (
    is           => 'ro',
    isa          => 'Str',
);

has 'debug_mode' => ( # not called 'debug' to avoid name clash
    is        => 'ro',
    isa       => 'Bool',
    default   => 0,
    );

has 'verbose' => (
    is        => 'ro',
    isa       => 'Bool',
    default   => 0,
    );

sub BUILD {
  my $self = shift;
  if ($self->debug_mode) { $self->logger->level($DEBUG); }
  elsif ($self->verbose) { $self->logger->level($INFO); }
  else { $self->logger->level($WARN); }
  my $input = $self->input();
  if (!(-e $input && -f $input)) {
      $self->logcroak("Invalid input path: \"$input\"");
  }
}

sub run {
    # run 'bcftools gtcheck' on the input; capture and parse the output
    my $self = shift;
    my $input = $self->input();
    my $bcftools = $self->_find_bcftools();
    my $cmd = "$bcftools gtcheck $input -G 1";
    $self->logger->info("Running bcftools command: $cmd");
    my $result = `$cmd`;
    $self->logger->debug("bcftools command output:\n$result");
    my @lines = split("\n", $result);
    my %results;
    my $max = 0; # maximum pairwise discordance
    foreach my $line (@lines) {
        if ($line !~ /^CN/) { next; }
        my @words = split(/\s+/, $line);
        my $discordance = $words[1];
        my $sites = $words[2];
        my $sample_i = $words[4];
        my $sample_j = $words[5];
        if (!defined($discordance)) {
            $self->logcroak("Cannot parse discordance from output: $line");
        } elsif (!defined($sites)) {
            $self->logcroak("Cannot parse sites from output: $line");
        } elsif (!($sample_i && $sample_j)) {
            $self->logcroak("Cannot parse sample names from output: $line");
        }
        my $discord_rate;
        if ($sites == 0) { $discord_rate = 'NA'; }
        else { $discord_rate = $discordance / $sites; }
        $results{$sample_i}{$sample_j} = $discord_rate;
        $results{$sample_j}{$sample_i} = $discord_rate;
        if ($discord_rate > $max) { $max = $discord_rate; }
    }
    return (\%results, $max);
}

sub write_results_json {
    # write maximum pairwise discordance rates in JSON format
    my $self = shift;
    my $resultsRef = shift;
    my $maxDiscord = shift;
    my $outPath = shift;
    my %output = ($MAX_DISCORDANCE_KEY => $maxDiscord,
                  $PAIRWISE_DISCORDANCE_KEY => $resultsRef);
    open my $out, ">", $outPath || $self->logcroak("Cannot open output $outPath");
    print $out encode_json(\%output);
    close $out || $self->logcroak("Cannot open output $outPath");
    if ($self->verbose) { print encode_json(\%output)."\n"; }
    return 1;
}

sub write_results_text {
    # write maximum pairwise discordance rates in text format
    # maximum appears in header
    # columns in body: sample_i, sample_j, pairwise discordance
    my $self = shift;
    my %results = %{ shift() };
    my $maxDiscord = shift;
    my $outPath = shift;
    my @samples = sort(keys(%results));
    open my $out, ">", $outPath || $self->logcroak("Cannot open output $outPath");
    printf $out "# $MAX_DISCORDANCE_KEY: %.5f\n", $maxDiscord;
    print $out "# sample_i\tsample_j\tpairwise_discordance\n";
    foreach my $sample_i (@samples) {
        foreach my $sample_j (@samples) {
            if ($sample_i eq $sample_j) { next; }
            my @fields = ($sample_i,$sample_j,$results{$sample_i}{$sample_j});
            printf $out "%s\t%s\t%.5f\n", @fields;
        }
    }
    close $out || $self->logcroak("Cannot open output $outPath");
    return 1;
}


sub _find_bcftools {
    # check existence and version of the bcftools executable
    my $self = shift;
    my $bcftools = `which bcftools`;
    chomp $bcftools;
    if (!$bcftools) { $self->logcroak("Cannot find bcftools executable"); }
    my $version_string = `bcftools --version`;
    if ($version_string !~ /^bcftools 0\.2\.0-rc9/) {
        $self->logger->logwarn("Must have bcftools version >= 0.2.0-rc9");
    }
    return $bcftools;
}

no Moose;

1;

__END__
