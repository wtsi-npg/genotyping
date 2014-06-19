
use utf8;

package WTSI::NPG::Genotyping::VCF::VCFConverter;

use DateTime;
use List::AllUtils qw(uniq);
use Log::Log4perl::Level;
use Moose;

use WTSI::NPG::Genotyping::SNPSet;
use WTSI::NPG::Genotyping::Fluidigm::AssayDataObject;
use WTSI::NPG::Genotyping::Fluidigm::AssayResultSet;
use WTSI::NPG::Genotyping::Sequenom::AssayDataObject;
use WTSI::NPG::Genotyping::Sequenom::AssayResultSet;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;

with 'WTSI::NPG::Loggable';

has 'irods'   =>
    (is       => 'ro',
     isa      => 'WTSI::NPG::iRODS',
     required => 1,
     default  => sub {
	 return WTSI::NPG::iRODS->new;
     });

has 'inputs' => (
    is        => 'ro',
    isa       => 'ArrayRef[Str]',
    required  => 1
    );

has 'input_type' => (
    is           => 'ro',
    isa          => 'Str',
    default      => 'sequenom', # sequenom or fluidigm
);

has 'sequenom_plex_dir' => (
    is           => 'ro',
    isa          => 'Str',
    default      => '/seq/sequenom/multiplexes',
);

has 'fluidigm_plex_dir' => (
    is           => 'ro',
    isa          => 'Str',
    default      => '/seq/fluidigm/multiplexes',
);

has 'resultsets' =>
    (is       => 'rw',
     isa      => 'ArrayRef', # Array of Sequenom OR Fluigidm AssayResultSet
    );

has 'sort' => (
    is        => 'ro',
    isa       => 'Bool',
    default   => 1,
    );

has 'verbose' => (
    is        => 'ro',
    isa       => 'Bool',
    default   => 0,
    );

sub BUILD {
  my $self = shift;
  my @inputs = @{ $self->inputs };
  $self->logger->level($WARN);
  # Make our iRODS handle use our logger by default
  $self->irods->logger($self->logger);
  my @results;
  my $total = 0;
  my $input_type = $self->input_type;
  if ($input_type ne 'sequenom' && $input_type ne 'fluidigm') {
      $self->logcroak("Unknown input data type: '$input_type'");
  }
  foreach my $irods_file (@inputs) {
      my $resultSet;
      if ($input_type eq 'sequenom') {
	  my $data_object = WTSI::NPG::Genotyping::Sequenom::AssayDataObject->new($self->irods, $irods_file);
	  $resultSet = WTSI::NPG::Genotyping::Sequenom::AssayResultSet->new(data_object => $data_object);
      } else {
	  my $data_object = WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new($self->irods, $irods_file);
	  $resultSet = WTSI::NPG::Genotyping::Fluidigm::AssayResultSet->new(data_object => $data_object);
      }
      $total += scalar(@{$resultSet->assay_results()});
      push(@results, $resultSet);
  }
  $self->resultsets(\@results);
  if ($self->verbose) { print "Found $total assay results\n"; }
}

sub convert {
    # convert one or more 'CSV' sample result files to a VCF file
    my $self = shift;
    my $output = shift; # TODO allow output to STDOUT (or string?)

    my $snpset_name = $self->_get_snpset_name($self->inputs);
    my $snpset_ipath = $self->_get_snpset_ipath($self->inputs, $snpset_name);
    my $snpset_obj = WTSI::NPG::iRODS::DataObject->new($self->irods, $snpset_ipath);
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_obj);
    $snpset->name($snpset_name);
    my $total = scalar(@{$snpset->snps()});
    if ($self->verbose) { print "Found $total SNPs in snpset\n"; }
    my @out_lines = $self->generate_vcf($snpset);
    if ($self->sort) {
        @out_lines = $self->_sort_output_lines(\@out_lines);
    }
    open my $out, ">", $output || $self->logcroak("Cannot open output '$output'");
    print $out join("\n", @out_lines)."\n";
    close $out, ">", $output || $self->logcroak("Cannot open output '$output'");

}

sub generate_vcf {
    # generate VCF data given a SNPSet and one or more AssayResultSets
    my $self = shift;
    my $snpset = shift;
    my $resultsRef = shift;
    $resultsRef ||= $self->resultsets;
    my ($callsRef, $samplesRef) = $self->_parse_calls_samples($resultsRef);
    my %calls = %{$callsRef};
    my $read_depth = 1; # placeholder
    my $qscore = 40;    # placeholder genotype quality
    my @output; # lines of text for output
    my @samples = sort(keys(%{$samplesRef}));
    push(@output, $self->_generate_vcf_header($snpset, \@samples));
    if ($self->verbose) { print scalar(@samples)." samples found.\n"; }
    foreach my $snp (@{$snpset->snps}) {
	my $ref = $snp->ref_allele();
	my $alt = $snp->alt_allele();
        my $chrom = $self->_convert_chromosome($snp->chromosome());
	my @fields = ( $chrom,                    # CHROM
		       $snp->position(),          # POS
		       $snp->name(),              # ID
		       $ref,                      # REF
		       $alt,                      # ALT
		       '.', '.',                  # QUAL, FILTER
		       'ORIGINAL_STRAND='.$snp->strand(),  # INFO
		       'GT:GQ:DP',                # FORMAT
	    );
	foreach my $sample (@samples) {
	    my $call_raw = $calls{$snp->name}{$sample};
	    my $call = $self->_call_to_vcf($call_raw, $ref, $alt,
					   $snp->strand());
            $call_raw ||= ".";
	    $call ||= ".";
	    my @sample_fields = ($call, $qscore, $read_depth);
	    push(@fields, join(':', @sample_fields));
	}
	push(@output, join("\t", @fields));
    }
    return @output;
}

sub _call_to_vcf {
    # convert the CSV genotype call to a VCF version
    # Sequenom CSV call may be of the form A, C, or AC; same or opposite strand to ref
    # Fluidigm will be of the form A:A, C:C, or A:C
    # VCF call is of the form 1/1, 0/0, or 1/0 for ref/alt
    # may have a 'no call'; if so return empty string
    #
    # Special case: Fluidigm gender markers have identical 'ref' and 'alt' values. But we may have data which does not match the ref or alt value. In this case we return a no call.
    my $self = shift;
    my ($call, $ref, $alt, $strand) = @_;
    if (!defined($call) || !$call) {
	return '';
    } elsif ($call =~ /[^ACGTN:]/) {
	$self->logcroak("Characters other than ACGTN in genotype '$call'");
    } elsif ($self->input_type() eq 'fluidigm') {
        $call =~ s/://g;
    }
    my %complement = ('A' => 'T',
                      'C' => 'G',
                      'G' => 'C',
                      'T' => 'A',
	              'N' => 'N');
    my $reverse;
    if ($strand eq '+') { $reverse = 0; }
    elsif ($strand eq '-') { $reverse = 1; }
    else { $self->logcroak("Unknown strand value '$strand'"); }
    my $new_call;
    if (length($call) == 1) {
	if ($reverse) { $call = $complement{$call}; }
	if ($call eq 'N') { $new_call = ''; }
        elsif ($ref eq $alt && $call ne $ref) { $new_call = ''; }
	elsif ($call eq $ref) { $new_call = '0/0'; }
	elsif ($call eq $alt) { $new_call = '1/1'; }
	else { $self->logcroak("Non-null call '$call' does not match reference '$ref' or alternate '$alt'"); }
    } elsif (length($call) == 2) {
	my @alleles = split(//, $call);
	my @new_alleles;
        my $alleles_ok = 1;
	foreach my $allele (@alleles) {
	    if ($reverse) { $allele = $complement{$allele}; }
	    if ($allele eq $ref) { push(@new_alleles, '0'); }
	    elsif ($allele eq $alt) { push(@new_alleles, '1'); }
            elsif ($ref eq $alt && $allele ne $ref) { $alleles_ok = 0; last; }
	    else { $self->logcroak("Non-null call '$allele' does not match reference '$ref' or alternate '$alt'");  }
	}
	if ($alleles_ok) { $new_call = join('/', @new_alleles); }
        else { $new_call = ''; } # special case; failed gender marker
    } else {
	$self->logcroak("Call '$call' is wrongly formatted, must have exactly one or two allele values");
    }
    return $new_call;
}

sub _convert_chromosome {
    # convert the chromosome field to standard GRCh37 format
    # chromsome names: 1, 2, 3, ... , 22, X, Y
    my $self= shift;
    my $input = shift;
    my $output;
    if ($input =~ /^[0-9]+$/ && $input >= 1 && $input <= 22 ) {
        $output = $input; # already in numeric chromosome format
    } elsif ($input eq 'X' || $input eq 'Y') {
        $output = $input; # already in standard X/Y format
    } elsif ($input =~ /^Chr/) {
        $input =~ s/Chr//g; # strip off 'Chr' prefix
        $output = $self->_convert_chromosome($input);
    } else {
        $self->logcroak("Unknown chromosome string: \"$input\"");
    }
    return $output;
}

sub _generate_vcf_header {
    my $self = shift;
    my $snpset = shift;
    my @samples = @{ shift() };
    if ($self->verbose) { print "Header snpset: ".$snpset->name()."\n"; }
    my $dt = DateTime->now(time_zone=>'local');
    my @header = ();
    push(@header, '##fileformat=VCFv4.0');
    push(@header, '##fileDate='.$dt->ymd(''));
    push(@header, '##source=WTSI::NPG::Genotyping::Sequenom::VCFConverter');
    # minimal contig tag, stops bcftools from complaining
    my @chromosomes = qw/1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20
                         21 22 X Y/;
    my @lengths = (249250621, 243199373, 198022430, 191154276, 180915260, 171115067, 159138663, 146364022, 141213431, 135534747, 135006516, 133851895, 115169878, 107349540, 102531392, 90354753, 81195210, 78077248, 59128983, 63025520, 48129895, 51304566, 155270560, 59373566); # GRCh37 chromosome lengths
    for (my $i=0; $i<@chromosomes; $i++) {
        my $chr = $chromosomes[$i];
        my $len = $lengths[$i];
        unless ($chr && $len) { $self->logcroak(); }
        push(@header, "##contig=<ID=$chr,length=$len,species=\"Homo sapiens\">");
    }
    push(@header, '##INFO=<ID=ORIGINAL_STRAND,Number=1,Type=String,Description="Direction of strand in input file">');
    push(@header, '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">');
    push(@header, '##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">');
    push(@header, '##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read Depth">');
    my @colHeads = qw/CHROM POS ID REF ALT QUAL FILTER INFO FORMAT/;
    push(@colHeads, @samples);
    push(@header, "#".join("\t", @colHeads));
    return @header;
}

sub _get_snpset_ipath {
    my $self = shift;
    my $inputsRef = shift;
    my $snpset_name = shift;
    my $genomeRef = 'GRCh37';
    if ($self->verbose) { print "SNP set name: $snpset_name\n"; }
    my $snpset_ipath;
    if ($self->input_type eq 'sequenom') {
        $snpset_ipath = $self->sequenom_plex_dir.'/'.$snpset_name.'_snp_set_info_'.$genomeRef.'.tsv';
    } elsif ($self->input_type eq 'fluidigm') {
        $snpset_ipath = $self->fluidigm_plex_dir.'/'.$snpset_name.'_fluidigm_snp_info_'.$genomeRef.'.tsv';
    } else {
        $self->logcroak("Unknown data type: ".$self->input_type);
    }
    if ($self->verbose) { print "SNP set iRODS path: $snpset_ipath\n"; }
    unless ($self->irods->list_object($snpset_ipath)) {
	$self->logconfess("No iRODS listing for snpset $snpset_ipath");
    }
    return $snpset_ipath;
}

sub _get_snpset_name {
    # get SNPset name for given sample Sequenom result files in iRODS
    # raise error if not all inputs have same SNPset
    my $self = shift;
    my @inputs = @{ shift() };
     # want to create an AssayResultSet for each input
    my @snpsets = ();
    foreach my $resultSet (@{$self->resultsets()}) {
	my $snpsetName = $resultSet->snpset_name();
	push(@snpsets, $snpsetName);
    }
    @snpsets = uniq(@snpsets);
    if (@snpsets != 1) {
	$self->logconfess("Must have exactly one SNP set in metadata");
    }
    return $snpsets[0];
}

sub _parse_calls_samples {
    # parse calls and sample IDs from reference to an array of ResultSets
    my $self = shift;
    my @results = @{ shift() };
    my (%calls, %samples);
    # generate a hash of calls by SNP and sample, and list of sample IDs
    foreach my $resultSet (@{$self->resultsets()}) {
	foreach my $ar (@{$resultSet->assay_results()}) {
            my ($sam_id, $snp_id);
	    if ($self->input_type eq 'sequenom'){
                $sam_id = $ar->sample_id();
                # assume assay_id of the form [plex name]-[snp name]
                my @terms = split("\-", $ar->assay_id());
                $snp_id = pop(@terms);
            } else {
                $sam_id = $ar->sample_name();
                $snp_id = $ar->snp_assayed();
            }
	    if ($calls{$snp_id}{$sam_id} && 
		$calls{$snp_id}{$sam_id} ne $ar->genotype_id()) {
		$self->logcroak("Conflicting genotype IDs for SNP $snp_id, sample $sam_id:".$calls{$snp_id}{$sam_id}.", ".$ar->genotype_id());
	    }
            if ($self->input_type eq 'sequenom') {
                $calls{$snp_id}{$sam_id} = $ar->genotype_id();
            } else {
                $calls{$snp_id}{$sam_id} = $ar->converted_call();
            }
	    $samples{$sam_id} = 1;
	}
    }
    return (\%calls, \%samples);
}


sub _sort_output_lines {
    # sort output lines by chromosome & position (1st, 2nd fields)
    # header lines are unchanged
    my $self = shift;
    my @input = @{ shift() };
    my (@output, %chrom, %pos, @data);
    foreach my $line (@input) {
        if ($line =~ /^#/) {
            push @output, $line;
        } else {
            push(@data, $line);
            my @fields = split(/\s+/, $line);
            my $chr = shift(@fields);
            if ($chr eq 'X') { $chr = 23; }
            elsif ($chr eq 'Y') { $chr = 24; }
            $chrom{$line} = $chr;
            $pos{$line} = shift(@fields);
        }
    }
    @data = sort { $chrom{$a} <=> $chrom{$b} || $pos{$a} <=> $pos{$b} } @data;
    push @output, @data;
    return @output;
}

no Moose;

1;

__END__
