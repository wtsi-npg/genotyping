
use utf8;

package WTSI::NPG::Genotyping::VCF::VCFConverter;

use DateTime;
use JSON;
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

our $GRCH37_GENOME = 'GRCh37';
our $SEQUENOM_TYPE = 'sequenom';
our $FLUIDIGM_TYPE = 'fluidigm';
our $CHROMOSOME_JSON_KEY = 'chromosome_json';

has 'genome' => (
    is           => 'ro',
    isa          => 'Str',
    default      => $GRCH37_GENOME,
);

has 'irods'   =>
    (is       => 'ro',
     isa      => 'WTSI::NPG::iRODS',
     required => 1,
     default  => sub {
	 return WTSI::NPG::iRODS->new;
     });

has 'input_type' => (
    is           => 'ro',
    isa          => 'Str',
    default      => 'sequenom', # sequenom or fluidigm
);

has 'sequenom_type' => ( is  => 'ro',
                         isa => 'Str',
                         default => $SEQUENOM_TYPE );


has 'fluidigm_type' => ( is  => 'ro',
                         isa => 'Str',
                         default => $FLUIDIGM_TYPE );

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

has 'snpset_path' => ( # path to local (non-iRODS) SNP manifest
    is           => 'ro',
    isa          => 'Str',
);

has 'chromosome_length_path' => ( # path to local (non-iRODS) chromosome JSON
    is           => 'ro',
    isa          => 'Str',
);

has 'resultsets' =>
    (is       => 'rw',
     isa      => 'ArrayRef', # Array of Sequenom OR Fluigidm AssayResultSet
    );

has 'sort' => ( # sort the sample names before output?
    is        => 'ro',
    isa       => 'Bool',
    default   => 1,
    );

sub BUILD {
  my $self = shift;
  # Make our iRODS handle use our logger by default
  $self->irods->logger($self->logger);
  my @results;
  my $input_type = $self->input_type;
  if ($input_type ne $SEQUENOM_TYPE && $input_type ne $FLUIDIGM_TYPE) {
      $self->logcroak("Unknown input data type: '$input_type'");
  }
  my $total = scalar( $self->resultsets() );
  $self->logger->info("Found $total assay results\n");
}

sub convert {
    # convert one or more 'CSV' sample result files to VCF format
    # optionally, write VCF to a file or STDOUT
    # return VCF output lines as a string
    my $self = shift;
    my $output = shift;
    my @out_lines = $self->generate_vcf();
    if ($self->sort) {
        @out_lines = $self->_sort_output_lines(\@out_lines);
    }
    my $out;
    my $outString = join("\n", @out_lines)."\n";
    if ($output) {
        $self->logger->info("Printing VCF output to $output");
        if ($output eq '-') {
            $out = *STDOUT;
        } else {
            open $out, '>:encoding(utf8)', $output || $self->logcroak("Cannot open output '$output'");
        }
        print $out $outString;
        if ($output ne '-') {
            close $out || $self->logcroak("Cannot close output '$output'");
        }
    }
    return $outString;
}

sub generate_vcf {
    # generate VCF data given a SNPSet and one or more AssayResultSets
    my $self = shift;
    my $resultsRef = $self->resultsets;
    my ($callsRef, $samplesRef) = $self->_parse_calls_samples($resultsRef);
    my %calls = %{$callsRef};
    my $read_depth = 1; # placeholder
    my $qscore = 40;    # placeholder genotype quality
    my @output; # lines of text for output
    my @samples = sort(keys(%{$samplesRef}));
    my ($chroms, $snpset);
    if ($self->snpset_path) { # manifest path supplied as argument
        $snpset = WTSI::NPG::Genotyping::SNPSet->new($self->snpset_path);
        if (!$self->chromosome_length_path) {
            $self->logcroak("Must specify path to chromosome length JSON for SNP set ".$self->snpset_path);
        }
        $chroms = $self->_read_json($self->chromosome_length_path);
    } else { # find manifest from iRODS metadata
        my $snpset_name = $self->_get_snpset_name();
        my $snpset_ipath = $self->_get_snpset_ipath($snpset_name);
        my $snpset_obj = WTSI::NPG::iRODS::DataObject->new($self->irods, $snpset_ipath);
        $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_obj);
        if ($self->chromosome_length_path) {
            $chroms = $self->_read_json($self->chromosome_length_path);
        } else {
            $chroms = $self->_chromosome_lengths_irods($snpset_obj);
        }
    }
    my $total = scalar(@{$snpset->snps()});
    push(@output, $self->_generate_vcf_header($snpset, $chroms, \@samples));
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
    } elsif ($self->input_type() eq $FLUIDIGM_TYPE) {
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

sub _chromosome_lengths_irods {
    # get reference to a hash of chromosome lengths
    # read from JSON file, identified by snpset metadata in iRODS
    my $self = shift;
    my $snpset_obj = shift;
    my @avus = $snpset_obj->find_in_metadata($CHROMOSOME_JSON_KEY);
    if (scalar(@avus)!=1) {
        $self->logcroak("Must have exactly one $CHROMOSOME_JSON_KEY value in iRODS metadata for SNP set file");
    }
    my %avu = %{ shift(@avus) };
    my $chromosome_json = $avu{'value'};
    my $data_object = WTSI::NPG::iRODS::DataObject->new($self->irods, $chromosome_json);
    return decode_json($data_object->slurp());
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
    my %lengths = %{ shift() };
    my @samples = @{ shift() };
    my $dt = DateTime->now(time_zone=>'local');
    my @header = ();
    push(@header, '##fileformat=VCFv4.0');
    push(@header, '##fileDate='.$dt->ymd(''));
    push(@header, '##source=WTSI::NPG::Genotyping::Sequenom::VCFConverter');
    # add contig tags with chromosome lengths to prevent bcftools warnings
    my @chromosomes = sort(keys(%lengths));
    foreach my $chr (@chromosomes) {
        push(@header, "##contig=<ID=$chr,length=$lengths{$chr},species=\"Homo sapiens\">");
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
    my $snpset_name = shift;
    my $genome_suffix; # suffix not necessarily equal to genome
    if ($self->genome eq $GRCH37_GENOME) { $genome_suffix = $GRCH37_GENOME; }
    else { $self->logcroak("Unknown genome designation: ".$self->genome); }
    my $snpset_ipath;
    if ($self->input_type eq $SEQUENOM_TYPE) {
        $snpset_ipath = $self->sequenom_plex_dir.'/'.$snpset_name.'_snp_set_info_'.$genome_suffix.'.tsv';
    } elsif ($self->input_type eq $FLUIDIGM_TYPE) {
        $snpset_ipath = $self->fluidigm_plex_dir.'/'.$snpset_name.'_fluidigm_snp_info_'.$genome_suffix.'.tsv';
    } else {
        $self->logcroak("Unknown data type: ".$self->input_type);
    }
    unless ($self->irods->list_object($snpset_ipath)) {
	$self->logconfess("No iRODS listing for snpset $snpset_ipath");
    }
    return $snpset_ipath;
}

sub _get_snpset_name {
    # get SNPset name for given sample Sequenom result files in iRODS
    # raise error if not all inputs have same SNPset
    my $self = shift;
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
	    if ($self->input_type eq $SEQUENOM_TYPE){
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
            if ($self->input_type eq $SEQUENOM_TYPE) {
                $calls{$snp_id}{$sam_id} = $ar->genotype_id();
            } else {
                $calls{$snp_id}{$sam_id} = $ar->converted_call();
            }
	    $samples{$sam_id} = 1;
	}
    }
    return (\%calls, \%samples);
}

sub _read_json {
    # read given path into a string and decode as JSON
    my $self = shift;
    my $input = shift;
    open my $in, '<:encoding(utf8)', $input || $self->logcroak("Cannot open input '$input'");
    my $data = decode_json(join("", <$in>));
    close $in || $self->logcroak("Cannot close input '$input'");
    return $data;
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
