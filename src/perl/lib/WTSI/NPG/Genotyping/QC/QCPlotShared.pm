# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# define shared constants and subroutines for QC plot scripts

package WTSI::NPG::Genotyping::QC::QCPlotShared;

use warnings;
use strict;
use Carp;
use Config::Tiny;
use Cwd;
use FindBin qw($Bin);
use POSIX qw(floor);
use Log::Log4perl qw(:easy);
use JSON;
use WTSI::NPG::Genotyping::Database::Pipeline;
use Exporter;

Log::Log4perl->easy_init($ERROR);

our @ISA = qw/Exporter/;
our @EXPORT_OK = qw/defaultPipelineDBConfig defaultConfigDir defaultJsonConfig defaultTexIntroPath getDatabaseObject getPlateLocations getPlateLocationsFromPath getSummaryStats meanSd median parseLabel parseThresholds plateLabel readFileToString readMetricResultHash readQCFileNames readQCMetricInputs readQCNameArray readQCShortNameHash readSampleData readSampleInclusion readThresholds $INI_PATH $INI_FILE_DEFAULT $UNKNOWN_PLATE $UNKNOWN_ADDRESS/;

our $VERSION = '';

use vars qw/$UNKNOWN_PLATE $UNKNOWN_ADDRESS/;

$UNKNOWN_PLATE = "Unknown_plate";
$UNKNOWN_ADDRESS = "Unknown_address";  

# duplicate threshold is currently hard-coded in /software/varinf/bin/genotype_qc/pairwise_concordance_bed

sub defaultConfigDir {
  return WTSI::NPG::Genotyping::config_dir();
}

sub defaultPipelineDBConfig {
  return defaultConfigDir()."/pipeline.ini";
}

sub defaultJsonConfig {
  return defaultConfigDir()."/qc_config.json";
}

sub defaultTexIntroPath {
  return defaultConfigDir()."/reportIntro.tex";
}

sub getDatabaseObject {
    # set up database object
    my $dbfile = shift;
    my $inifile = shift;

    $inifile ||= defaultPipelineDBConfig();

    my $db = WTSI::NPG::Genotyping::Database::Pipeline->new
	(name => 'pipeline',
	 inifile => $inifile,
	 dbfile => $dbfile);
    $db->connect(RaiseError => 1,
                 on_connect_do => 'PRAGMA foreign_keys = ON');
    return $db;
}

sub getPlateLocations {
    # get plate and (x,y) location from database
    my $db = shift;
    my @samples = $db->sample->all;
    my %plateLocs;
    $db->in_transaction(sub {
        foreach my $sample (@samples) {
            my ($plate, $x, $y) = (0,0,0);
            my $uri = $sample->uri;
            if (!defined($uri)) {
                carp "Sample $sample has no uri!";
                next;
            } elsif ($sample->include == 0) {
                next; # excluded sample
            }
            my $well = ($sample->wells->all)[0]; # assume one well per sample
            if (defined($well)) { 
                my $address = $well->address;
                my $label = $address->label1;
                $plate = $well->plate;
                my $plateName = $plate->ss_barcode;
                $plateLocs{$uri} = [$plateName, $label];  
            } else {
                $plateLocs{$uri} = [$UNKNOWN_PLATE, $UNKNOWN_ADDRESS];  
            }
        }
                        });
    return %plateLocs;
}
	
sub getPlateLocationsFromPath {
    my $dbPath = shift;
    my $iniPath = shift;
    my $db = openDatabase($dbPath, $iniPath);
    my %plateLocs = getPlateLocations($db);
    $db->disconnect();
    return %plateLocs;
}

sub getSummaryStats {
    # read .json file of qc status and get summary values
    # interesting stats: mean/sd of call rate, and overall pass/fail
    my $inPath = shift;
    my $configPath = shift;
    my %allResults = readMetricResultHash($inPath, $configPath);
    my @cr;
    my $fails = 0;
    my @samples = keys(%allResults);
    my $total = @samples;
    foreach my $sample (@samples) {
	my %results = %{$allResults{$sample}};
	my $samplePass = 1;
	foreach my $key (keys(%results)) {
	    my ($pass, $value) = @{$results{$key}};
	    if ($key eq 'call_rate') { push(@cr, $value); }
	    unless ($pass) { $samplePass = 0; }
	}
	unless ($samplePass) { $fails++; }
    }
    my ($mean, $sd) = meanSd(@cr);
    my $passRate = 1 - ($fails/$total);
    return ($total, $fails, $passRate, $mean, $sd);
}

sub hashKeysEqual {
    my %hash1 = %{ shift() };
    my %hash2 = %{ shift() };
    my (%keys1, %keys2);
    my $equal = 1;
    foreach my $key (keys %hash1) { $keys1{$key} = 1; }
    foreach my $key (keys %hash2) { $keys2{$key} = 1; }
    foreach my $key (keys %hash1) { # is hash2 <= hash1 ?
        if (!$keys2{$key}) { $equal = 0; last; }
    }
    if ($equal) { # is hash1 <= hash2 ?
        foreach my $key (keys %hash2) {
            if (!$keys1{$key}) { $equal = 0; last; }
        }
    }
    return $equal;
}

sub meanSd {
    # find mean and standard deviation of input list
    # first pass -- mean
    my @inputs = @_;
    my ($mean, $sd);
    if (@inputs==0) {
	$mean = undef;
	$sd = 0;
    } else {
	my $total = 0;
	foreach my $x (@inputs) { $total+= $x; }
	$mean = $total / @inputs;
	# second pass -- sd
	$total = 0;
	foreach my $x (@inputs) { $total += abs($x - $mean); }
	$sd = $total / @inputs;
    }
    return ($mean, $sd);
}

sub median {
    # return median of given list
    my @inputs = @_;
    @inputs = sort numeric @inputs;
    my $length = @inputs;
    my $mid = floor($length/2);
    return $inputs[$mid];
}

sub numeric {
    return $a <=> $b;
}

sub openDatabase {
    # open connection to pipeline DB
    my $dbfile = shift;
    my $inifile = shift;
    $inifile ||= defaultPipelineDBConfig();
    my $db = WTSI::NPG::Genotyping::Database::Pipeline->new
	(name => 'pipeline',
	 inifile => $inifile,
	 dbfile => $dbfile);
    my $schema = $db->connect(RaiseError => 1,
			      on_connect_do => 'PRAGMA foreign_keys = ON')->schema;
    return $db;
}


sub parseLabel {
    # parse sample label of the form H10 for x=8, y=10; may be obtained from pipeline DB
    # silently return undefined values if name not in correct format
    my $label = shift;
    my ($x, $y);
    if ($label =~ m{^[[:upper:]]\d+$}msx) { # check name format, eg H10
        my @chars = split //msx, $label;
        $x = ord(uc(shift(@chars))) - 64; # letter -> position in alphabet 
        $y = join('', @chars);
        $y =~ s/^0+//msx; # remove leading zeroes from $y
    }
    return ($x, $y);
}

sub parseThresholds {
    # read QC metric thresholds from contents of .json config 
    my %config = @_;
    my %thresholds = %{$config{"Metrics_thresholds"}};
    my %qcMetricNames;
    foreach my $name (@{$config{"name_array"}}) {
        $qcMetricNames{$name} = 1;
    }
    foreach my $name (keys(%thresholds)) { # validate metric names
        unless ($qcMetricNames{$name}) {
            croak "Unknown QC metric name: $!";
        }
    }
    return %thresholds;
}

sub plateLabel {
    # label each plate with plate count and (possibly truncated) plate name
    # ensures meaningful representation of very long plate names
    # also remove whitespace from plate names (prevents error in R script)
    my ($plate, $i, $maxLen, $addPrefix) = @_;
    $maxLen ||= 20;
    $addPrefix ||= 1;
    $plate =~ s/\W+/_/msxg; # get rid of nonword characters, for LaTeX
    my $label;
    my @chars = split //msx, $plate;
    my @head = splice(@chars, 0, $maxLen);
    my $name = join('', @head);
    if ($addPrefix) {
        my $num = sprintf("%03d", $i);
        $label = $num.":".$name;
    } else {
        $label = $name;
    }
    return $label;
}

sub readFileToString {
    # generic method to read a file (eg. json) into a single string variable
    my $inPath = shift();
    if (!(defined($inPath)) || !(-r $inPath)) { carp "Cannot read input path \"$inPath\"\n"; }
    open my $in, "<", $inPath;
    my @lines = <$in>;
    close $in;
    return join('', @lines);
}

sub readMetricResultHash {
    # read QC results data structure from JSON file
    # assumes top-level structure is a hash
    # remove any 'non-metric' data such as plate names
    my $inPath = shift;
    my $configPath = shift;
    my %allResults = readQCResultHash($inPath); # hash of QC data indexed by sample name
    my %metricResults;
    my %metricNames = readQCNameHash($configPath);
    foreach my $sample (keys(%allResults)) {
        my %results = %{$allResults{$sample}};
        foreach my $key (keys(%results)) {
            unless ($metricNames{$key}) {
                delete $results{$key};
            }
        }
        $metricResults{$sample} = \%results;
    }
    return %metricResults;
}

sub readQCFileNames {
    # read default qc file names
    my $inPath = shift();
    my %allNames = readQCNameConfig($inPath);
    my %fileNames = %{$allNames{'file_names'}};
    return %fileNames;
}

sub readQCNameConfig {
    # read qc metric names from JSON config
    my $inPath = shift();
    my %names = %{decode_json(readFileToString($inPath))};
    return %names;
}

sub readQCMetricInputs {
    # default input defined in readQCNameConfig
    my $inPath = shift();
    my %names = readQCNameConfig($inPath);
    my %inputs = %{$names{'input_names'}};
    return %inputs;
}

sub readQCNameArray {
    # default input defined in readQCNameConfig
    my $inPath = shift();
    my %names = readQCNameConfig($inPath);
    my @nameArray = @{$names{'name_array'}};
    return @nameArray;
}

sub readQCNameHash {
    # convenience method, find hash for checking name legality
    # default input defined in readQCNameConfig
    my $inPath = shift();
    my @nameArray = readQCNameArray($inPath);
    my %nameHash;
    foreach my $name (@nameArray) { $nameHash{$name} = 1; }
    return %nameHash;
}

sub readQCShortNameHash {
    my $inPath = shift();
    my %names = readQCNameConfig($inPath);
    my %shortNames = %{$names{'short_names'}};
    return %shortNames;
}

sub readQCResultHash {
    # read QC results data structure from JSON file
    # assumes top-level structure is a hash
    my $inPath = shift;
    my %results = %{decode_json(readFileToString($inPath))};
    return %results;
}

sub readSampleData {
    # read data for given sample names from space-delimited file; return array of arrays of data read
    # optional start, stop points counting from zero
    my ($inPath, $startLine, $stopLine) = @_;
    unless (-e $inPath) { return (); } # silently return empty list if input does not exist
    $startLine ||= 0;
    $stopLine ||= 0;
    my @data;
    open my $in, "<", $inPath || croak "Cannot open input path $inPath: $!";
    my $line = 0;
    while (<$in>) {
	$line++;
	if (m{^\#}msx || $line <= $startLine) { next; } # comments start with a #
	elsif ($stopLine && $line+1 == $stopLine) { last; }
	my @fields = split;
	push(@data, \@fields);
    }
    close $in;
    return @data;    
}

sub readSampleInclusion {
    # get inclusion/exclusion status of each sample in pipeline DB
    # returns a hash reference
    my $dbfile = shift;
    my $result = `echo 'select name,include from sample;' | sqlite3 $dbfile`;
    my @lines = split("\n", $result);
    my %inclusion;
    foreach my $line (@lines) {
	my @fields = split('\|', $line); 
	my $status = pop @fields;
	my $name = join("|", @fields); # OK even if name includes | characters
	$inclusion{$name} = $status;
    }
    return \%inclusion;
}

sub readThresholds {
    # read QC metric thresholds from config path
    my $configPath = shift;
    my %config = %{decode_json(readFileToString($configPath))};
    return parseThresholds(%config);
}

1;
