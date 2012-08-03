# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# define shared constants and subroutines for QC plot scripts

package WTSI::Genotyping::QC::QCPlotShared;

use warnings;
use strict;
use Carp;
use Cwd;
use FindBin qw($Bin);
use POSIX qw(floor);
use Log::Log4perl qw(:easy);
use JSON;
use WTSI::Genotyping::Database::Pipeline;
use Exporter;

Log::Log4perl->easy_init($ERROR);

our @ISA = qw/Exporter/;
our @EXPORT = qw/getDatabaseObject getPlateLocationsFromPath getSummaryStats meanSd median parseLabel readQCNameArray readQCShortNameHash $ini_path/;

use vars qw/$ini_path $INI_FILE_DEFAULT/;
$ini_path = "$Bin/../etc/";
$INI_FILE_DEFAULT = $ENV{HOME} . "/.npg/genotyping.ini";

# read default qc names and thresholds from .json files

# duplicate threshold is currently hard-coded in /software/varinf/bin/genotype_qc/pairwise_concordance_bed


sub getDatabaseObject {
    # set up database object
    my $dbfile = shift;
    my $inifile = shift;
    $inifile ||= $INI_FILE_DEFAULT;
    my $db = WTSI::Genotyping::Database::Pipeline->new
	(name => 'pipeline',
	 inifile => $inifile,
	 dbfile => $dbfile);
    my $schema = $db->connect(RaiseError => 1,
		       on_connect_do => 'PRAGMA foreign_keys = ON')->schema;
    # $db->populate;
    return $db;
}

sub getPlateLocations {
    # get plate and (x,y) location form database
    my $db = shift;
    my @samples = $db->sample->all;
    my %plateLocs;
    $db->in_transaction(sub {
	foreach my $sample (@samples) {
	    my ($plate, $x, $y) = (0,0,0);
	    my $uri = $sample->uri;
	    my $well = ($sample->wells->all)[0]; # assume only one well per sample
	    my $address = $well->address;
	    my $label = $address->label1;
	    $plate = $well->plate;
	    my $plateName = $plate->ss_barcode;
	    $plateLocs{$uri} = [$plateName, $label];  
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
    my %allResults = readMetricResultHash($inPath);
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

sub meanSd {
    # find mean and standard deviation of input list
    # first pass -- mean
    my ($mean, $sd);
    unless (@_) {
	$mean = undef;
	$sd = 0;
    } else {
	my $total = 0;
	foreach my $x (@_) { $total+= $x; }
	$mean = $total / @_;
	# second pass -- sd
	$total = 0;
	foreach my $x (@_) { $total += abs($x - $mean); }
	$sd = $total / @_;
    }
    return ($mean, $sd);
}

sub median {
    # return median of given list
    @_ = sort numeric @_;
    my $length = @_;
    my $mid = floor($length/2);
    return $_[$mid];
}

sub numeric {
    $a <=> $b
}

sub openDatabase {
    # open connection to pipeline DB
    my $dbfile = shift;
    my $inifile = shift;
    $inifile ||= $INI_FILE_DEFAULT;
    my $start = getcwd;
    # very hacky, but ensures correct loading of database using pipeline.ini
    # assumes script is run from a subdirectory of src/perl (eg. perl/t or perl/bin)
    chdir("$Bin/.."); 
    my $db = WTSI::Genotyping::Database::Pipeline->new
	(name => 'pipeline',
	 inifile => $inifile,
	 dbfile => $dbfile);
    my $schema = $db->connect(RaiseError => 1,
			      on_connect_do => 'PRAGMA foreign_keys = ON')->schema;
    $db->populate;
    chdir $start;
    return $db;
}


sub parseLabel {
    # parse sample label of the form H10 for x=8, y=10; may be obtained from pipeline DB
    # silently return undefined values if name not in correct format
    my $label = shift;
    my ($x, $y);
    if ($label =~ m/^[A-Z][0-9]+$/) { # check name format, eg H10
	my @chars = split //, $label;
	$x = ord(uc(shift(@chars))) - 64; # convert letter to position in alphabet 
	$y = join('', @chars);
	$y =~ s/^0+//; # remove leading zeroes from $y
    }
    return ($x, $y);
}

sub readFileToString {
    # generic method to read a file (eg. json) into a single string variable
    my $inPath = shift();
    open IN, "< $inPath";
    my @lines = <IN>;
    close IN;
    return join('', @lines);
}

sub readMetricResultHash {
    # read QC results data structure from JSON file
    # assumes top-level structure is a hash
    # remove any 'non-metric' data such as plate names
    my $inPath = shift;
    my %allResults = readQCResultHash($inPath); # hash of QC data indexed by sample name
    my %metricResults;
    my %metricNames = readQCNameHash();
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
    $inPath ||= $Bin."/../json/qc_name_config.json";
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
    open IN, "< $inPath" || croak "Cannot open input path $inPath: $!";
    my $line = 0;
    while (<IN>) {
	$line++;
	if (/^#/ || $line <= $startLine) { next; } # comments start with a #
	elsif ($stopLine && $line+1 == $stopLine) { last; }
	my @fields = split;
	push(@data, \@fields);
    }
    close IN;
    return @data;    
}

sub readThresholds {
    # read QC metric thresholds from config path
    my $configPath = shift;
    my %config = %{decode_json(readFileToString($configPath))};
    my %thresholds = %{$config{"Metrics_thresholds"}};
    my %qcMetricNames = readQCNameHash();
    foreach my $name (keys(%thresholds)) { # validate metric names
	unless ($qcMetricNames{$name}) {
	    croak "Unknown QC metric name: $!";
	}
    }
    return %thresholds;
}

return 1;
