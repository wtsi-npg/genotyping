#!/software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

package WTSI::Genotyping::QC::QCPlotTests;

use strict;
use warnings;
use Carp;
use Cwd qw /getcwd abs_path/;
use File::Temp qw/mktemp tempdir/;
use FindBin qw /$Bin/;
use POSIX qw/floor strftime/;
use JSON;
use XML::Parser;
use WTSI::Genotyping::QC::QCPlotShared qw/$ini_path/; 
use WTSI::Genotyping::Database::Pipeline;
use Exporter;

our @ISA = qw/Exporter/;
our @EXPORT_OK = qw/jsonPathOK pngPathOK xmlPathOK createTestDatabase createTestDatabasePlink readPlinkSampleNames $ini_path/;

sub createTestDatabase {
    # create temporary test database with given sample names
    my ($namesRef, $dbfile, $runName, $projectName, $uriStrip) = @_;
    $runName ||= "pipeline_run";
    $projectName ||= "dataset_socrates";
    $uriStrip ||= 1; # strip off excess colon-delimited uri fields, eg. foo:bar:item001 -> item001
    my (@names, %names);
    if ($namesRef) { 
	@names = @$namesRef; 
	if ($uriStrip) {
	    for my $i (0..@names-1) {
		my @fields = split(/:/, $names[$i]);
		my $name = pop(@fields);
		if ($names{$name}) { croak "Error; sample name $name not unique after removing URI prefixes"; }
		else { $names[$i] = $name; $names{$name}=1; }  
	    }
	}
    } else {
	foreach my $i (1..20) { push @names, sprintf("sample_%03i", ($i)); }
    }
    $dbfile ||= tempdir(CLEANUP => 1).'/pipeline.db'; # remove database file on successful script exit
    my $db = WTSI::Genotyping::Database::Pipeline->new
	(name => 'pipeline',
	 inifile => "$ini_path/pipeline.ini",
	 dbfile => $dbfile);
    my $schema = $db->connect(RaiseError => 1,
			      on_connect_do => 'PRAGMA foreign_keys = ON')->schema;
    $db->populate;
    ## (supplier, snpset) table objects are required 
    my $supplier = $db->datasupplier->find_or_create({name => $ENV{'USER'},
						      namespace => 'wtsi'});
    my $snpset = $db->snpset->find({name => 'HumanOmni25-8v1'});
    ## additional database setup
    my $run = $db->piperun->find_or_create({name => $runName,
					    start_time => time()});
    my $dataset = $run->add_to_datasets({if_project => $projectName,
				     datasupplier => $supplier,
				     snpset => $snpset});
    my $pass = $db->state->find({name => 'autocall_pass'});
    my $supplied = $db->method->find({name => 'Supplied'});
    $db->in_transaction(sub {
	foreach my $i (0..@names-1) {
	    my $sample = $dataset->add_to_samples
		({name => $names[$i],
		  beadchip => 'ABC123456',
		  include => 1});
	    $sample->add_to_states($pass);
	    my $gender = $db->gender->find({name => 'Not Available'});
	    $sample->add_to_genders($gender, {method => $supplied});
	    my ($plate, $addr) = createPlateAddress($db, $i);
	    $sample->add_to_wells({address => $addr,
				   plate => $plate});
	}
			});
    $db->disconnect();
    return $dbfile;
}

sub createTestDatabasePlink {
    # convenience method to create a test pipline database with sample names from a plink .fam file
    my ($famPath, $dbPath) = @_;
    my @names = readPlinkSampleNames($famPath);
    createTestDatabase(\@names, $dbPath);
    return 1;
}

sub createPlateAddress {
    # create plate and address objects in pipeline DB
    my ($db, $sampleNum, $rows, $cols) = @_;
    $rows ||= 12;
    $cols ||= 8;
    my $plateSize = $rows*$cols;
    my $plateSuffix = sprintf("plate%04i", (floor($sampleNum/$plateSize)+1));
    my $plate = $db->plate->find_or_create({ss_barcode => 'SS-'.$plateSuffix,
					    if_barcode => 'IF-'.$plateSuffix});
    my $i = $sampleNum % $plateSize; # index within plate
    my $row = pack("c", floor($i / $rows)+65);
    my $col = $i % $rows+1;
    my $lab1 = $row.sprintf("%02d", $col);
    my $lab2 = $row.$col;
    my $addr = $db->address->find_or_create({label1 => $lab1,
					     label2 => $lab2});
    return ($plate, $addr); 
}

sub jsonOK {
    # check if given filehandle contains valid JSON
    my $fh = shift;
    my $ok = 0;
    my $maxLength = 100*(10**6);
    my $contents;
    read($fh, $contents, $maxLength); 
    my $result = eval { my $stuff = decode_json($contents);  }; # trap any errors from parser
    if (defined($result)) { $ok = 1; }
    return $ok;
}

sub jsonPathOK {
    my $path = shift;
    return pathOK($path, 2);
}

sub pngOK {
    # check given filehandle for correct png header
    # return 1 if png header is valid, 0 otherwise
    my $fh = shift;
    my @correctHeader = (137, 80, 78, 71, 13, 10, 26, 10); # 'magic numbers' required in png header
    my $header;
    my $length = read($fh, $header, @correctHeader);
    my @header = split('', $header);
    my $ok = 1;
    if ($length != @correctHeader) { 
	$ok = 0; 
    } else {
	for (my $i=0;$i<@correctHeader;$i++) {
	    if (ord($header[$i]) != $correctHeader[$i]) { $ok = 0; last; }
	}
    }
    return $ok;
}

sub pngMultiplePathsOK {
    my @input = @_;
    return multiplePathsOK(\@input, 0);
}

sub pngPathOK {
    # check on png path; optional second argument is start time (ie. oldest permitted age of file)
    my ($path, $start) = @_;
    return pathOK($path, 0, $start);
}

sub multiplePathsOK {
    # check on multiple paths
    my @paths = @{ shift() };
    my $mode = shift;
    my $ok = 1;
    foreach my $path (@paths) {
	unless (pathOK($path, $mode)) { $ok = 0; last; }
    }
    return $ok;
}

sub pathOK {
    # check if given path exists and contents are in correct format; optionally, check modification time
    my $inPath = shift;
    my $mode = shift;
    my $startTime = shift;
    my $ok = 0;
    my $modTime = (stat($inPath))[9];
    if (not -r $inPath) {
	carp "Warning: Cannot read path $inPath";
    } elsif ($startTime && ($modTime < $startTime-1)) {
	carp "Warning: Path $inPath last modified before given start time";
    } else {
	my $fh;
	open $fh, "<", $inPath;
	if ($mode==0) { $ok = pngOK($fh); }
	elsif ($mode==1) { $ok = xmlOK($fh); }
	elsif ($mode==2) { $ok = jsonOK($fh); }
	close $fh;
	if (not $ok) { carp "Warning: File format not OK for $inPath"; }
    } 
    return $ok;
}

sub readPlinkSampleNames {
    # read sample names from a PLINK .fam file; convenience method to get names list for createTestDatabase
    my $famPath = shift;
    open my $in, "<", $famPath || croak "Cannot open input $famPath";
    my %samples;
    while (<$in>) {
	my @words = split;
	my $name = $words[1];
	if ($samples{$name}) {
	    carp "Sample name $name repeated in $famPath";
	} else {
	    $samples{$name} = 1;
	}
    }
    close $in || croak "Cannot close input $famPath";
    my @samples = keys(%samples);
    @samples = sort(@samples);
    return @samples;
}

sub timeNow {
    # return a string representing the present (local) time
    my $tf = shift;
    $tf ||= "%Y-%m-%d_%H:%M:%S";
    return strftime($tf, localtime());
}

sub wrapPlotCommand {
    # wrapper to execute R script and check PNG output
    # @args = all other arguments (including paths to R executable and script)
    # @outputs = PNG output files (must come after other @args on command line)
    my ($argsRef, $outputsRef, $returnOutput) = @_;
    my @args = @$argsRef;
    my @outputs = @$outputsRef;
    $returnOutput ||= 0;
    my $plotsOK = 1;
    my $temp = mktemp("r_script_output_XXXXXX"); # creates temporary filename
    my $cmd = join(' ', @args).' '.join(' ', @outputs)." >& $temp"; # assumes csh for redirect
    my $startTime = time();
    my $result = system($cmd);
    my $info;
    if ($result != 0) {
	open my $in, "<", $temp || croak "Could not open temporary file $temp";
	$info = join("", <$in>);
	close $in || croak "Could not close temporary file $temp";
	carp "Warning: Non-zero return code from command \"$cmd\". Command output: \"$info\"";
	$plotsOK = 0;
    } else {
	foreach my $out (@outputs) {
	    my $ok = pngPathOK($out, $startTime);
	    if (not $ok) { 
		carp "Problem creating output file $out"; 
		$plotsOK = 0;
	    }
	}
    }
    system("rm -f $temp");
    if ($returnOutput) { return ($plotsOK, $cmd, $info); }
    else { return $plotsOK; }
}

sub xmlOK {
    # check if given filehandle contains valid xml
    # also works for XHTML, eg. as written by Perl CGI
    my $fh = shift;
    my $ok = 0;
    my $p = new XML::Parser();
    my $result = eval {$p -> parse($fh);}; # trap any errors from parser
    if (defined($result)) { $ok = 1; }
    return $ok;
}

sub xmlPathOK {
    # check on xml path
    my $path = shift;
    return pathOK($path, 1);
}

return 1; # must have a true return value for module import
