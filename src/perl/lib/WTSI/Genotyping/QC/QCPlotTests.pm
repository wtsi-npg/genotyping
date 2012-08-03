#!/software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

package WTSI::Genotyping::QC::QCPlotTests;

use strict;
use warnings;
use Carp;
use Cwd qw /getcwd abs_path/;
use File::Temp qw/tempfile tempdir/;
use FindBin qw /$Bin/;
use POSIX qw/floor strftime/;
use JSON;
use XML::Parser;
use WTSI::Genotyping::QC::QCPlotShared;  # must have path to WTSI in PERL5LIB
use WTSI::Genotyping::Database::Pipeline;
use Exporter;

our @ISA = qw/Exporter/;
our @EXPORT_OK = qw/jsonPathOK pngPathOK xmlPathOK createTestDatabase createTestDatabasePlink readPlinkSampleNames $ini_path/;

sub columnsMatch {
    # check for difference in specific columns (of space-delimited files, can also do for other separators)
    # use where reference file sample names column has been scrubbed
    my ($path1, $path2, $indicesRef, $verbose, $sep, $commentPattern) = @_;
    $sep = '\s+';
    $verbose ||= 0;
    $commentPattern ||= "^#";
    my $match = 1;
    my @indices = @$indicesRef; # column indices to check
    my ($line1, $line2);
    open(IN1, $path1) || croak "Cannot open $path1: $!";
    open(IN2, $path2) || croak "Cannot open $path2: $!";
    my $j = 0; # line count
    while (1) {
	$line1 = readline(IN1);
	$line2 = readline(IN2);
	if ($line1) {chomp $line1;}
	if ($line2) {chomp $line2;}
	if (!($line1) && !($line2)) {
	    # both files ended
	    if ($verbose) {print STDERR "No difference found!\n"; }
	    last;
	} elsif (!($line1) || !($line2)) {
	    # only one file ended; file lengths differ
	    if ($verbose) {print STDERR "Warning: File lengths of $path1 and $path2 differ.\n";}
	    $match = 0;
	    last;
	} elsif ($line1=~$commentPattern && $line2=~$commentPattern) {
	    next;
	} else {
	    chomp $line1;
	    chomp $line2;
	    my @words1 = split(/$sep/, $line1);
	    my @words2 = split(/$sep/, $line2);
	    foreach my $i (@indices) {
		if ($words1[$i] ne $words2[$i]) {
		    if ($verbose) {print STDERR "Warning: Difference in column index $i at line index $j.\n";}
		    $match = 0;
		    last;
		}
	    }
	    $j++;
	    if (not $match) { last; }
	}
    }
    close IN1;
    close IN2;
    return $match;
}


sub createTestDatabase {
    # create temporary test database with given sample names
    my ($namesRef, $dbfile, $runName, $projectName) = @_;
    $runName ||= "pipeline_run";
    $projectName ||= "dataset_socrates";
    my @names;
    if ($namesRef) { 
	@names = @$namesRef; 
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


sub diffGlobs {
    # glob for files in output and reference directories
    # want files to be identical
    # if output file differs or is missing, record as failure
    my ($refDir, $outDir, $fh, $tests, $failures, $globExpr, $colsRef) = @_;
    $tests ||= 0;
    $failures ||= 0;
    $globExpr ||= '*.txt';
    my $colString;
    if ($colsRef) { $colString = join(":", @$colsRef); }
    else { $colString = "ALL_COLS"; }
    print $fh "###\tStarting diff tests: $refDir $outDir $globExpr $colString\n";
    my $startDir = getcwd();
    $outDir = abs_path($outDir);
    chdir($refDir);
    my @ref = glob($globExpr);
    @ref = sort(@ref);
    foreach my $name (@ref) {
	$tests++;
	my $outPath = $outDir.'/'.$name;
	if (not(-r $outPath)) { 
	    print $fh "FAIL\tdiff $name (output file missing)\n"; $failures++; 
	} elsif ($colsRef) { # diff on specific columns only
	    if (!(columnsMatch($name, $outPath, $colsRef))) {
		print $fh "FAIL\tdiff $name (columns differ)\n"; $failures++;
	    } else {
		print $fh "OK\tdiff $name\n"; 
	    }
	} elsif (filesDiffer($name, $outPath)) { # diff entire files
	    print $fh "FAIL\tdiff $name (files differ)\n"; $failures++; 
	} else { 
	    print $fh "OK\tdiff $name\n"; 
	}
    }
    chdir($startDir);
    return ($tests, $failures);
}

sub filesDiffer {
    # use system call to diff to see if two files differ
    # will return true if one or both files are missing!
    my ($path1, $path2) = @_;
    my $result = system('diff -q '.$path1.' '.$path2.' 2> /dev/null');
    if ($result==0) { return 0; } # no difference
    else { return 1; }
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
    return pathOK($_[0], 2);
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
    return multiplePathsOK(\@_, 0);
}

sub pngPathOK {
    # check on png path
    return pathOK($_[0], 0);
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
    # check if given path exists and contents are in correct format
    my $inPath = shift;
    my $mode = shift;
    my $ok = 0;
    if (-r $inPath) {
	my $fh;
	open $fh, "< $inPath";
	if ($mode==0) { $ok = pngOK($fh); }
	elsif ($mode==1) { $ok = xmlOK($fh); }
	elsif ($mode==2) { $ok = jsonOK($fh); }
	close $fh;
    }
    return $ok;
}

sub readPlinkSampleNames {
    # read sample names from a PLINK .fam file
    my $famPath = shift;
    open my $in, "< $famPath" || croak "Cannot open input $famPath";
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

sub readPrefix {
    # read PLINK file prefix from a suitable .txt file; prefix is first non-comment line
    # use to anonymise test data (name of chip/project not visible)
    my $inPath = shift;
    open IN, "< $inPath" || croak "Cannot open input file $inPath: $!";
    my $prefix = 0;
    while (<IN>) {
	unless (/^#/) { # comments start with #
	    chomp;
	    $prefix = $_;
	    last;
	}
    }
    close IN;
    return $prefix;
}

sub testPlotRScript {
    # test for low-level R plotting scripts
    # R script run with any preliminary arguments, followed by one or more PNG output paths
    # typical expected arguments: Rscript, script path, input, output paths
    # applies to: [box|bean]plot*.R, heatmapCrHetDensity.R, plotCrHetDensity.R, plot[Cr|Het|XYdiff]Plate.R
    my $allOK= 1;
    my @args = @{shift()};
    my @outputs = @{shift()};
    my $verbose = shift;
    my $silentR = shift;
    $verbose ||= 0;
    $silentR ||= 1; # R command made silent by default
    my $cmd = join(' ', @args).' '.join(' ', @outputs);
    if ($verbose) { print $cmd."\n"; }
    if ($silentR) { $cmd .= " > /dev/null 2> /dev/null " }; # suppress stdout/stderr from R script
    my $startTime = time();
    my $result = eval { system($cmd); }; # execute R script, capture any errors
    if (not(defined($result))) { # check for script error 
	if ($verbose) { print "\tScript error!\n"; }
	$allOK = 0; 
    } else { # check required output files
	$allOK = validatePng(\@outputs, $startTime);
    }
    return $allOK;
}

sub timeNow {
    # return a string representing the present (local) time
    my $tf = shift;
    $tf ||= "%Y-%m-%d_%H:%M:%S";
    return strftime($tf, localtime());
}

sub validatePng {
    # check expected PNG output paths for validity
    my @outputs = @{ shift() };
    my $startTime = shift;
    $startTime ||= time();
    my $ok = 1;
    foreach my $output (@outputs) {
	if (not -e $output) { 
	    confess "PNG output $output not found: $!"; 
	    $ok = 0; 
	    last; 
	} 
	my $modTime = (stat($output))[9];
	if ($modTime < $startTime - 1) { # too old to have been generated by this script invocation
	    confess "PNG output $output not generated (older than script run time): $!"; 
	    $ok = 0; 
	    last; 
	}
	open PNG, "< $output" || confess "Cannot open PNG file $output: $!";
	if (pngOK(\*PNG) == 0) { 
	    confess "Incorrect PNG header in $output: $!"; 
	    $ok = 0; # close filehandle before breaking out
	}
	close PNG || confess "Cannot close PNG file $output: $!";
	if ($ok==0) { last; }
    }
    return $ok;
}

sub wrapCommand {
    # generic wrapper for a system call; assume non-zero return value indicates an error
    # if given a filehandle, execute command in test mode and print result; otherwise just run command
    # increment and return the given test/failure counts
    my ($cmd, $fh, $tests, $failures, $verbose) = @_;
    my $result;
    $tests ||= 0;
    $failures ||= 0;
    $verbose ||= 0;
    $tests++;
    if ($fh) {
	$result = system($cmd);  # return value of $cmd, or undef for unexpected Perl error

    $result == 0 or confess "$cmd failed: $?\n";
	if (not(defined($result)) || $result != 0) { 
	    $failures++; 
	    if ($verbose) {print $fh "FAIL\t$cmd\n"; }
	} elsif ($verbose) { 
	    print $fh "OK\t$cmd\n";
	}
    } else {
      system($cmd) == 0 or confess "$cmd failed: $?\n";
    }
    return ($tests, $failures);
}

sub wrapCommandList {
    # wrap multiple commands in a list
    my ($cmdsRef, $tests, $failures, $verbose, $omitRef, $logPath) = @_;
    my @cmds = @$cmdsRef;
    my $total = @cmds;
    my @omits;
    if ($omitRef) { @omits = @$omitRef; }
    else { for (my $i=0;$i<$total;$i++) { push(@omits, 0); } }
    my $fh;
    my $start = time();
    if ($logPath) { open $fh, "> $logPath" || croak "Cannot open log $logPath: $!"; }
    else { $fh = *STDOUT; }
    if ($verbose) { print timeNow()." Command list started.\n"; }
    for (my $i=0;$i<$total;$i++) {
	if ($omits[$i]) { 
	    if ($verbose) { print timeNow()." Omitting $cmds[$i]\n"; } 
	} else {
	    ($tests, $failures) = wrapCommand($cmds[$i], $fh, $tests, $failures);
	    if ($verbose) { print timeNow()." Finished command ".($i+1)." of $total.\n"; }
	}
    }
    if ($verbose) {
	my $duration = time() - $start;
	print "Command list finished. Duration: $duration s\n";
    }
    if ($logPath) { close $fh; }
    return ($tests, $failures);
}

sub wrapPlotCommand {
    # wrapper to (optionally) run tests while executing R script to do plot
    # if not in test mode, just do system call
    # @outputs = PNG output files
    # @args = all other arguments (including paths to R executable and script)
    # caller = name of calling script
    my ($argsRef, $outputsRef, $test, $verbose) = @_;
    my @args = @$argsRef;
    my @outputs = @$outputsRef;
    $test ||= 1;
    $verbose ||= 0;
    my $plotsOK = 1;
    my $cmd = join(' ', @args).' '.join(' ', @outputs);
    if ($test) {
	my $result = testPlotRScript($argsRef, $outputsRef, $verbose); 
	if ($result==0) { 
	    print STDERR "ERROR: Plotting command failed: $cmd\n";
	    $plotsOK = 0;
	} 
    } else {
	system($cmd);
    }
    return $plotsOK;
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
    return pathOK($_[0], 1);
}

return 1; # must have a true return value for module import
