#!/software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

package WTSI::Genotyping::QC::QCPlotTests;

use strict;
use warnings;
use Cwd qw /getcwd abs_path/;
use XML::Parser;
use WTSI::Genotyping::QC::QCPlotShared;  # must have path to WTSI in PERL5LIB


sub columnsMatch {
    # check for difference in specific columns (of space-delimited files, can also do for other separators)
    # use where reference file sample names column has been scrubbed
    my ($path1, $path2, $indicesRef, $verbose, $sep) = @_;
    $sep = '\s+';
    $verbose ||= 1;
    my $match = 1;
    my @indices = @$indicesRef; # column indices to check
    my ($line1, $line2);
    open(IN1, $path1) || die "Cannot open $path1: $!";
    open(IN2, $path2) || die "Cannot open $path2: $!";
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

sub diffGlobs {
    # glob for files in output and reference directories
    # want files to be identical
    # if output file differs or is missing, record as failure
    my ($refDir, $outDir, $fh, $tests, $failures, $globExpr, $colsRef) = @_;
    $tests ||= 0;
    $failures ||= 0;
    $globExpr ||= '*.txt';
    my @cols;
    if ($colsRef) { @cols = @$colsRef; }
    else { @cols = (); }
    print $fh "###\tStarting diff tests: $refDir $outDir $globExpr\n";
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
	} elsif (@cols && !(columnsMatch($name, $outPath, @cols))) {
	    print $fh "FAIL\tdiff $name (columns differ)\n"; $failures++;
	} elsif (filesDiffer($name, $outPath)) { 
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
    if ($silentR) { $cmd .= " > /dev/null 2> /dev/null " }; # suppress stdout/stderr from R script
    my $result = eval { system($cmd); }; # execute R script, capture any errors
    if (not(defined($result))) { # check for script error 
	if ($verbose) { print "\tScript error!\n"; }
	$allOK = 0; 
    } else { # check required output files
	foreach my $output (@outputs) {
	    if (not -e $output) { 
		if ($verbose) { print "\tOutput $output not found!\n"; }
		$allOK = 0; 
		last; 
	    }
	    open PNG, "< $output" || die "Cannot open PNG file $output: $!";
	    if (pngOK(\*PNG) == 0) { 
		if ($verbose) { print "Incorrect PNG header in $output\n"; }
		$allOK = 0; 
		last; 
	    }
	    close PNG || die "Cannot close PNG file $output: $!";
	}
    }
    return $allOK;
}

sub wrapCommand {
    # generic wrapper for a system call; assume non-zero return value indicates an error
    # if given a filehandle, execute command in test mode and print result; otherwise just run command
    # increment and return the given test/failure counts
    my ($cmd, $fh, $tests, $failures) = @_;
    my $result;
    $tests ||= 0;
    $failures ||= 0;
    $tests++;
    if ($fh) {
	$result = eval { system($cmd); }; # return value of $cmd, or undef for unexpected Perl error
	if (not(defined($result)) || $result != 0) { $failures++; print $fh "FAIL\t$cmd\n"; } 
	else { print $fh "OK\t$cmd\n"; }
    } else {
	system($cmd);
    }
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

return 1; # must have a true return value for module import
