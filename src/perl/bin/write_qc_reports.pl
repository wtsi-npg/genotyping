#! /software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# October 2012

# not used for initial report generation; run_qc imports from Reports.pm

use strict;
use warnings;
use Getopt::Long;
use Carp;
use WTSI::NPG::Genotyping::QC::QCPlotShared qw(defaultJsonConfig 
                                               defaultTexIntroPath);
use WTSI::NPG::Genotyping::QC::Reports qw(createReports qcNameFromPath);

my $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";
my $defaultInput = ".";
my $defaultPrefix = "pipeline_summary";

my ($help, $prefix, $texPath, $iniPath, $resultPath, $configPath, 
    $dbPath, $genderThresholdPath, $qcDir, $texIntroPath, $qcName);

GetOptions("help"        => \$help,
           "prefix=s"    => \$prefix,
           "ini=s"       => \$iniPath,
           "input=s"     => \$qcDir,
           "database=s"  => \$dbPath,
    );

if ($help) {
    print STDERR "Usage: $0 [ options ]
Convenience script to regenerate the PDF report file.
This script does not regenerate plots contained in the PDF report.  
In order to regenerate plots, re-run the individual plotting scripts, 
such as plot_scatter_metric.pl for metric scatterplots.
Options:
--input             Path to \"supplementary\" directory containing QC results.
                    Defaults to current working directory.
--prefix            Prefix for output files.  Defaults to $defaultPrefix
                    Filenames will be of the form [prefix].pdf
--ini               .ini path for configuration; defaults to $DEFAULT_INI
--database          Path to .db file containing pipeline SQLite database
--help              Print this help text and exit
";
    exit(0);
}

$prefix ||= $defaultPrefix;
$texPath = $prefix.".tex";
$iniPath ||= $DEFAULT_INI;
$qcDir ||= $defaultInput;
$qcName = qcNameFromPath($qcDir."/.."); # look at parent of supplementary dir
foreach my $input (($qcDir, $dbPath, $iniPath)) {
    if (!(-e $input)) { croak "Input path \"$input\" does not exist!"; }
}
if (!(-d $qcDir)) { croak "Path $qcDir is not a directory!"; }
$configPath = defaultJsonConfig($iniPath);
$texIntroPath = defaultTexIntroPath($iniPath);
$resultPath = $qcDir."/qc_results.json";
$genderThresholdPath = $qcDir."/sample_xhet_gender_thresholds.txt";

createReports($texPath, $resultPath, $configPath, $dbPath, 
              $genderThresholdPath, $qcDir, $texIntroPath, $qcName);
