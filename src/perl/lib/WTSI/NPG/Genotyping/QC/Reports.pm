# Author:  Iain Bancarz, ib5@sanger.ac.uk
# July 2012

# Generate plots and PDF report containing QC results
# Accompanies CSV file generated in Collation.pm

package WTSI::NPG::Genotyping::QC::Reports;

use strict;
use warnings;
use Carp;
use Cwd qw/getcwd abs_path/;
use File::Basename;
use File::Slurp qw/read_file/;
use JSON;
use POSIX qw/strftime/;
use WTSI::NPG::Genotyping::QC::QCPlotShared qw/defaultJsonConfig getDatabaseObject getSummaryStats meanSd median readQCNameArray readQCShortNameHash plateLabel/; 
use WTSI::NPG::Genotyping::Database::Pipeline;
use Exporter;

our @ISA = qw/Exporter/;
our @EXPORT_OK = qw/createReports qcNameFromPath/; 

our $VERSION = '';

our @DB_INFO_HEADERS = qw/run project data_supplier snpset
                        supplier_name rowcol beadchip_number/;
our $ALL_METRICS_NAME = "ALL_METRICS";
our $ALL_PLATES_NAME = "ALL_PLATES";
our @METRIC_NAMES =  qw/identity duplicate gender call_rate heterozygosity 
  magnitude/;

sub createReports {
    # 'main' method to write text and PDF files
    my ($texPath, $resultPath, $idPath, $config, $dbPath, $genderThresholdPath, $qcDir, $introPath, $qcName, $title, $author) = @_;
    $qcName ||= qcNameFromPath($qcDir);
    writeSummaryLatex($texPath, $resultPath, $idPath, $config, $dbPath, 
                      $genderThresholdPath, $qcDir, $introPath,
                      $qcName, $title, $author);
    my $pdfOK = texToPdf($texPath);
    if (not $pdfOK) { carp "Warning: Creation of PDF summary failed."; }
    return $pdfOK;
}

sub dbDatasetInfo {
    # get general information on analysis run(s) from pipeline database
    my $dbfile = shift;
    my $db = getDatabaseObject($dbfile);
    my @datasetInfo;
    my @runs = $db->piperun->all;
    foreach my $run (@runs) {
        my @datasets = $run->datasets->all;
        foreach my $dataset (@datasets) {
            my @info = ($run->name, $dataset->if_project, $dataset->datasupplier->name, $dataset->snpset->name);
            push(@datasetInfo, \@info);
        }
    }
    $db->disconnect();
    return @datasetInfo;
}

sub findPlateFields {
    # find report fields for a single plate, or an entire experiment
    my ($total, $pass, $crRef, $hetRef) = @_;
    my $excl = $total - $pass;
    my $exclPercent = ($excl/$total)*100;
    my @cr = @{$crRef};
    my ($crMean, $crSd) = meanSd(@cr);
    my $crMedian = median(@cr);
    my ($hetMean, $hetSd) = meanSd(@{$hetRef});
    return ($total, $excl, $exclPercent, $crMean, $crMedian, $hetMean);
}

sub getMetricTableHeader {
    # header for pass/fail count tables; use short names if available
    my $metric = shift;
    my %shortNames = %{ shift() };
    my $header;
    if ($shortNames{$metric}) { 
        $header = $shortNames{$metric}; 
    } else { 
        $header = lc($metric);
        $header =~ s/_/ /msxg;
    }
    return $header;
}

sub getPlateInfo {
    # foreach plate, get failure counts and percentage
    # want stats for each individual metric, and for all metrics combined
    my %records = %{ shift() };
    my (%passCounts, %sampleCounts);
    my ($sampleTotal, $passTotal) = (0,0);
    foreach my $sample (keys(%records)) {
        $sampleTotal++;
        my %record = %{$records{$sample}};
        my $plate = $record{'plate'};
        my $samplePass = 1;
        foreach my $metric (@METRIC_NAMES) {
            my ($pass, $val);
            if ($record{$metric}) {
                ($pass, $val) = @{$record{$metric}};
            } else {
                ($pass, $val) = (1, "NA"); # placeholders if metric not in use
            }
            if ($pass==0) { $samplePass = 0; }
            $passCounts{$plate}{$metric} += $pass;
        }
        if ($samplePass) { $passTotal += 1; }
        $passCounts{$plate}{$ALL_METRICS_NAME} += $samplePass;
        $sampleCounts{$plate}++;
    }
    return (\%passCounts, \%sampleCounts, $sampleTotal, $passTotal);
}

sub latexFooter {
    my $footer = "\n\\end{document}\n";
    return $footer;
}

sub latexHeader {
    my ($title, $author) = @_;
    my $date = strftime("%Y-%m-%d %H:%M", localtime(time()));
    # formerly used graphicx, but all plots are now pdf
    my $header = '\documentclass{article} 
\title{'.$title.'}
\author{'.$author.'}
\date{'.$date.'}

\usepackage{pdfpages}

\renewcommand{\familydefault}{\sfdefault} % sans serif font

\begin{document}

\maketitle
';
    return $header;
}

sub latexSectionInput {
    # .tex for "Inputs" section
    my ($qcName, $dbPath) = @_;
    my @lines = ();
    push @lines, "\\section{Input data}\n\n";
    push @lines, textForDatasets($dbPath, $qcName);
    return join("", @lines);
}

sub latexSectionMetrics {
    my $config = shift;
    my $gtPath = shift; # gender thresholds
    my ($mMax, $fMin) = readGenderThreholds($gtPath);
    my @lines = ();
    push @lines, "\\pagebreak\n\n";
    push @lines, "\\subsection{Summary of metrics and thresholds}\n";
    push @lines, "\\label{sec:metric-summary}\n\n";
    my @text = textForMetrics($config, $mMax, $fMin);
	foreach my $table (latexTables(\@text)) { push @lines, $table."\n"; }
    return join("", @lines);
}

sub latexResultNotes {
    my @lines;
    push @lines, "\\subsection{Plots}\n";
    push @lines, "\\subsubsection*{Metric scatterplots}\n";
    push @lines, "Scatterplots are produced for each metric. For analyses with a large number of plates, the results for a given metric may be split across more than one plot.  Note that the identity metric plots may be absent if QC plex results are not available.\n\n";
    push @lines, "\\subsubsection*{Other}\n";
    push @lines, "The following additional plots are included:\n";
    push @lines, "\\begin{itemize}\n";
    push @lines, "\\item Causes of sample failure: Individual and combined\n";
    push @lines, "\\item Scatterplots of call rate versus heterozygosity: All samples, failed samples, and failed samples passing call rate and heterozygosity filters\n";
    push @lines, "\\end{itemize}\n";
    push @lines, "\\clearpage\n";
    return join("", @lines);
}

sub latexSectionResults {
    my ($config, $qcDir, $resultPath, $identityPath) = @_;
    my @lines = ();
    push @lines, "\\section{Results}\n\n";
    push @lines, textForIdentity($identityPath);
    push @lines, "\\subsection{Tables}\n\n";
    my @titles = ("Pass/fail summary",
                  "Key to metric abbreviations", 
                  "Total samples passing filters",
                  "Sample pass rates");
    my @refs = textForPlates($resultPath, $config);
    my @headers = (0,1,1,1);
    my @centre = (0,0,1,1);
    for (my $i=0;$i<@refs;$i++) {
        push @lines, "\\subsubsection*{".$titles[$i]."}\n";
        foreach my $table (latexTables($refs[$i], $headers[$i], $centre[$i])) { 
            push @lines, $table."\n"; 
        }
        if ($i>0) {
            push @lines, "\\clearpage\n\n"; # flush table buffer to output
            push @lines, "\\pagebreak\n\n"; 
        }
    }
    push @lines, latexResultNotes();
    foreach my $metric (@METRIC_NAMES) {
        my @plotPaths = sort(glob($qcDir."/scatter_".$metric."*.pdf"));
        foreach my $plotPath (@plotPaths) {
            push(@lines, "\\includepdf[landscape=true]{".$plotPath."}\n");
        }
    }
    my @morePdf =  qw(failsIndividual failsCombined
                      crHetDensityScatter failScatterPlot failScatterDetail);
    foreach my $name (@morePdf) {
        my $plotPath = $qcDir."/".$name.".pdf";
	if (-e $plotPath) {
	    push(@lines, "\\includepdf[pages={1}]{".$plotPath."}\n");
	} elsif ($name eq 'crHetDensityScatter') {
	    croak "CR/Het density scatter plot not found!";
	} else {
	    push(@lines, "\\begin{itemize}\n");
	    push(@lines, "\\item No failed samples found. Omitted $name graph.\n");
	    push(@lines, "\\end{itemize}\n"); 
	}
    }
    return join("", @lines);
}

sub latexTables {
	# convert array of arrays into one or more strings containing LaTeX tables
	# enforce maximum number of rows per table, before starting a new table 
    # (allows breaking across pages)
	# assume that first row is header; repeat header at start of each table
    my ($rowsRef, $header, $centre, $caption, $label, $maxRows) = @_;
    if (!defined($header)) { $header = 1; } # header=0 stays as 0
    if (!defined($centre)) { $centre = 1; } # centre=0 stays as 0
	$maxRows ||= 38;
	my @rows = @$rowsRef;
	my @tables = ();
	if (@rows > $maxRows) {
        my $headRef;
		if ($header) { $headRef = shift(@rows);}
		my @outRows;
		my $part = 1;
		while (@rows > $maxRows) {
			if ($header) { @outRows = ($headRef,); }
			push(@outRows, splice(@rows, 0, $maxRows));
			my $newCaption;
			if ($caption) { $newCaption = $caption." (Part $part)"; }
			else { $newCaption = ""; }
			push(@tables, latexTableSingle(\@outRows, $header, 
                                           $newCaption, $label));
			$part++;
		}
		if (@rows>0) { # deal with remainder (if any)
			unshift(@rows, $headRef);
			my $newCaption;
			if ($caption) { $newCaption = $caption." (Part $part)"; }
			else { $newCaption = ""; }
			push(@tables, latexTableSingle(\@rows, $header, 
                                           $newCaption, $label));
		}
	} else {
		push(@tables, latexTableSingle(\@rows, $header, $centre, 
                                       $caption, $label));
	}
	return @tables;
}

sub latexTableSingle {
    # convert array of arrays into a (centred) table
    my ($rowsRef, $header, $centre, $caption, $label) = @_;
    if (!defined($header)) { $header = 1; } # header=0 stays as 0
    if (!defined($centre)) { $centre = 1; } # centre=0 stays as 0
    my @rows = @$rowsRef;
    my $cols = @{$rows[0]};
    my $table;
    if ($centre) {
        $table = "\n\\begin{table}[!h]\n\\centering\n\\begin{tabular}{|";
    } else {
        $table = "\n\\begin{table}[!h]\n\\begin{tabular}{|";
    }
    foreach my $i (1..$cols) { $table.=" l |"; }
    $table.="} \\hline\n";
    foreach my $ref (@rows) {
        my @row = @$ref;
        foreach my $item (@row) {
            $item =~ s/[_]/\\_/msxg;
            $item =~ s/%/\\%/msxg;
            if ($header) { $item = "\\textbf{".$item."}"; } # first row
        }
        $table.=join(" & ", @row)." \\\\ \\hline";
        if ($header) { $table .= ' \hline'; $header = 0; } 
        $table.="\n";
    }
    $table.="\\end{tabular}\n";
    if ($caption) { $table.="\\caption{".$caption."}\n"; }
    if ($label) { $table.="\\label{".$label."}\n"; }
    $table.="\\end{table}\n";
    return $table;
}

sub qcNameFromPath {
    # try to find name of analysis program from path
    my $dir = shift;
    my @items = split('/', abs_path($dir));
    if (!($items[-1])) { pop @items; }
    my $qcName;
    foreach my $item (@items) {
        if ($item =~ m{illuminus|gencall}msxi) {
            $qcName = $item; last;
        }
    }
    $qcName ||= pop(@items);
    return $qcName;
}

sub readGenderThreholds {
    # read sample_xhet_gender_thresholds.txt
    my $inPath = shift;
    my ($mMax, $fMin);
    open my $in, "<", $inPath || croak "Cannot open input path $inPath";
    while (<$in>) {
        chomp;
        my @words = split();
        my $thresh = pop(@words);
        if (/^M_max/msx) { $mMax = $thresh; }
        if (/^F_min/msx) { $fMin = $thresh; }
    }
    close $in || croak "Cannot close input path $inPath";
    return ($mMax, $fMin);
}

sub readJson {
    my $inPath = shift;
    my @lines;
    open my $in, "<", $inPath || croak "Cannot open input path $inPath";
    while (<$in>) { push(@lines, $_); }
    close $in || croak "Cannot close input path $inPath";
    my $ref = decode_json(join('', @lines));
    return $ref;
}

sub textForDatasets {
    # text for dataset identification; includes optional directory name
    # fields: run project data_supplier snpset directory
    # print as nested unordered lists (not table rows) to handle long names
    my $dbPath = shift;
    my $qcDir = shift;
    my @headers = @DB_INFO_HEADERS[0..3];
    if ($qcDir) { push(@headers, "directory"); }
    foreach my $header (@headers) { $header =~ s/_/\\_/msxg; }
    my @datasetInfo = dbDatasetInfo($dbPath);
    my @text = ();
    push(@text, "\\begin{itemize}\n");
    foreach my $ref (@datasetInfo) {
        my @fields = @$ref;
        if ($qcDir) { push(@fields, $qcDir); }
	foreach my $field (@fields) { $field =~ s/_/\\_/msxg; }
	my $item = "\\item \\textbf{".$headers[0].":} ".$fields[0]."\n";
	push(@text, $item);
	push(@text, "\\begin{itemize}\n"); # nested list with details
	for (my $i=1;$i<@fields;$i++) {
	    $item = "\\item \\textbf{".$headers[$i].":} ".$fields[$i]."\n";
	    push(@text, $item);
	}
	push(@text, "\\end{itemize}\n");
    }
    push(@text, "\\end{itemize}\n");
    return @text;
}

sub textForIdentity {
    # text for subsection to describe status of identity metric
    my $idResultsPath = shift;
    my %results = %{readJson($idResultsPath)};
    my $idCheck = $results{'identity_check_run'}; # was identity check run?
    my $minSnps = $results{'min_snps'};
    my $commonSnps = $results{'common_snps'}; # Illumina/Sequenom shared SNPs
    my $text = "\\subsection{Identity Metric}\n\n\\begin{itemize}\n \\item Minimum number of SNPs for identity check = $minSnps\n\\item Common SNPs between input and QC plex = $commonSnps\n";
    if ($idCheck) {
	$text.= "\\item Identity check run successfully.\n\\end{itemize}\n\n";
    } else {
	$text.= "\\item \\textbf{Identity check omitted.} All samples pass with respect to identity; scatterplot not created.\n\\end{itemize}\n\n";
    }
    return $text;
}

sub textForMetrics {
    # text for metric threshold/description table
    my ($jsonPath, $mMax, $fMin) = @_;
    if ($mMax < 0.001) { $mMax = sprintf("%.2e", $mMax); }
    else { $mMax = sprintf("%.3f", $mMax); }
    if ($fMin < 0.001) { $fMin = sprintf("%.2e", $fMin); }
    else { $fMin = sprintf("%.3f", $fMin); }
    my %doc = %{readJson($jsonPath)};
    my %thresh = %{$doc{'Metrics_thresholds'}};
    my @headers = qw/metric threshold description/;
    my %descs = %{$doc{'Metric_descriptions'}};
    my %types = %{$doc{'Threshold_types'}};
    my @text = (\@headers,);
    my @names = @METRIC_NAMES;
    foreach my $name (@names) {
        my $thresholdTex;
        if ($name eq 'gender') {
            $thresholdTex = "M\_max=".$mMax."; F\_min=".$fMin;
        } else {
            $thresholdTex = $thresh{$name}." ".lc($types{$name});
        }
        push(@text, [$name, $thresholdTex, $descs{$name}]);
    }
    return @text;
}

sub textForMetricKey {
    my %shortNames = @_;
    my @headers = qw(metric abbreviation);
    my @text = (\@headers,);
    foreach my $metric (@METRIC_NAMES) {
        my @fields = ($metric, $shortNames{$metric});
        push(@text, \@fields);
    }
    return @text;
}

sub textForPass {
    # find pass/fail numbers and rates for each metric
    # return text for tables
    my %shortNames = %{ shift() };
    my %passCounts = %{ shift() };
    my %sampleCounts = %{ shift() };
    my @headers1 = qw/plate samples/;
    my @headers2 = qw/plate/;
    my @metricNames = ();
    foreach my $name (@METRIC_NAMES) { push(@metricNames, $name); }
    push(@metricNames, $ALL_METRICS_NAME);
    foreach my $metric (@metricNames) { 
        push @headers1, getMetricTableHeader($metric, \%shortNames);
        push @headers2, getMetricTableHeader($metric, \%shortNames);
    }
    my @text1 = (\@headers1, );
    my @text2 = (\@headers2, );
    my $sampleTotal = 0;
    my %passTotals;
    my @plates = keys(%sampleCounts);
    @plates = sort @plates;
    my $i = 0;
    foreach my $plate (@plates) { # count of passed/failed samples by plate
        my $plateLabel = plateLabel($plate, $i);
        my @fields1 = ($plateLabel, $sampleCounts{$plate});
        my @fields2 = ($plateLabel, );
        $sampleTotal += $sampleCounts{$plate};
        foreach my $metric (@metricNames) {
            push(@fields1, $passCounts{$plate}{$metric});
            my $rate = $passCounts{$plate}{$metric} / $sampleCounts{$plate};
            push(@fields2,  sprintf("%.3f", $rate));
            $passTotals{$metric} += $passCounts{$plate}{$metric};
        }
        push(@text1, \@fields1);
        push(@text2, \@fields2);
        $i++;
    }
    my $name =  "\\textbf{".lc($ALL_PLATES_NAME)."}"; 
    $name =~ s/_/ /msxg;
    my @fields1 = ($name, $sampleTotal);
    my @fields2 = ($name, );
    foreach my $metric (@metricNames) {
        push(@fields1, $passTotals{$metric});
        my $rate = $passTotals{$metric} / $sampleTotal;
        push(@fields2,  sprintf("%.3f", $rate));
    }
    push(@text1, \@fields1);
    push(@text2, \@fields2);
    return (\@text1, \@text2);
}

sub textForPassSummary {
    my ($samples, $passed) = @_;
    my $passPercent = sprintf("%.01f", 100*($passed/$samples));
    my @text = (
        ["Total samples", $samples],
        ["Samples passing QC filters", $passed],
        ["Pass rate", $passPercent."%"],
        );
    return @text;
}

sub textForPlates {
    # produce text for two tables: pass counts and pass rates
    my $resultPath = shift;
    my $config = shift;
    my $resultsRef = readJson($resultPath);
    my ($ref1, $ref2, $samples, $passed) = getPlateInfo($resultsRef, $config);
    my %passCounts = %{$ref1};
    my %sampleCounts = %{$ref2};
    my %shortNames = readQCShortNameHash($config);
    my @textSummary = textForPassSummary($samples, $passed);
    my @textKey = textForMetricKey(%shortNames);
    my ($textRef1, $textRef2) = textForPass(\%shortNames, \%passCounts, 
                                            \%sampleCounts);
    return (\@textSummary, \@textKey, $textRef1, $textRef2);
}

sub texToPdf {
    my $texPath = shift;
    my $cleanup = shift;
    $cleanup ||= 1;
    $texPath = abs_path($texPath);
    my $texDir = dirname($texPath);
    # run pdflatex twice; needed to get cross-references correct
    my $args = '-output-directory '.$texDir.' '.$texPath;
    system('pdflatex '.$args.' -draftmode > /dev/null'); # draftmode is faster
    my $result = system('pdflatex '.$args.' > /dev/null');
    if ($cleanup) {
	my $texBase = basename($texPath, '.tex');
	my @suffixes = qw/.aux .dvi .lof/; # keeps .log, .tex, .pdf
	foreach my $suffix (@suffixes) {
	    system("rm -f $texDir/$texBase$suffix");
	}
    }
    if ($result==0) { return 1; }
    else { return 0; }
}

sub writeSummaryLatex {
    # write .tex file for report
    my ($texPath, $resultPath, $idPath, $config, $dbPath, $genderThresholdPath,
        $qcDir, $introPath, $qcName, $title, $author) = @_;
    $texPath ||= "pipeline_summary.tex";
    $title ||= "Genotyping QC Report";
    $author ||= "Wellcome Trust Sanger Institute\\\\\n".
        "Illumina Beadchip Genotyping Pipeline";
    $config ||= defaultJsonConfig();
    $qcDir ||= ".";
    $qcName ||= "Unknown";
    open my $out, ">", $texPath || croak "Cannot open output path $texPath";
    print $out latexHeader($title, $author);
    print $out latexSectionInput($qcName, $dbPath);
    print $out read_file($introPath); # new section = Preface
    print $out latexSectionMetrics($config, $genderThresholdPath);
    print $out latexSectionResults($config, $qcDir, $resultPath, $idPath);
    print $out latexFooter();
    close $out || croak "Cannot close output path $texPath";
}

1;
