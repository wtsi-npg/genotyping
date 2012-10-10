# Author:  Iain Bancarz, ib5@sanger.ac.uk
# July 2012

# Generate human-readable reports containing QC results

package WTSI::Genotyping::QC::Reports;

use strict;
use warnings;
use Carp;
use Cwd qw/getcwd abs_path/;
use JSON;
use POSIX qw/strftime/;
use WTSI::Genotyping::QC::QCPlotShared qw/defaultJsonConfig getDatabaseObject getSummaryStats meanSd median readQCNameArray readQCShortNameHash/; 
use WTSI::Genotyping::Database::Pipeline;
use Exporter;

our @ISA = qw/Exporter/;
our @EXPORT_OK = qw/createReports writeSummaryLatexNew/;
our @dbInfoHeaders = qw/run project supplier snpset/;
our $allMetricsName = "ALL_METRICS";
our $allPlatesName = "ALL_PLATES";
our @METRIC_NAMES =  qw/identity duplicate gender call_rate heterozygosity 
  magnitude/;

sub createReports {
    # 'main' method to write text and CSV files
    my ($results, $db, $csv, $tex, $config, $qcDir, $title, $author, 
        $introPath, $qcName) = @_; # I/O paths
    $author ||= "";
    if (!$qcName) { 
        my @items = split('/', abs_path($qcDir));
        $qcName = pop(@items);
    }
    my $csvOK = writeCsv($results, $db, $config, $csv);
    if (not $csvOK) { carp "Warning: Creation of CSV summary failed."; }
    writeSummaryLatex($tex, $results, $config, $db, $qcDir, $title, $author,
        $introPath, $qcName);
    my $pdfOK = texToPdf($tex);
    if (not $pdfOK) { carp "Warning: Creation of PDF summary failed."; }
    my $ok = $csvOK && $pdfOK;
    return $ok;
}

sub dbDatasetInfo {
    # get general information on analysis run(s) from pipeline database
    my $dbfile = shift;
    my $db = getDatabaseObject($dbfile);
    my @datasetInfo;
    my @runs = $db->piperun->all;
    foreach my $run (@runs) {
	my @info;
	my @datasets = $run->datasets->all;
	foreach my $dataset (@datasets) {
	    @info = ($run->name, $dataset->if_project, $dataset->datasupplier->name, $dataset->snpset->name);
	    push(@datasetInfo, \@info);
	    #print join("\t", @info)."\n";
	}
    }
    return @datasetInfo;
}

sub dbSampleInfo {
    # get general information on analysis run from pipeline database
    # return a hash indexed by sample
    my $dbfile = shift;
    my $db = getDatabaseObject($dbfile);
    my %sampleInfo;
    my @runs = $db->piperun->all;
    foreach my $run (@runs) {
	my @info;
	my @datasets = $run->datasets->all;
	foreach my $dataset (@datasets) {
	    my @samples = $dataset->samples->all;
	    foreach my $sample (@samples) {
		@info = ($run->name, $dataset->if_project, $dataset->datasupplier->name, $dataset->snpset->name);
		$sampleInfo{$sample->uri} = \@info;
	    }
	}
    }
    return %sampleInfo;
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

sub getCsvHeaders {
    my $config = shift;
    my @headers = @dbInfoHeaders;
    push @headers, qw/sample plate well pass/;
    foreach my $metric (@METRIC_NAMES) {
	my @suffixes;
	if ($metric eq 'gender') {
	    @suffixes = qw/pass xhet inferred supplied/;
	} else {
	    @suffixes = qw/pass value/;
	}
	foreach my $suffix (@suffixes) {
	    push(@headers, $metric."_".$suffix);
	}
    }
    return @headers;
}

sub getMetricTableHeader {
    my $metric = shift;
    my %shortNames = %{ shift() };
    my $header;
    if ($shortNames{$metric}) { 
        $header = $shortNames{$metric}; 
    } else { 
        $header = lc($metric);
        $header =~ s/_/ /g;
    }
    return $header;
}

sub getPlateInfo {
    # foreach plate, get failure counts and percentage
    # want stats for each individual metric, and for all metrics combined
    my %records = %{ shift() };
    my (%passCounts, %sampleCounts, %passRates, $key);
    foreach my $sample (keys(%records)) {
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
        $passCounts{$plate}{$allMetricsName} += $samplePass;
        $sampleCounts{$plate}++;
    }
    return (\%passCounts, \%sampleCounts);
}

sub getSampleInfo {
    # for each sample, get plate, well, metric values and pass/fail status
    # also get overall sample pass/fail
    my %records = %{ shift() };
    my @sampleFields;
    my @samples = keys(%records);
    @samples = sort @samples;
    foreach my $sample (@samples) {
	if (not $records{$sample}) { croak "No QC results found for sample $sample!"; }
	my %record = %{$records{$sample}};
	my $samplePass = 1;
	my @fields = ($sample, $record{'plate'}, $record{'address'}, $samplePass); # $samplePass is placeholder
	foreach my $metric (@METRIC_NAMES) {
	    if (not $record{$metric}) { 
		push(@fields, (1, "NA")); # no results found; use placeholders for pass/fail and metric value
	    } else {
		my @status =  @{$record{$metric}}; # pass/fail and one or more metric values
		if ($status[0]==0) { $samplePass = 0; }
		push(@fields, @status); 
	    }
	}
	$fields[3] = $samplePass;
	push @sampleFields, \@fields;
    }
    return @sampleFields;
}


sub latexAllPlots {
    my $graphicsDir = shift;
    my @plots = qw/failsIndividual.png failsCombined.png failScatterPlot.png failScatterDetail.png sample_xhet_gender.png cr_boxplot.png het_boxplot.png xydiff_boxplot.png crHetDensityHeatmap.png crHetDensityScatter.png crHistogram.png hetHistogram.png/;
    my %captions = (
	"failsIndividual.png" => "Individual causes of sample failure",
	"failsCombined.png" => "Combined causes of sample failure",
	"failScatterPlot.png" => "Scatterplot of failed samples",
	"failScatterDetail.png" => "Scatterplot of failed samples passing CR/Het thresholds",
	"sample_xhet_gender.png" => "Gender model",
	"cr_boxplot.png" => "Call rate (CR) boxplot",
	"het_boxplot.png" => "Heterozygosity (het) boxplot",
	"xydiff_boxplot.png" => "XY intensity difference boxplot",
	"crHetDensityHeatmap.png" => "Heatmap of sample density by CR/Het",
	"crHetDensityScatter.png" => "Scatterplot of samples by CR/Het",
	"crHistogram.png" => "Histogram of CR",
	"hetHistogram.png" => "Histogram of heterozygosity",
	);
    my (@text, @missing); # gender plot is missing if mixture model fails sanity checks
    foreach my $plot (@plots) {
	if (-r $graphicsDir."/".$plot) {
	    push(@text, latexPlot($plot, $captions{$plot}));
	} else {
	    carp "Cannot read plot ".$graphicsDir."/".$plot;
	    push(@missing, $plot);
	    next;
	}
    }
    if (@missing>0) {
	push(@text, "\n\\paragraph*{Missing plots:}");
	push(@text, "\\begin{itemize}");
	foreach my $plot (@missing) { 
	    my $item = $plot;
	    $item =~ s/_/\\_/g;
	    $item = "\\item ".$item;
	    if ($captions{$plot}) { $item .= ": ".$captions{$plot}; }
	    push @text, $item;
	}
	push(@text, "\\end{itemize}\n");
    }
    return join("\n", @text)."\n";
}

sub latexFooter {
    my $footer = "\n\\end{document}\n";
    return $footer;
}

sub latexHeader {
    my ($title, $author, $graphicsDir) = @_;
    my $date = strftime("%Y-%m-%d %H:%M", localtime(time()));
    # TODO graphicx not needed if we convert all plots to PDF
    my $header = '\documentclass{article} 
\title{'.$title.'}
\author{'.$author.'}
\date{'.$date.'}

\usepackage{graphicx}
\graphicspath{{'.$graphicsDir.'}}

\usepackage{pdfpages}

\renewcommand{\familydefault}{\sfdefault} % sans serif font

\begin{document}

\maketitle
';
    return $header;
}

sub latexPlot {
    # .tex for a single plot entry
    # TODO check existence of plot file?
    my ($plot, $caption, $label, $height) = @_;
    $height ||= 400;
    my $defaultLabel = $plot;
    $defaultLabel =~ s/_//g;
    $caption ||= $defaultLabel;
    $label ||= $defaultLabel; 
    my $text = '
\begin{figure}[!h]
\includegraphics[height='.$height.'px]{'.$plot.'}';
    if ($caption) { $text.="\n\\caption{".$caption."}"; }
    if ($label) { $text.="\n\\label{".$label."}"; }
    $text .= "\n\\end{figure}\n";
    return $text;
}


sub latexSectionInput {
    # .tex for "Inputs" section
    my ($qcName, $dbPath) = @_;
    my @lines = ();
    push @lines, "\\section{Input data}\n\n";
    my @text = textForDatasets($dbPath, $qcName);
	foreach my $table (latexTables(\@text)) { push(@lines, $table."\n"); }
    return join("", @lines);
}

sub latexSectionMetrics {
    my $config = shift;
    my $gtPath = shift; # gender thresholds
    my ($mMax, $fMin) = readGenderThreholds($gtPath);
    my @lines = ();
    push @lines, "\\pagebreak\n\n";
    push @lines, "\\subsection{Summary of metrics and thresholds}";
    push @lines, "\\label{sec:metric-summary}\n\n";
    my @text = textForMetrics($config, $mMax, $fMin);
	foreach my $table (latexTables(\@text)) { push @lines, $table."\n"; }
    return join("", @lines);
    
}

sub latexSectionResults {
    my ($config, $qcDir, $plotDescPath, $resultPath) = @_;
    my @lines = ();
    push @lines, "\\section{Results}\n\n";
    push @lines, "\\subsection{Tables}\n\n";
    my @titles = ("Key to metric abbreviations", 
                  "Total samples passing filters",
                  "Sample pass rates");
    my @refs = textForPlates($resultPath, $config);
    for (my $i=0;$i<@refs;$i++) {
        push @lines, "\\subsubsection*{".$titles[$i]."}\n";
        my @text = @{$refs[$i]};
        foreach my $table (latexTables(\@text)) { push @lines, $table."\n"; }
        push(@lines, "\n\n");
    }
    push @lines, "\\subsection{Plots}\n";
    push @lines, "\\subsubsection*{Metric scatterplots}\n";
    push @lines, "Scatterplots are produced for each metric. For analyses with a large number of plates, the results for a given metric may be split across more than one plot.  Note that the identity metric plots may be absent if Sequenom results are not available.\n\n";
    push @lines, "\\subsubsection*{Other}\n";
    push @lines, "The following additional plots are included:\n";
    push @lines, "\\begin{itemize}\n";
    push @lines, "\\item Causes of sample failure: Individual and combined\n";
    push @lines, "\\item Scatterplots of call rate versus heterozygosity: All samples, failed samples, and failed samples passing call rate and heterozygosity filters\n";
    push @lines, "\\end{itemize}\n";
    #push @lines, readFileToString($plotDescPath); # TODO more details in file
    my @metrics = readQCNameArray($config);
    my @includeLines = ();
    foreach my $metric (@metrics) {
        my @plotPaths = sort(glob($qcDir."/scatter_".$metric."*.pdf"));
        foreach my $plotPath (@plotPaths) {
            push(@includeLines, "\\includepdf[landscape=true]{".
                 $plotPath."}\n");
        }
    }
    my @morePdf =  qw(failsIndividual.pdf failsCombined.pdf 
  crHetDensityScatter.pdf failScatterPlot.pdf failScatterDetail.pdf);
    foreach my $name (@morePdf) {
        my $plotPath = $qcDir."/".$name;
        push(@includeLines, "\\includepdf[pages={1}]{".$plotPath."}\n");
    }
    push @lines, @includeLines;
    return join("", @lines);
}

sub latexTables {
	# convert array of arrays into one or more strings containing LaTeX tables
	# enforce maximum number of rows per table, before starting a new table (allows breaking across pages)
	# assume that first row is header; repeat header at start of each table
    my ($rowsRef, $caption, $label, $maxRows) = @_;
	$maxRows ||= 38;
	my @rows = @$rowsRef;
	my @tables = ();
	if (@rows > $maxRows) {
		my $headRef = shift(@rows);
		my @outRows;
		my $part = 1;
		while (@rows > $maxRows) {
			@outRows = ($headRef,);
			push(@outRows, splice(@rows, 0, $maxRows));
			my $newCaption;
			if ($caption) { $newCaption = $caption." (Part $part)"; }
			else { $newCaption = ""; }
			push(@tables, latexTableSingle(\@outRows, $newCaption, $label));
			$part++;
		}
		if (@rows>0) { # deal with remainder (if any)
			unshift(@rows, $headRef);
			my $newCaption;
			if ($caption) { $newCaption = $caption." (Part $part)"; }
			else { $newCaption = ""; }
			push(@tables, latexTableSingle(\@rows, $newCaption, $label));
		}
	} else {
		push(@tables, latexTableSingle(\@rows, $caption, $label));
	}
	return @tables;
}

sub latexTableSingle {
    # convert array of arrays into a (centred) table
    my ($rowsRef, $caption, $label) = @_;
    my @rows = @$rowsRef;
    my $cols = @{$rows[0]};
    my $table = "\n\\begin{table}[!h]\n\\centering\n\\begin{tabular}{|";
    foreach my $i (1..$cols) { $table.=" l |"; }
    $table.="} \\hline\n";
    my $first = 1;
    foreach my $ref (@rows) {
        my @row = @$ref;
        foreach my $item (@row) {
            $item =~ s/[_]/\\_/g;
            $item =~ s/%/\\%/g;
            if ($first) { $item = "\\textbf{".$item."}"; } # first row is header
        }
        $table.=join(" & ", @row)." \\\\ \\hline";
        if ($first) { $table .= ' \hline'; $first = 0; } 
        $table.="\n";
    }
    $table.="\\end{tabular}\n";
    if ($caption) { $table.="\\caption{".$caption."}\n"; }
    if ($label) { $table.="\\label{".$label."}\n"; }
    $table.="\\end{table}\n";
    return $table;
}

sub plateFieldsToText {
    # convert title and unformatted results to array of formatted text fields
    my $title = shift;
    my @stats = @{ shift() };
    my @out = ($title, );
    for (my $i=0;$i<@stats;$i++) {
	my $stat;
	if ($i>2) { $stat = sprintf("%.3f", $stats[$i]); }
	elsif ($i==2) { $stat = sprintf("%.1f", $stats[$i]); }
	else { $stat = $stats[$i]; }
	push(@out, $stat);
    }
    return @out;
}

sub readCsv {
    # read .csv file into array of arrays
    my $csvPath = shift;
    my @rows;
    open my $in, "<", $csvPath || croak "Cannot open $csvPath";
    while (<$in>) {
        chomp;
        my @fields = split(/,/);
        push(@rows, \@fields);
    }
    close $in || croak "Cannot close $csvPath";
    return @rows;

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
        if (/^M_max/) { $mMax = $thresh; }
        if (/^F_min/) { $fMin = $thresh; }
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

sub readFileToString {
    my $inPath = shift;
    open my $in, "<", $inPath || croak "Cannot open input path $inPath";
    my $string = join("", <$in>);
    close $in || croak "Cannot close input path $inPath";
    return $string;
}

sub sampleFieldsToText {
    # convert unformatted results to array of formatted text fields
    my @fields = @{ shift() };
    my @out;
    for (my $i=0;$i<@fields;$i++) {
	if ($fields[$i] =~ /^\d+$/ || $fields[$i] =~ /[a-zA-Z]+/) { push(@out, $fields[$i]); } # text or integers
	else { push(@out, sprintf('%.4f', $fields[$i])); }
    }
    return @out;
}

sub textForCsv {
    my ($resultPath, $dbPath, $config) = @_;
    my $resultsRef = readJson($resultPath);
    my @headers = getCsvHeaders($config);
    my @text = (\@headers,);
    my @sampleFields = getSampleInfo($resultsRef, $config);
    my %dbInfo = dbSampleInfo($dbPath);
    foreach my $ref (@sampleFields) {
	my @out = sampleFieldsToText($ref);
	my $sample = $out[0];
	unshift(@out, @{$dbInfo{$sample}});
	if ($#headers != $#out) { croak "Numbers of output headers and fields differ: $#headers != $#out"; }
	push(@text, \@out);
    }
    return @text;
}

sub textForDatasets {
    # text for datasets table
    my $dbPath = shift;
    my $qcDir = shift;
    my @headers;
    push @headers, @dbInfoHeaders;
    if ($qcDir) { push(@headers, "directory"); }
    my @text = (\@headers, );
    my @datasetInfo = dbDatasetInfo($dbPath);
    foreach my $ref (@datasetInfo) {
        my @items = @$ref;
        if ($qcDir) { push(@items, $qcDir); }
        push(@text, \@items);
    }
    return @text;
}

sub textForMetrics {
    my ($jsonPath, $mMax, $fMin) = @_;
    $mMax = sprintf("%.3f", $mMax);
    $fMin = sprintf("%.3f", $fMin);
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
    # find total number of samples passing filters for each metric
    # also find pass rates
    # return text for tables
    my %shortNames = %{ shift() };
    my %passCounts = %{ shift() };
    my %sampleCounts = %{ shift() };
    my @headers1 = qw/plate samples/;
    my @headers2 = qw/plate/;
    my @metricNames = ();
    foreach my $name (@METRIC_NAMES) { push(@metricNames, $name); }
    push(@metricNames, $allMetricsName);
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
    foreach my $plate (@plates) { # count of passed/failed samples by plate
        my @fields1 = ($plate, $sampleCounts{$plate});
        my @fields2 = ($plate, );
        $sampleTotal += $sampleCounts{$plate};
        foreach my $metric (@metricNames) {
            push(@fields1, $passCounts{$plate}{$metric});
            my $rate = $passCounts{$plate}{$metric} / $sampleCounts{$plate};
            push(@fields2,  sprintf("%.3f", $rate));
            $passTotals{$metric} += $passCounts{$plate}{$metric};
        }
        push(@text1, \@fields1);
        push(@text2, \@fields2);
    }
    my $name =  "\\textbf{".lc($allPlatesName)."}"; 
    $name =~ s/_/ /g;
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

sub textForPlates {
    # produce text for two tables: pass counts and pass rates
    my $resultPath = shift;
    my $config = shift;
    my $resultsRef = readJson($resultPath);
    my @refs = getPlateInfo($resultsRef, $config);
    my %passCounts = %{$refs[0]};
    my %sampleCounts = %{$refs[1]};
    my %shortNames = readQCShortNameHash($config);
    my @textKey = textForMetricKey(%shortNames);
    my ($textRef1, $textRef2) = textForPass(\%shortNames, \%passCounts, 
                                            \%sampleCounts);
    return (\@textKey, $textRef1, $textRef2);
}

sub texToPdf {
    my $texPath = shift;
    my $cleanup = shift;
    $cleanup ||= 1;
    $texPath = abs_path($texPath);
    my @terms = split('/', $texPath);
    my $file = pop @terms;
    my $texDir = join('/', @terms);
    my $startDir = getcwd();
    # run pdflatex twice; needed to get cross-references correct, eg. in list of figures
    chdir($texDir);
    system('pdflatex -draftmode '.$file.' > /dev/null'); # first pass; draft mode is faster
    my $result = system('pdflatex '.$file.' > /dev/null'); # second pass
    if ($cleanup) {
	my @rm = qw/*.aux *.dvi *.lof/; # remove intermediate LaTeX files; keeps .log, .tex, .pdf
	system('rm -f '.join(' ', @rm));
    }
    chdir($startDir);
    if ($result==0) { return 1; }
    else { return 0; }
}

sub writeCsv {
    my ($resultPath, $dbPath, $config, $outPath) = @_;
    $config ||= defaultJsonConfig();
    my @text = textForCsv($resultPath, $dbPath, $config);
    open my $out, ">", $outPath || croak "Cannot open output path $outPath";
    foreach my $lineRef (@text) {
	print $out join(',', @$lineRef)."\n";
    }
    close $out || croak  "Cannot close output path $outPath";
    if (@text > 0) { return 1; } # at least one line written without croak
    else { return 0; }
}

sub writeSummaryLatex {
    # write .tex input file for LaTeX; use to generate PDF
    my ($texPath, $resultPath, $config, $dbPath, $graphicsDir, $title, $author,
        $introPath, $qcName) = @_;
    $texPath ||= "pipeline_summary.tex";
    $title ||= "Genotyping QC Report";
    $author ||= "Wellcome Trust Sanger Institute\\\\\nIllumina Beadchip Genotyping Pipeline";
    $config ||= defaultJsonConfig();
    $graphicsDir ||= ".";
    $qcName ||= "Unknown";
    open my $out, ">", $texPath || croak "Cannot open output path $texPath";
    print $out latexHeader($title, $author, $graphicsDir);
    print $out "\\section{Input data}\n\n";
    print $out "\\paragraph*{Directory name:} $qcName\n";
    my @text = textForDatasets($dbPath);
	foreach my $table (latexTables(\@text)) { print $out $table."\n"; }
    print $out readFileToString($introPath); # new section = Introduction
    print $out "\\subsection{Summary of metrics and thresholds}";
    print $out "\\label{sec:metric-summary}\n\n";
    @text = textForMetrics($config);
	foreach my $table (latexTables(\@text)) { print $out $table."\n"; }
    #print $out "\\pagebreak\n";
    print $out "\\section{Results}\n\n";
    @text = textForPlates($resultPath, $config);
    print $out "\\subsection{Plates}\n\n";
	foreach my $table (latexTables(\@text)) { print $out $table."\n"; }
    # TODO "Sample number and release summary" goes here
    print $out "\\pagebreak\n";
    print $out "\\listoffigures\n\n";
    print $out latexAllPlots($graphicsDir);
    print $out latexFooter();
    close $out || croak "Cannot close output path $texPath";
    return 1;
}


sub writeSummaryLatexNew {
    # test of new format, will later replace old one
    my ($texPath, $resultPath, $config, $dbPath, $genderThresholdPath,
        $graphicsDir, $pdfDir, $title, $author, $introPath, $plotDescPath, 
        $qcName) = @_;
    $texPath ||= "pipeline_summary.tex";
    $title ||= "Genotyping QC Report";
    $author ||= "Wellcome Trust Sanger Institute\\\\\nIllumina Beadchip Genotyping Pipeline";
    $config ||= defaultJsonConfig();
    $graphicsDir ||= ".";
    $pdfDir ||= $graphicsDir;
    $qcName ||= "Unknown";
    open my $out, ">", $texPath || croak "Cannot open output path $texPath";
    print $out latexHeader($title, $author, $graphicsDir);
    print $out latexSectionInput($qcName, $dbPath);
    print $out readFileToString($introPath); # new section = Preface
    print $out latexSectionMetrics($config, $genderThresholdPath);
    print $out latexSectionResults($config, $pdfDir, $plotDescPath, 
                                   $resultPath);
    print $out latexFooter();
    close $out || croak "Cannot close output path $texPath";

}

1;
