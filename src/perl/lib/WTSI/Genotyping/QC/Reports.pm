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
our @EXPORT_OK = qw/createReports/;
our @dbInfoHeaders = qw/run project supplier snpset/;

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
    my @metricNames = readQCNameArray($config);
    foreach my $metric (@metricNames) {
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

sub getPlateInfo {
    # for each plate, find: samples found, samples excluded, CR mean/median, het mean
    my %records = %{ shift() };
    my $config = shift;
    my @metricNames = readQCNameArray($config);
    my (%crByPlate, %hetByPlate, %passByPlate, %samplesByPlate, %plateStats, @cr, @het, $pass, $samples);
    foreach my $sample (keys(%records)) {
	if (not $records{$sample}) { croak "No QC results found for sample $sample!"; }
	my %record = %{$records{$sample}};
	my $plate = $record{'plate'};
	my $samplePass = 1;
	foreach my $metric (@metricNames) {
	    my @values;
	    my ($pass, $val);
	    if ($record{$metric}) {
		($pass, $val) = @{$record{$metric}};
	    } else {
		($pass, $val) = (1, "NA"); # placeholders if metric not in use
	    }
	    if ($pass==0) { $samplePass = 0; }
	    if ($metric eq 'call_rate') { 
		if ($crByPlate{$plate}) { push @{$crByPlate{$plate}}, $val; }
		else { $crByPlate{$plate} = [$val]; }
		push @cr, $val;
	    } elsif ($metric eq 'heterozygosity') {  
		if ($hetByPlate{$plate}) { push @{$hetByPlate{$plate}}, $val; }
		else { $hetByPlate{$plate} = [$val]; }
		push @het, $val;
	    }
	}
	$passByPlate{$plate} += $samplePass;
	$pass += $samplePass;
	$samplesByPlate{$plate}++;
	$samples++;
    }
    # stats for each plate
    foreach my $plate (keys(%samplesByPlate)) {
	my @fields = findPlateFields($samplesByPlate{$plate}, $passByPlate{$plate}, $crByPlate{$plate},
				     $hetByPlate{$plate});
	$plateStats{$plate} = [@fields];
    }
    my @aggregate = findPlateFields($samples, $pass, \@cr, \@het);  # aggregate stats for all plates
    return (\%plateStats, \@aggregate);
}

sub getSampleInfo {
    # for each sample, get plate, well, metric values and pass/fail status
    # also get overall sample pass/fail
    my %records = %{ shift() };
    my $config = shift;
    my @metricNames = readQCNameArray($config);
    # metric name order: ["call_rate","heterozygosity","duplicate","identity","gender","xydiff]"
    my @sampleFields;
    my @samples = keys(%records);
    @samples = sort @samples;
    foreach my $sample (@samples) {
	if (not $records{$sample}) { croak "No QC results found for sample $sample!"; }
	my %record = %{$records{$sample}};
	my $samplePass = 1;
	my @fields = ($sample, $record{'plate'}, $record{'address'}, $samplePass); # $samplePass is placeholder
	foreach my $metric (@metricNames) {
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
    my $header = '\documentclass{article} 
\title{'.$title.'}
\author{'.$author.'}
\date{'.$date.'}

\usepackage{graphicx}
\graphicspath{{'.$graphicsDir.'}}

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
\begin{figure}[p]
\includegraphics[height='.$height.'px]{'.$plot.'}';
    if ($caption) { $text.="\n\\caption{".$caption."}"; }
    if ($label) { $text.="\n\\label{".$label."}"; }
    $text .= "\n\\end{figure}\n";
    return $text;
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
    my $table = "\n\\begin{table}[ht]\n\\centering\n\\begin{tabular}{|";
    foreach my $i (1..$cols) { $table.=" l |"; }
    $table.="} \\hline\n";
    my $first = 1;
    foreach my $ref (@rows) {
	my @row = @$ref;
	foreach my $item (@row) {
	    $item =~ s/[_]/\\_/g;
	    $item =~ s/%/\\%/g;
	    if ($first) { $item = "\\textbf{".$item."}"; }
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

sub textForDatasets {
    # text for datasets table
    my $dbPath = shift;
    my @text = (\@dbInfoHeaders, );
    my @datasetInfo = dbDatasetInfo($dbPath);
    foreach my $ref (@datasetInfo) {
	push(@text, $ref);
    }
    return @text;
}

sub textForMetrics {
    my $jsonPath = shift;
    my %doc = %{readJson($jsonPath)};
    my %thresh = %{$doc{'Metrics_thresholds'}};
    my @headers = qw/metric threshold type description/;
    my %descs = %{$doc{'Metric_descriptions'}};
    my %types = %{$doc{'Threshold_types'}};
    my @text = (\@headers,);
    my @names;
    @names = qw(duplicate identity gender call_rate heterozygosity magnitude);
    foreach my $name (@names) {
        push(@text, [$name, $thresh{$name}, $types{$name}, $descs{$name}]);
    }
    return @text;
}

sub textForPlates {
    my $resultPath = shift;
    my $config = shift;
    my $resultsRef = readJson($resultPath);
    my ($plateRef, $aggRef) = getPlateInfo($resultsRef, $config);
    my %plateStats = %$plateRef;
    my @aggregate = @$aggRef;
    my @plates = keys(%plateStats);
    @plates = sort @plates;
    my @headers = qw/plate samples excluded excl% cr_mean cr_median het_mean/;
    my @text = (\@headers, );
    foreach my $plate (@plates) {
	my @out = plateFieldsToText($plate, $plateStats{$plate});
	push(@text, \@out);
    }
    my @out = plateFieldsToText("ALL_PLATES", \@aggregate);
    push(@text, \@out);
    return @text;
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
    print $out "\\pagebreak\n";
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

1;
