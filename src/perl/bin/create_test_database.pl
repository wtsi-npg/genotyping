#! /software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# November 2012

# create a pipeline database populated with dummy values for testing

use warnings;
use strict;
use Carp;
use File::Slurp qw/read_file/;
use Getopt::Long;
use Text::CSV;
use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::Database::Pipeline;
use WTSI::NPG::Genotyping::QC::QCPlotShared qw/defaultConfigDir/;
use WTSI::NPG::Genotyping::SNP;

our $VERSION = '';

my ($sampleGenderPath, $dbPath, $plexManifest, $iniPath, $plateSize,
    $plateTotal, $qcPlexMethod, $qcPlexName, $excl, $help);

GetOptions("sample-gender=s"   => \$sampleGenderPath,
           "qc-plex=s"     => \$plexManifest,
           "dbpath=s"      => \$dbPath,
           "inipath=s"     => \$iniPath,
           "plate-size=i"  => \$plateSize,
           "plate-total=i" => \$plateTotal,
           "plex-method=s" => \$$qcPlexMethod,
           "plex-name=s"   => \$$qcPlexName,
           "excl=f"        => \$excl,
           "help"          => \$help,
    );

if ($help) {
    print STDERR "Usage: $0 [ options ]
Options:
--sample-gender     Path to sample_xhet_gender.txt file, to input sample
                    names and inferred genders
--qc-plex           Path to a tab-separated manifest file of QC plex SNPs.
--dbpath            Path to pipeline.db file output
--inipath           Path to .ini file for pipeline database
--plate-size        Number of samples on each plate (maximum 384)
--plate-total       Total number of plates.  If (plate size)*(plate total)
                    is less than number of samples, excess samples will have
                    no plate information.
--qc-method         QC plex method, eg. Sequenom or Fluidigm. Must be defined
                    in methods.ini file for pipeline DB.
--qc-name           Name of QC plex in pipeline DB. Must be defined in
                    snpsets.ini file for pipeline DB.
--excl              Probability of sample being arbitrarily excluded
--help              Print this help text and exit
Unspecified options will receive default values.
";
    exit(0);
}

$sampleGenderPath ||= "./sample_xhet_gender.txt";
$dbPath ||= "./test_genotyping.db";
$iniPath ||= $ENV{HOME} . "/.npg/genotyping.ini";
$plateSize ||= 96;
$plateTotal ||= 20;
unless (defined($excl)) { $excl = 0.05; } # may have excl==0

$plexManifest ||= '/nfs/srpipe_references/genotypes/W30467_snp_set_info_1000Genomes.tsv';
$qcPlexMethod ||= 'Sequenom';
$qcPlexName ||= 'W30467';


my $etcDir = defaultConfigDir($iniPath);
if (! (-e $etcDir)) { 
    croak "Config directory \"$etcDir\" does not exist!";
} elsif ($plateSize > 384) { 
    croak "Maximum plate size = 384\n"; 
} elsif (! (-e $sampleGenderPath)) { 
    croak "Input \"$sampleGenderPath\" does not exist!";
}
if (-e $dbPath) { system("rm -f $dbPath"); }

sub addQcCalls {
    # add dummy QC calls to a given sample in the pipeline database
    my ($db, $sample, $plexManifest, $method) = @_;
    $method ||= $db->method->find({name => 'Fluidigm'});
    my $snpset = $db->snpset->find({name => 'qc'});
    my $result = $sample->add_to_results({method => $method});
    my $calls = createDummyCalls($plexManifest);
    foreach my $call (@{$calls}) {
        my $snp = $db->snp->find_or_create
            ({name       => $call->snp->name,
              chromosome => $call->snp->chromosome,
              position   => $call->snp->position,
              snpset     => $snpset});
        $result->add_to_snp_results({snp   => $snp,
                                     value => $call->genotype});
    }
}

sub createDummyCalls {

    my ($plexPath,) = @_;
    my $csv = Text::CSV->new({eol              => "\n",
                              sep_char         => "\t",
                              binary           => 1,
                              allow_whitespace => undef,
                              quote_char       => undef});
    my @calls;
    my @lines = read_file($plexPath);
    foreach my $line (@lines) {
        if ($line =~ m{^#SNP_NAME}msx) { next; }
        $csv->parse($line);
        my @fields = $csv->fields();
        my ($snp_name, $ref, $alt, $chrom, $pos, $strand) = @fields;
        my $snp = WTSI::NPG::Genotyping::SNP->new
            (name       => $snp_name,
             ref_allele => $ref,
             alt_allele => $alt,
             chromosome => $chrom,
             position   => $pos,
             strand     => $strand);
        # for now, all dummy calls are reference homozygote
        my $call = WTSI::NPG::Genotyping::Call->new(snp => $snp,
                                                    genotype => $ref.$ref);
        push @calls, $call;
    }
    return \@calls;
}

sub readSampleNamesGenders {
    # from sample_xhet_gender.txt
    my $inPath = shift;
    open (my $in, "< $inPath");
    my (@samples, @inferred, @supplied);
    my $first = 1;
    while (<$in>) {
        if ($first) { $first = 0; next; } # skip header
        my @words = split;
        push(@samples, $words[0]);
        push(@inferred, $words[2]);
        push(@supplied, $words[3]);
    }
    close $in;
    return (\@samples, \@inferred, \@supplied);
}

sub addSampleGender {
    my ($db, $sample, $genderCode, $inferred) = @_;
    # $inferred: Gender inferred if true, supplied otherwise
    my $method;
    if ($inferred) {
        $method = $db->method->find({name => 'Inferred'});
    } else {
        $method = $db->method->find({name => 'Supplied'});
    }
    my $gender;
    if ($genderCode==1) { 
        $gender = $db->gender->find({name => 'Male'});
    } elsif ($genderCode==2) { 
        $gender = $db->gender->find({name => 'Female'});
    } elsif ($genderCode==0) { 
        $gender = $db->gender->find({name => 'Unknown'});
    } else { 
        $gender = $db->gender->find({name => 'Not available'});
    }
    $sample->add_to_genders($gender, {method => $method});
}

Log::Log4perl->init("etc/log4perl.conf");

my ($namesRef, $gRefInferred, $gRefSupplied)
    = readSampleNamesGenders($sampleGenderPath);
my @sampleNames = @$namesRef;
my @gInferred = @$gRefInferred;
my @gSupplied = @$gRefSupplied;
print "Read ".@sampleNames." samples from file.\n";

## create & initialize database object
my $db = WTSI::NPG::Genotyping::Database::Pipeline->new
    (name => 'pipeline',
     inifile => "$etcDir/pipeline.ini",
     dbfile => $dbPath);
print "Database object initialized.\n";
my $schema;
$schema = $db->connect(RaiseError => 1,
                       on_connect_do => 'PRAGMA foreign_keys = ON')->schema;
$db->populate;
my $run = $db->piperun->find_or_create({name => 'pipeline_run',
                                        start_time => time()});
## get values for (supplier, snpset, dataset) objects
my $supplier = $db->datasupplier->find_or_create({name => $ENV{'USER'},
                                                  namespace => 'wtsi'});
my $snpset = $db->snpset->find({name => 'HumanOmni25-8v1'});
my $dataset = $run->add_to_datasets({if_project => "test_project",
                                     datasupplier => $supplier,
                                     snpset => $snpset});

## create some dummy plates
my @plates;
for (my $i=0;$i<$plateTotal;$i++) {
    my $ssbc = "ssbc".sprintf("%05d",$i);
    my $ifbc = "ifbc".sprintf("%05d",$i);
    my $plate = $db->plate->find_or_create( {ss_barcode => $ssbc,
                                             if_barcode => $ifbc} );
    push @plates, $plate;
}
print "Plates added.\n";

## set sample genders & wells; arbitrarily exclude some samples
my $flipTotal = 0;
my $exclTotal = 0;
my $wells = $plateSize * $plateTotal;
$db->in_transaction(sub {
    foreach my $i (0..@sampleNames-1) {
        my $include = 1;
        if (rand() <= $excl) { $include = 0; $exclTotal++; }
        # 'sample names' are actually URI's; strip off prefix
        my $uri = $sampleNames[$i];
        my @terms = split(':', $sampleNames[$i]);
        my $name = pop(@terms);
        my $sample = $dataset->add_to_samples
            ({name => $name,
              sanger_sample_id => $uri,
              supplier_name => 'supplier_WeylandYutani'.sprintf("%05d",$i),
              rowcol => 'rowcol_number'.sprintf("%03d", $i % $plateSize),
              beadchip => 'beadchip_ABC123456',
              cohort => 'xenomorph_cohort',
              include => $include});
        addSampleGender($db, $sample, $gInferred[$i], 1);
        addSampleGender($db, $sample, $gSupplied[$i], 0);
        addQcCalls($db, $sample, $plexManifest);
        if ($i >= $wells) { next; }
        my $plateNum = int($i / $plateSize);
        my $plate = $plates[$plateNum];
        my $addressNum = ($i % $plateSize)+1;
        my $address = $db->address->find({ id_address => $addressNum});
        my $well = $db->well->find_or_create( {
            id_address => $address->id_address,
            id_plate => $plate->id_plate,
            id_sample => $sample->id,
                                              } );
    }
                    });
print "$flipTotal sample genders flipped.\n";
print "$exclTotal samples excluded.\n";
$db->disconnect();
undef $db;

### now try to read data back in from database file
$db = WTSI::NPG::Genotyping::Database::Pipeline->new
    (name => 'pipeline',
     inifile => "$etcDir/pipeline.ini",
     dbfile => $dbPath);
$schema = $db->connect(RaiseError => 1,
		       on_connect_do => 'PRAGMA foreign_keys = ON')->schema;
$db->populate;
my @samples = $db->sample->all;
print "Read ".@samples." samples from $dbPath\n";
my $mismatch = 0;
my $i = 0;
foreach my $sample (@samples) {
    my $sample_name = "urn:wtsi:".$sample->name; # convert to uri
    my $gender = $db->gender->find
        ({'sample.id_sample' => $sample->id_sample,
          'method.name' => 'Supplied'},
         {join => {'sample_genders' => ['method', 'sample']}},
         {prefetch =>  {'sample_genders' => ['method', 'sample']} });
    my $well = ($sample->wells->all)[0]; # assume one well per sample
    my $label = 'Unknown_address';
    my $plateName = 'Unknown_plate';
    if (defined($well)) { 
        my $address = $well->address;
        $label = $address->label1;
        my $plate = $well->plate;
        $plateName = $plate->ss_barcode;
    }
    if ($i % 100 == 0) {
        print $sample_name." ".$gender->code." ".
            $label." ".$plateName."\n";
    }
    $i++;
}

$db->disconnect();


__END__

=head1 NAME

create_test_database

=head1 DESCRIPTION

Create a test SQLite database for the genotyping pipeline

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>, Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2012, 2013, 2014, 2015, 2016, 2017 Genome Research Limited.
All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
