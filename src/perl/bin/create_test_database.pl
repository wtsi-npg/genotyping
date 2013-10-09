#! /software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# November 2012

# create a pipeline database populated with dummy values for testing

use warnings;
use strict;
use Carp;
use Getopt::Long;
use WTSI::NPG::Genotyping::Database::Pipeline;
use WTSI::NPG::Genotyping::QC::QCPlotShared qw/defaultConfigDir/;

my ($sampleGenderPath, $dbPath, $iniPath, $plateSize, $plateTotal, 
    $flip, $excl, $help);

GetOptions("sample-gender=s"   => \$sampleGenderPath,
           "dbpath=s"      => \$dbPath,
           "inipath=s"     => \$iniPath,
           "plate-size=i"  => \$plateSize,
           "plate-total=i" => \$plateTotal,
           "excl=f"        => \$excl,
           "help"          => \$help,
    );

if ($help) {
    print STDERR "Usage: $0 [ options ] 
Options:
--sample-gender     Path to sample_xhet_gender.txt file, to input sample names and inferred genders
--dbpath            Path to pipeline.db file output
--inipath           Path to .ini file for pipeline database
--plate-size        Number of samples on each plate (maximum 384)
--plate-total       Total number of plates.  If (plate size)*(plate total) is less than number of samples, excess samples will have no plate information.
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

my $etcDir = defaultConfigDir($iniPath);
if (! (-e $etcDir)) { 
    croak "Config directory \"$etcDir\" does not exist!";
} elsif ($plateSize > 384) { 
    croak "Maximum plate size = 384\n"; 
} elsif (! (-e $sampleGenderPath)) { 
    croak "Input \"$sampleGenderPath\" does not exist!"; 
}
if (-e $dbPath) { system("rm -f $dbPath"); }

sub getSampleNamesGenders {
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
    = getSampleNamesGenders($sampleGenderPath);
my @sampleNames = @$namesRef;
my @gInferred = @$gRefInferred;
my @gSupplied = @$gRefSupplied;
print "Read ".@sampleNames." samples from file.\n";

## create & initialize database object
my $db = WTSI::Genotyping::Database::Pipeline->new
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
my $flipTotal = 0;my $exclTotal = 0;
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
              include => $include});
        addSampleGender($db, $sample, $gInferred[$i], 1);
        addSampleGender($db, $sample, $gSupplied[$i], 0);
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
$db = WTSI::Genotyping::Database::Pipeline->new
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


