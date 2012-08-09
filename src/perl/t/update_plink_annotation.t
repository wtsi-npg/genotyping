use utf8;

use strict;
use warnings;
use File::Temp qw(tempfile tempdir);
use JSON;

use Test::More tests => 6;
use Test::Exception;

use WTSI::Genotyping qw(update_snp_locations update_sample_genders);

my @samples = ({name => 'sample_0000',
                uri => 'urn:wtsi:sample_0000',
                gender => 'Male',
                gender_code => 1,
                gender_method => 'Submitted'},
               {name => 'sample_0001',
                uri => 'urn:wtsi:sample_0001',
                gender => 'Female',
                gender_code => 2,
                gender_method => 'Submitted'},
               {name => 'sample_0003',
                uri => 'urn:wtsi:sample_0003',
                gender => 'Male',
                gender_code => 1,
                gender_method => 'Submitted'},
               {name => 'sample_0004',
                uri => 'urn:wtsi:sample_0004',
                gender => 'Female',
                gender_code => 2,
                gender_method => 'Submitted'});

my @snps = ({name => 'rs0001',
             chromosome => 1,
             position => 11},
            {name => 'rs0002',
             chromosome => 1,
             position => 101},
            {name => 'rs0004',
             chromosome => 2,
             position => 201});

my $data_path = "t/update_plink_annotation";
my $tmp_dir = tempdir(CLEANUP => 1);


# Update SNP annotation
my $test_snps;
my $snout;

($snout, $test_snps) = tempfile();
print $snout to_json(\@snps, {utf8 => 1, pretty => 1});
$snout->flush;

dies_ok {
  update_snp_locations("$data_path/input.bed", "$data_path/output.bed",
                       $test_snps, $tmp_dir)
} 'Expected update to fail on unknown SNP';
close($snout);

push(@snps, {name => 'rs0003',
             chromosome => 2,
             position => 21}),

($snout, $test_snps) = tempfile();
print $snout to_json(\@snps, {utf8 => 1, pretty => 1});
close($snout);

my $num_snps_updated =
  update_snp_locations("$data_path/input.bed", "$data_path/output.bed",
                       $test_snps, $tmp_dir);
is($num_snps_updated, 4);

# Confirm expected BIM file
open(my $snobs, '<', "$data_path/output.bim")
  or die "Failed to open '$data_path/output.bim'";
my @snps_observed = <$snobs>;
close($snobs);

open(my $snexp, '<', "$data_path/expected.bim")
  or die "Failed to open '$data_path/expected.bim'";
my @snps_expected = <$snexp>;
close($snexp);

is_deeply(\@snps_observed, \@snps_expected);
unlink("$data_path/output.bim");


# Update sample annotation
my $test_samples;
my $smout;
($smout, $test_samples) = tempfile();
print $smout to_json(\@samples, {utf8 => 1, pretty => 1});
close($smout);

dies_ok {
  update_sample_genders("$data_path/input.bed", "$data_path/output.bed",
                        $test_samples, $tmp_dir);
} 'Expected update to fail on unknown sample';


push(@samples, {name => 'sample_0002',
                uri => 'urn:wtsi:sample_0002',
                gender => 'Male',
                gender_code => 1,
                gender_method => 'Submitted'});

($smout, $test_samples) = tempfile();
print $smout to_json(\@samples, {utf8 => 1, pretty => 1});
close($smout);

my $num_samples_updated =
  update_sample_genders("$data_path/input.bed", "$data_path/output.bed",
                        $test_samples, $tmp_dir);
is($num_samples_updated, 5);

# Confirm expected FAM file
open(my $smobs, '<', "$data_path/output.fam")
  or die "Failed to open '$data_path/output.fam'";
my @samples_observed = <$smobs>;
close($smobs);

open(my $smexp, '<', "$data_path/expected.fam")
  or die "Failed to open '$data_path/expected.fam'";
my @samples_expected = <$smexp>;
close($smexp);

is_deeply(\@samples_observed, \@samples_expected);
unlink("$data_path/output.fam");
