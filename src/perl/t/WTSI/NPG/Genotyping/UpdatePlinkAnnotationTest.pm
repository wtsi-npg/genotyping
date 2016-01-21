
use utf8;

package WTSI::NPG::Genotyping::UpdatePlinkAnnotationTest;

use strict;
use warnings;
use File::Temp qw(tempdir tempfile);
use JSON;
use Log::Log4perl;

use base qw(WTSI::NPG::Test);
use Test::More tests => 9;
use Test::Exception;

use WTSI::NPG::Genotyping::Plink qw(update_placeholder 
                                    update_snp_locations
                                    update_sample_genders);

my $workdir;
my $data_path = "t/update_plink_annotation";

sub setup : Test(setup) {
    $workdir = tempdir("update_plink_annotation_test_XXXXXX", CLEANUP => 1);
}

sub teardown : Test(teardown) {
    # placeholder, does nothing for now
}

sub require : Test(1) {

    require_ok('WTSI::NPG::Genotyping::Plink');

}

sub test_update_fam_placeholder : Test(2) {

    my $total_updated = update_placeholder("$data_path/input_placeholder.bed", 
					   "$data_path/output_placeholder.bed",
					   -9, $workdir);
    is($total_updated, 5, 'Correct number of samples in placeholder update');
    # Confirm expected FAM file
    open(my $smobs, '<', "$data_path/output_placeholder.fam")
	or die "failed to open '$data_path/output_placeholder.fam'";
    my @samples_observed = <$smobs>;
    close($smobs);
    open(my $smexp, '<', "$data_path/expected_placeholder.fam")
	or die "Failed to open '$data_path/expected_placeholder.fam'";
    my @samples_expected = <$smexp>;
    close($smexp);
    
    is_deeply(\@samples_observed, \@samples_expected, 'Placeholder update outputs are identical');
    unlink("$data_path/output_placeholder.fam");

}

sub test_update_sample_genders : Test(3) {
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

    my ($smout, $test_samples) = tempfile();
    print $smout to_json(\@samples, {utf8 => 1, pretty => 1});
    close($smout);
    my $in_prefix = "input_gender";
    my $out_prefix = "output_gender";
    my $exp_prefix = "expected_gender";
    dies_ok {
	update_sample_genders("$data_path/$in_prefix.bed", 
			      "$data_path/$out_prefix.bed",
			      $test_samples, $workdir);
    } 'Expected update to fail on unknown sample';

    push(@samples, {name => 'sample_0002',
                uri => 'urn:wtsi:sample_0002',
                gender => 'Male',
                gender_code => 1,
                gender_method => 'Submitted'});
    ($smout, $test_samples) = tempfile();
    print $smout to_json(\@samples, {utf8 => 1, pretty => 1});
    close($smout);
    my $total_updated =
	update_sample_genders("$data_path/$in_prefix.bed", 
			      "$data_path/$out_prefix.bed",
			      $test_samples, $workdir);
    is ($total_updated, 5, 'Correct number of samples in gender update');
    # Confirm expected FAM file
    open(my $smobs, '<', "$data_path/$out_prefix.fam")
	or die "failed to open '$data_path/$out_prefix.fam'";
    my @samples_observed = <$smobs>;
    close($smobs);
    
    open(my $smexp, '<', "$data_path/$exp_prefix.fam")
	or die "Failed to open '$data_path/$exp_prefix.fam'";
    my @samples_expected = <$smexp>;
    close($smexp);

    is_deeply(\@samples_observed, \@samples_expected, 'Gender update outputs are identical');
    unlink("$data_path/$out_prefix.fam");
} 

sub test_update_snp_locations : Test(3) {
    my @snps = ({name => 'rs0001',
		 chromosome => 1,
		 position => 11},
		{name => 'rs0002',
		 chromosome => 1,
		 position => 101},
		{name => 'rs0004',
		 chromosome => 2,
		 position => 201});
    open my $temp, '>', $workdir.'/snps.json' || log->logcroak("Cannot open temporary SNP file");

    my ($snout, $test_snps) = tempfile();
    print $snout to_json(\@snps, {utf8 => 1, pretty => 1});
    $snout->flush;
    dies_ok {
	update_snp_locations("$data_path/input.bed", "$data_path/output.bed",
			     $test_snps, $workdir)
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
			     $test_snps, $workdir);
    is($num_snps_updated, 4, 'Correct number of SNPs updated');

    # Confirm expected BIM file
    open(my $snobs, '<', "$data_path/output.bim")
	or die "Failed to open '$data_path/output.bim'";
    my @snps_observed = <$snobs>;
    close($snobs);

    open(my $snexp, '<', "$data_path/expected.bim")
	or die "Failed to open '$data_path/expected.bim'";
    my @snps_expected = <$snexp>;
    close($snexp);

    is_deeply(\@snps_observed, \@snps_expected, 'SNP update outputs are identical');
    unlink("$data_path/output.bim");
}


return 1;
