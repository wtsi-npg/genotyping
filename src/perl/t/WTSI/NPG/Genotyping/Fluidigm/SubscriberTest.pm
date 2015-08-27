
use utf8;

package WTSI::NPG::Genotyping::Fluidigm::SubscriberTest;

use strict;
use warnings;
use DateTime;
use List::AllUtils qw(uniq);

use base qw(Test::Class);
use Test::More tests => 42;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Fluidigm::Subscriber') };

use WTSI::NPG::Genotyping::Fluidigm::Subscriber;
use WTSI::NPG::Genotyping::Fluidigm::AssayDataObject;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;

my $data_path = './t/fluidigm_subscriber';
my @assay_resultset_files = qw(S01_1381735059.csv S01_1381735060.csv
                               S02_1381735059.csv);
my @sample_identifiers = qw(ABC0123456789 ABC0123456789 XYZ0123456789);
my @sample_plates = qw(1381735059 1381735060 1381735059);
my @sample_wells = qw(S01 S01 S02);
my $non_unique_identifier = 'ABCDEFGHI';

my $reference_name = 'Homo_sapiens (1000Genomes)';
my $snpset_name = 'qc';
my $snpset_file = 'qc.tsv';

my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods_tmp_coll = "FluidigmSubscriberTest.$pid";
  $irods->add_collection($irods_tmp_coll);
  $irods->add_object("$data_path/$snpset_file", "$irods_tmp_coll/$snpset_file");

  my $snpset_obj = WTSI::NPG::iRODS::DataObject->new
    ($irods,"$irods_tmp_coll/$snpset_file")->absolute;
  $snpset_obj->add_avu('fluidigm_plex', $snpset_name);
  $snpset_obj->add_avu('reference_name', $reference_name);

  foreach my $i (0..2) {
    my $file = $assay_resultset_files[$i];
    $irods->add_object("$data_path/$file", "$irods_tmp_coll/$file");
    my $resultset_obj = WTSI::NPG::iRODS::DataObject->new
      ($irods,"$irods_tmp_coll/$file")->absolute;
    $resultset_obj->add_avu('fluidigm_plex', $snpset_name);
    $resultset_obj->add_avu('fluidigm_plate', $sample_plates[$i]);
    $resultset_obj->add_avu('fluidigm_well', $sample_wells[$i]);
    $resultset_obj->add_avu('dcterms:identifier', $sample_identifiers[$i]);
    $resultset_obj->add_avu('dcterms:identifier', $non_unique_identifier);
  }
}

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Fluidigm::Subscriber');
}

sub constructor : Test(1) {
  my $irods = WTSI::NPG::iRODS->new;

  new_ok('WTSI::NPG::Genotyping::Fluidigm::Subscriber',
         [irods          => $irods,
          data_path      => $irods_tmp_coll,
          reference_path => $irods_tmp_coll,
          reference_name => $reference_name,
          snpset_name    => $snpset_name]);
}

sub get_assay_resultsets_and_vcf_metadata : Test(6) {
  my $irods = WTSI::NPG::iRODS->new;
  my ($resultsets1, $meta1) = WTSI::NPG::Genotyping::Fluidigm::Subscriber->new
    (irods          => $irods,
     data_path      => $irods_tmp_coll,
     reference_path => $irods_tmp_coll,
     reference_name => $reference_name,
     snpset_name    => $snpset_name)->get_assay_resultsets_and_vcf_metadata
       ([uniq @sample_identifiers]);

  my $expected_meta = {
      'plex_type' => [ 'fluidigm' ],
      'plex_name' => [ 'qc' ]
  };
  is_deeply($meta1, $expected_meta, "VCF metadata matches expected values");

  cmp_ok(scalar keys %$resultsets1, '==', 2, 'Assay resultsets for 2 samples');
  cmp_ok(scalar @{$resultsets1->{ABC0123456789}}, '==', 2,
         '2 of 3 results for 1 sample');
  cmp_ok(scalar @{$resultsets1->{XYZ0123456789}}, '==', 1,
         '1 of 3 results for 1 sample');

  dies_ok {
    WTSI::NPG::Genotyping::Fluidigm::Subscriber->new
      (irods          => $irods,
       data_path      => $irods_tmp_coll,
       reference_path => $irods_tmp_coll,
       reference_name => $reference_name,
       snpset_name    => $snpset_name)->get_assay_resultsets_and_vcf_metadata
           ([$non_unique_identifier]);
  } 'Fails when query finds results for >1 sample';

  my ($resultsets2, $meta2) = WTSI::NPG::Genotyping::Fluidigm::Subscriber->new
     (irods          => $irods,
      data_path      => $irods_tmp_coll,
      reference_path => $irods_tmp_coll,
      reference_name => $reference_name,
      snpset_name    => $snpset_name)->get_assay_resultsets_and_vcf_metadata
          ([map { 'X' . $_ } 1 .. 100]);
  ok(defined $resultsets2, "'IN' query of 100 args");
}

sub get_assay_resultset : Test(2) {
  my $irods = WTSI::NPG::iRODS->new;
  my $resultset = WTSI::NPG::Genotyping::Fluidigm::Subscriber->new
    (irods          => $irods,
     data_path      => $irods_tmp_coll,
     reference_path => $irods_tmp_coll,
     reference_name => $reference_name,
     snpset_name    => $snpset_name)->get_assay_resultset('XYZ0123456789');

  ok($resultset, 'Assay resultsets');
  dies_ok {
    WTSI::NPG::Genotyping::Fluidigm::Subscriber->new
        (irods          => $irods,
         data_path      => $irods_tmp_coll,
         reference_path => $irods_tmp_coll,
         reference_name => $reference_name,
         snpset_name    => $snpset_name)->get_assay_resultset
           ($non_unique_identifier);
  } 'Fails on matching multiple results';
}

sub get_calls : Test(31) {
  my $irods = WTSI::NPG::iRODS->new;

  # check we can correctly read a single resultset
  my @calls_observed = _get_observed_calls($irods, $irods_tmp_coll,
                                           $reference_name, $snpset_name,
                                           'XYZ0123456789');
  is(26, scalar @calls_observed,
     "Correct number of calls in singular resultset");

  my @calls_expected = (['GS34251',    'TC'],
                        ['GS34251',    'TC'],
                        ['GS35220',    'CT'],
                        ['GS35220',    'CT'],
                        ['rs11096957', 'TG'],
                        ['rs12828016', 'GT'],
                        ['rs156697',   'AG'],
                        ['rs1801262',  'TC'],
                        ['rs1805034',  'CT'],
                        ['rs1805087',  'AG'],
                        ['rs2247870',  'GA'],
                        ['rs2286963',  'TG'],
                        ['rs3742207',  'TG'],
                        ['rs3795677',  'GA'],
                        ['rs4075254',  'GA'],
                        ['rs4619',     'AG'],
                        ['rs4843075',  'GA'],
                        ['rs5215',     'CT'],
                        ['rs6166',     'CT'],
                        ['rs649058',   'GA'],
                        ['rs6557634',  'TC'],
                        ['rs6759892',  'TG'],
                        ['rs7298565',  'GA'],
                        ['rs753381',   'TC'],
                        ['rs7627615',  'GA'],
                        ['rs8065080',  'TC']);

  is_deeply(\@calls_observed, \@calls_expected,
            "All calls match in singular resultset") or
              diag explain \@calls_observed;

  # get merged results for S01_1381735059.csv and S01_1381735060.csv
  # S01_1381735060.csv differs by a no call for rs8065080

  @calls_expected = (['GS34251',    'TC'],
                     ['GS34251',    'TC'],
                     ['GS35220',    'CT'],
                     ['GS35220',    'CT'],
                     ['rs11096957', 'TG'],
                     ['rs12828016', 'NN'], # 'No Call'
                     ['rs156697',   'NN'], # 'Invalid' call
                     ['rs1801262',  'TC'],
                     ['rs1805034',  'CT'],
                     ['rs1805087',  'AG'],
                     ['rs2247870',  'GA'],
                     ['rs2286963',  'TG'],
                     ['rs3742207',  'TG'],
                     ['rs3795677',  'GA'],
                     ['rs4075254',  'GA'],
                     ['rs4619',     'AG'],
                     ['rs4843075',  'GA'],
                     ['rs5215',     'CT'],
                     ['rs6166',     'CT'],
                     ['rs649058',   'GA'],
                     ['rs6557634',  'TC'],
                     ['rs6759892',  'TG'],
                     ['rs7298565',  'GA'],
                     ['rs753381',   'TC'],
                     ['rs7627615',  'GA'],
                     ['rs8065080',  'TC']);

  @calls_observed = _get_observed_calls($irods, $irods_tmp_coll,
                                        $reference_name, $snpset_name,
                                        'ABC0123456789');

  is(26, scalar @calls_observed,
     "Correct number of calls in merged resultset");

  is_deeply(\@calls_observed, \@calls_expected,
            "All calls match in merged resultset") or
              diag explain \@calls_observed;

  # add a third, non-matching call set to iRODS and repeat the get_calls
  my $file = 'S01_1381735061.csv';
  $irods->add_object("$data_path/$file", "$irods_tmp_coll/$file");
  my $resultset_obj = WTSI::NPG::iRODS::DataObject->new
      ($irods,"$irods_tmp_coll/$file")->absolute;
  $resultset_obj->add_avu('fluidigm_plex', 'qc');
  $resultset_obj->add_avu('fluidigm_plate', '1381735061');
  $resultset_obj->add_avu('fluidigm_well', 'S01');
  $resultset_obj->add_avu('dcterms:identifier', $sample_identifiers[0]);
  $resultset_obj->add_avu('dcterms:identifier', $non_unique_identifier);

  @calls_observed = _get_observed_calls($irods, $irods_tmp_coll,
                                        $reference_name, $snpset_name,
                                        'ABC0123456789');

  # Merging 3 un-mergable resultsets
  is (scalar @calls_expected, scalar @calls_observed,
      "Call lists of equal length");
  my $i = 0;
  my $unmatched_pos = 24;
  while ($i < scalar @calls_expected) {
      if ($i != $unmatched_pos) {
          is_deeply($calls_observed[$i], $calls_expected[$i],
                    "Calls match for snp at position $i");
      }
      $i++;
  }
  # calls differ at the 25th SNP, rs7627615
  isnt($calls_observed[$unmatched_pos][1], $calls_expected[$unmatched_pos][1],
       "Calls do not match for snp at position $unmatched_pos");
}

sub _get_observed_calls {
  # get (snp_name, genotype) pair observed for each call
  my ($irods, $irods_coll, $rname, $sname, $sample_id) = @_;

  my $subscriber = WTSI::NPG::Genotyping::Fluidigm::Subscriber->new
    (irods          => $irods,
     data_path      => $irods_coll,
     reference_path => $irods_coll,
     reference_name => $rname,
     snpset_name    => $sname);
  my ($assay_resultsets, $vcf_meta) =
      $subscriber->get_assay_resultsets_and_vcf_metadata([$sample_id]);
  my $calls = $subscriber->get_calls($assay_resultsets->{$sample_id});

  my @calls_observed;
  foreach my $call (@$calls) {
    push @calls_observed, [$call->snp->name, $call->genotype],
  }

  return @calls_observed;
}

1;
