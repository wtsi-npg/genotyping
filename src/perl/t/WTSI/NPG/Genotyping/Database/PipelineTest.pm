
use utf8;

package WTSI::NPG::Genotyping::Database::PipelineTest;

use strict;
use warnings;

use base qw(Test::Class);
use Test::More tests => 69;
use Test::Exception;

use Log::Log4perl;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Database::Pipeline'); }

use WTSI::NPG::Genotyping::Database::Pipeline;

my $ini_path = './etc';
my $dbfile = 't/pipeline.' . $$ . '.db';
my $db;

my $project_base = 'test_project';
my $sample_base = 'test_sample';

sub make_fixture : Test(setup) {
   $db = WTSI::NPG::Genotyping::Database::Pipeline->new
     (name    => 'pipeline',
      inifile => "$ini_path/pipeline.ini",
      dbfile  => $dbfile);

   $db->connect(RaiseError    => 1,
                on_connect_do => 'PRAGMA foreign_keys = ON');
   $db->populate;
}

sub teardown : Test(teardown) {
  undef $db;

  if (-e $dbfile) {
    unlink($dbfile);
  }
}

sub require : Test(3) {
  require_ok('WTSI::NPG::Genotyping::Database::Pipeline');
}

sub autoload : Test(4) {
  my $tmpdb = WTSI::NPG::Genotyping::Database::Pipeline->new
    (name    => 'pipeline',
     inifile => "$ini_path/pipeline.ini",
     dbfile  => $dbfile);

  dies_ok { $tmpdb->should_not_autoload_this_method->all }
    'AUTOLOAD fails on an invalid method';

  dies_ok { $tmpdb->snpset->all }
    'AUTOLOAD fails when disconnected';

  ok($tmpdb->connect, 'Can connect');
  ok($tmpdb->snpset->all, 'AUTOLOAD succeeds when connected');
}

sub connect : Test(7) {
  my $tmpdb = WTSI::NPG::Genotyping::Database::Pipeline->new
    (name    => 'pipeline',
     inifile => "$ini_path/pipeline.ini",
     dbfile  => $dbfile);

  is($tmpdb->name, 'pipeline', 'Has correct name');
  ok($tmpdb->data_source, 'Has a data_source');
  ok($tmpdb->username, 'Has a username');

  ok(!$tmpdb->is_connected, 'Initially, is not connected');
  ok($tmpdb->connect, 'Can connect');
  ok($tmpdb->is_connected, 'Is connected');
  ok($tmpdb->dbh, 'Has a database handle');
}

sub disconnect : Test(4) {
  ok($db->is_connected, 'Is connected');
  ok($db->disconnect, 'Can disconnect');
  ok(!$db->is_connected, 'Finally, is not connected');
}

sub populate : Test(7) {
  my $tmpdb = WTSI::NPG::Genotyping::Database::Pipeline->new
    (name    => 'pipeline',
     inifile => "$ini_path/pipeline.ini",
     dbfile  => $dbfile);

  ok($tmpdb->connect, 'Can connect');
  ok($tmpdb->is_connected, 'Is connected');
  ok($tmpdb->populate);
  ok($tmpdb->snpset->count > 0, 'The snpset dictionary');
  ok($tmpdb->method->count > 0, 'The method dictionary');
  ok($tmpdb->relation->count > 0, 'The relation dictionary');
  ok($tmpdb->state->count > 0, 'The state dictionary',);
}

sub state : Test(2) {
  ok($db->state->find({name => 'autocall_pass'}, 'Pass state found'));
  ok($db->state->find({name => 'gtc_unavailable'}, 'Fail state found'));
}

sub snpset : Test(1) {
  my $snpset = $db->snpset->find({name => 'HumanOmni25-8v1'});
  ok($snpset, 'A snpset found');
}

sub add_supplier : Test(1) {
  my $supplier = $db->datasupplier->find_or_create({name      => $ENV{'USER'},
                                                    namespace => 'wtsi'});
  ok($supplier, 'A supplier inserted');
}

sub add_run : Test(2) {
  my $run = $db->piperun->find_or_create({name       => 'test',
                                          start_time => time()});
  ok($run, 'A run inserted');

  my $snpset = $db->snpset->find({name => 'HumanOmni25-8v1'});
  ok($run->validate_snpset($snpset), 'A snpset validated in an empty piperun');
}

sub add_datasets : Test(6) {
  my $supplier = $db->datasupplier->find_or_create({name      => $ENV{'USER'},
                                                    namespace => 'wtsi'});
  my $snpset = $db->snpset->find({name => 'HumanOmni25-8v1'});
  my $run = $db->piperun->find_or_create({name       => 'test',
                                          start_time => time()});

  foreach my $i (1..3) {
    my $dataset = $run->add_to_datasets
      ({if_project   => sprintf("%s_%d", $project_base, $i),
        datasupplier => $supplier,
        snpset       => $snpset});

    ok($dataset, "dataset $i inserted");
  }

  ok($run->validate_datasets, 'A piperun validated');
  ok($run->validate_snpset($snpset), 'A snpset validated in a full piperun');
  ok(!$run->validate_snpset($db->snpset->find({name => 'HumanOmni25-4v1'})),
     'A snpset validated in a mismatched piperun');
}

sub transaction : Test(8) {
  my $supplier = $db->datasupplier->find_or_create({name      => $ENV{'USER'},
                                                    namespace => 'wtsi'});
  my $snpset = $db->snpset->find({name => 'HumanOmni25-8v1'});
  my $run = $db->piperun->find_or_create({name       => 'transaction_test',
                                          start_time => time()});

  my @datasets;
  foreach my $i (1..3) {
    my $dataset = $run->add_to_datasets
      ({if_project   => sprintf("%s_%d", $project_base, $i),
        datasupplier => $supplier,
        snpset       => $snpset});
    push @datasets, $dataset;
  }

  my $autocall = $db->method->find({name => 'Autocall'});
  my $infinium = $db->method->find({name => 'Infinium'});
  my $pass = $db->state->find({name => 'autocall_pass'});

  my $gtc_path = '\\netapp1a\illumina_geno1\0123456789\0123456789.gtc';
  my $red_path = '\\netapp1a\illumina_geno1\0123456789\0123456789_red.idat';
  my $grn_path = '\\netapp1a\illumina_geno1\0123456789\0123456789_grn.idat';

  $db->in_transaction(sub {
                        foreach my $i (1..1000) {
                          my $sample = $datasets[0]->add_to_samples
                            ({name     => sprintf("%s_%d", $sample_base, $i),
                              beadchip => 'ABC123456',
                              include  => 1});
                          $sample->add_to_states($pass);

                          $sample->add_to_results({method => $autocall,
                                                   value  => $gtc_path});
                          $sample->add_to_results({method => $infinium,
                                                   value  => $red_path});
                          $sample->add_to_results({method => $infinium,
                                                   value  => $grn_path});
                        }
                      });

  my @samples = $datasets[0]->samples;
  cmp_ok(scalar @samples, '==', 1000, 'Expected samples found');

  my @states = $samples[0]->states;
  is(scalar @states, 1);
  is($states[0]->name, 'autocall_pass');

  is($samples[0]->gtc, '/nfs/new_illumina_geno01/0123456789/0123456789.gtc',
    'GTC file NFS path');
  is($samples[0]->idat('red'),
     '/nfs/new_illumina_geno01/0123456789/0123456789_Red.idat',
     'Red idat file NFS path');
  is($samples[0]->idat('green'),
     '/nfs/new_illumina_geno01/0123456789/0123456789_Grn.idat',
     'Green idat file NFS path');

  dies_ok {
    $db->in_transaction(sub {
                          foreach my $i (1001..2000) {
                            my $sample = $datasets[0]->add_to_samples
                              ({name     => sprintf("%s_%d", $sample_base, $i),
                                beadchip => 'ABC123456',
                                include  => 1});
                            $sample->add_to_states($pass);

                            if ($i == 1900) {
                              die "Test error at $i\n";
                            }
                          }
                        });
  } 'Expected transaction to fail';

  cmp_ok(scalar $datasets[0]->samples, '==', 1000, 'Successful rollback');
}

sub state_changes : Test(23) {
  my $supplier = $db->datasupplier->find_or_create({name      => $ENV{'USER'},
                                                    namespace => 'wtsi'});
  my $snpset = $db->snpset->find({name => 'HumanOmni25-8v1'});
  my $run = $db->piperun->find_or_create({name       => 'states_test',
                                          start_time => time()});

  my $dataset = $run->add_to_datasets
    ({if_project   => 'state_test_project',
      datasupplier => $supplier,
      snpset       => $snpset});

  my $pass =              $db->state->find({name => 'autocall_pass'});
  my $fail =              $db->state->find({name => 'gtc_unavailable'});
  my $pi_approved =       $db->state->find({name => 'pi_approved'});
  my $pi_excluded =       $db->state->find({name => 'pi_excluded'});
  my $consent_withdrawn = $db->state->find({name => 'consent_withdrawn'});
  my $excluded =          $db->state->find({name => 'excluded'});

  $db->in_transaction(sub {
                        foreach my $i (1..10) {
                          my $sample = $dataset->add_to_samples
                            ({name     => sprintf("%s_%d", $sample_base, $i),
                              beadchip => 'ABC123456',
                              include  => 1});
                          $sample->add_to_states($pass);
                        }
                      });

  my $passed_sample1 = ($dataset->samples)[0];
  my $sample_id1 = $passed_sample1->id_sample;

  # Test removing and adding states
  $passed_sample1->remove_from_states($pass);
  cmp_ok(scalar $passed_sample1->states, '==', 0,
         'autocall_pass state removed');

  $passed_sample1->add_to_states($fail);
  cmp_ok(scalar $passed_sample1->states, '==', 1,
         'gtc_unavailable state added 1');
  ok((grep { $_->name eq 'gtc_unavailable' } $passed_sample1->states),
     'gtc_unavailable state added 2');

  # Test that changing states allows inclusion policy to be updated
  ok($passed_sample1->include, 'Passed sample included');
  $passed_sample1->include_from_state;
  $passed_sample1->update;

  my $failed_sample1 = $db->sample->find({id_sample => $sample_id1});
  ok($failed_sample1, 'Found a failed sample');
  ok(!$failed_sample1->include, 'Sample excluded after gtc_unavailable');

  # Swap failed to excluded
  $failed_sample1->remove_from_states($fail);
  $failed_sample1->add_to_states($excluded);

  # Test that pi_approved state overrides exclusion
  $failed_sample1->add_to_states($pi_approved);
  is(scalar $failed_sample1->states, 2, 'pi_approved state added 1');
  ok((grep { $_->name eq 'pi_approved' } $failed_sample1->states),
     'pi_approved state added 2');
  $failed_sample1->include_from_state;
  $failed_sample1->update;

  $failed_sample1 = $db->sample->find({id_sample => $sample_id1});
  ok($failed_sample1->include, 'Sample included after pi_approved');

  # Test that consent_withdrawn overrides everything
  $failed_sample1->add_to_states($consent_withdrawn);
  cmp_ok(scalar $failed_sample1->states, '==', 3,
         'consent_withdrawn state added 1');
  ok((grep { $_->name eq 'consent_withdrawn' } $failed_sample1->states),
     'consent_withdrawn state added 2');
  $failed_sample1->include_from_state;
  $failed_sample1->update;

  $failed_sample1 = $db->sample->find({id_sample => $sample_id1});
  ok(!$failed_sample1->include, 'Sample excluded after consent_withdrawn');

  my $passed_sample2 = ($dataset->samples)[1];
  my $sample_id2 = $passed_sample2->id_sample;

  $passed_sample2->add_to_states($excluded);
  cmp_ok(scalar $passed_sample2->states, '==', 2,
         'excluded state added 1');
  ok((grep { $_->name eq 'excluded' } $passed_sample2->states),
     'excluded state added 2');

  # Test that excluded results in sample exclusion
  ok($passed_sample2->include);
  $passed_sample2->include_from_state;
  $passed_sample2->update;

  my $excluded_sample2 = $db->sample->find({id_sample => $sample_id2});
  ok($excluded_sample2);
  ok(!$excluded_sample2->include, 'Sample exclusion after excluded');

  # Test that pi_approved state overrides exclusion
  $excluded_sample2->add_to_states($pi_approved);
  cmp_ok(scalar $excluded_sample2->states, '==', 3,
         'pi_approved state added 1');
  ok((grep { $_->name eq 'pi_approved' } $excluded_sample2->states),
     'pi_approved state added 2');
  $excluded_sample2->include_from_state;
  $excluded_sample2->update;

  $excluded_sample2 = $db->sample->find({id_sample => $sample_id2});
  ok($excluded_sample2->include,
     'excluded sample included after pi_approved');

  my $passed_sample3 = ($dataset->samples)[2];
  my $sample_id3 = $passed_sample3->id_sample;

  # Test that pi_excluded state overrides inclusion
  $passed_sample3->add_to_states($pi_excluded);
  is(scalar $passed_sample3->states, 2, 'pi_excluded state added 1');
  ok((grep { $_->name eq 'pi_excluded' } $passed_sample3->states),
     'pi_excluded state added 2');
  $passed_sample3->include_from_state;
  $passed_sample3->update;

  $passed_sample1 = $db->sample->find({id_sample => $sample_id3});
  ok(!$passed_sample3->include, 'Sample excluded after pi_excluded');
}

1;
