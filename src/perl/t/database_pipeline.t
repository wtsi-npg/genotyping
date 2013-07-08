
# Tests WTSI::NPG::Genotyping::Database::Pipeline

use utf8;

use strict;
use warnings;
use Log::Log4perl;

use Test::More tests => 40;
use Test::Exception;

use Data::Dumper;

BEGIN { use_ok('WTSI::NPG::Genotyping::Schema'); }
require_ok('WTSI::NPG::Genotyping::Schema');

use WTSI::NPG::Genotyping::Database::Pipeline;

Log::Log4perl::init('etc/log4perl_tests.conf');

my $ini_path = './etc';
my $dbfile = 't/pipeline.db';

if (-e $dbfile) {
  unlink($dbfile);
}

my $db = WTSI::NPG::Genotyping::Database::Pipeline->new
  (name => 'pipeline',
   inifile => "$ini_path/pipeline.ini",
   dbfile => $dbfile);
ok($db, 'A pipeline Database');
ok(-e $dbfile);
ok($db->disconnect);
undef $db;

$db = WTSI::NPG::Genotyping::Database::Pipeline->new
  (name => 'pipeline',
   inifile => "$ini_path/pipeline.ini",
   dbfile => $dbfile);
ok($db, 'A database file reopened.');

ok(WTSI::NPG::Genotyping::Database::Pipeline->new
   (name => 'pipeline',
    inifile => "$ini_path/pipeline.ini",
    dbfile => $dbfile,
    overwrite => 1), 'A database file overwrite');

dies_ok { $db->snpset->all }
  'Expected AUTOLOAD to fail when unconnected';

my $schema = $db->connect(RaiseError => 1,
                          on_connect_do => 'PRAGMA foreign_keys = ON')->schema;
ok($schema, 'A database Schema');

dies_ok { $db->should_not_autoload_this_method->all }
  'Expected AUTOLOAD to fail on invalid method';

$db->populate;

is($db->snpset->count, 29, 'The snpset dictionary');
is($db->method->count, 5, 'The method dictionary');
is($db->relation->count, 2, 'The relation dictionary');
is($db->state->count, 12, 'The state dictionary',);

my $supplier = $db->datasupplier->find_or_create({name => $ENV{'USER'},
                                                  namespace => 'wtsi'});
ok($supplier, 'A supplier inserted');

my $run = $db->piperun->find_or_create({name => 'test',
                                        start_time => time()});
ok($run, 'A run inserted');

my $project_base = 'test_project';
my $snpset = $db->snpset->find({name => 'HumanOmni25-8v1'});
ok($snpset, 'A snpset found');
ok($run->validate_snpset($snpset), 'A snpset validated in an empty piperun');

my @datasets;
foreach my $i (1..3) {
  my $dataset =
    $run->add_to_datasets({if_project => sprintf("%s_%d", $project_base, $i),
                           datasupplier => $supplier,
                           snpset => $snpset});
  ok($dataset, 'A dataset inserted');
  push @datasets, $dataset;
}

ok($run->validate_datasets, 'A piperun validated');
ok($run->validate_snpset($snpset), 'A snpset validated in a full piperun');
ok(! $run->validate_snpset($db->snpset->find({name => 'HumanOmni25-4v1'})),
   'A snpset validated in a mismatched piperun');

my $sample_base = 'test_sample';
my $pass = $db->state->find({name => 'autocall_pass'});
ok($pass, 'A state found');

my $fail = $db->state->find({name => 'autocall_fail'});
my $pi_approved = $db->state->find({name => 'pi_approved'});
my $consent_withdrawn = $db->state->find({name => 'consent_withdrawn'});

$db->in_transaction(sub {
                      foreach my $i (1..1000) {
                        my $sample = $datasets[0]->add_to_samples
                          ({name => sprintf("%s_%d", $sample_base, $i),
                            beadchip => 'ABC123456',
                            include => 1});
                        $sample->add_to_states($pass);
                      }
                    });

my @samples = $datasets[0]->samples;
is(scalar @samples, 1000, 'Expected samples found');
my @states = $samples[0]->states;
is(scalar @states, 1);
is($states[0]->name, 'autocall_pass');

dies_ok {
  $db->in_transaction(sub {
                        foreach my $i (1001..2000) {
                          my $sample = $datasets[0]->add_to_samples
                            ({name => sprintf("%s_%d", $sample_base, $i),
                              beadchip => 'ABC123456',
                              include => 1});
                          $sample->add_to_states($pass);

                          if ($i == 1900) {
                            die "Test error at $i\n";
                          }
                        }
                      });
} 'Expected transaction to fail';

is(scalar $datasets[0]->samples, 1000, 'Successful rollback');

# Test removing and adding states
my $passed_sample = ($datasets[0]->samples)[0];
my $sample_id = $passed_sample->id_sample;

$passed_sample->remove_from_states($pass);
is(scalar $passed_sample->states, 0, "autocall_pass state removed");
$passed_sample->add_to_states($fail);
is(scalar $passed_sample->states, 1, "autocall_fail state added 1");
ok((grep { $_->name eq 'autocall_fail' } $passed_sample->states),
   "autocall_fail state added 2");

# Test that changing states allows inclusion policy to be updated
ok($passed_sample->include);
$passed_sample->include_from_state;
$passed_sample->update;

my $failed_sample = $db->sample->find({id_sample => $sample_id});
ok($failed_sample);
ok(!$failed_sample->include, "Sample excluded after autocall_fail");

# Test that pi_approved state overrides exclusion
$failed_sample->add_to_states($pi_approved);
ok((grep { $_->name eq 'pi_approved' } $failed_sample->states),
    "pi_approved state added");
$failed_sample->include_from_state;
$failed_sample->update;

$failed_sample = $db->sample->find({id_sample => $sample_id});
ok($failed_sample->include, "Sample included after pi_approved");

# Test that consent_withdrawn overrides everything
$failed_sample->add_to_states($consent_withdrawn);
ok((grep { $_->name eq 'consent_withdrawn' } $failed_sample->states),
    "consent_withdrawn state added");
$failed_sample->include_from_state;
$failed_sample->update;

$failed_sample = $db->sample->find({id_sample => $sample_id});
ok(!$failed_sample->include, "Sample excluded after consent_withdrawn");

# Clean up
unlink($dbfile);
