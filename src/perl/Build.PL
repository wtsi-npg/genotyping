
use strict;
use warnings;
use lib qw(. lib);

use Module::Build;


my $build = Module::Build->new(module_name => 'WTSI::Genotyping',
                               dist_abstract => 'WTSI genotyping data management tools',
                               dist_version => 0.1.0,
                               license  => 'perl',
                               requires => { 'Carp' => 0,
                                             'Test::More' => 0 });
$build->create_build_script;
