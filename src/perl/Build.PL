
use utf8;

use strict;
use warnings;
use lib qw(.);

use Module::Build;
use Build;

my $build = Build->new
  (module_name => 'WTSI::Genotyping',
   dist_author => ['Iain Bancarz <ib5@sanger.ac.uk>',
                   'Keith James <kdj@sanger.ac.uk>'],
   dist_abstract => 'WTSI genotyping data management tools',
   dist_version => '1.2.2',
   license  => 'gpl',
   use_tap_harness => 1,
   tap_harness_args => {lib => ['./blib/lib', './blib/arch', './t']},
   requires => {'Carp' => 0,
                'Test::Class' => 0,
                'Test::More' => 0,
                'Test::Exception' => 0,
                'DBIx::Class' => 0,
                'Config::IniFiles' => 0},
   ini_files => {'etc/genders.ini' => 'etc/genders.ini',
                 'etc/methods.ini' => 'etc/methods.ini',
                 'etc/pipeline.ini' => 'etc/pipeline.ini',
                 'etc/relations.ini' => 'etc/relations.ini',
                 'etc/snpsets.ini' => 'etc/snpsets.ini',
                 'etc/states.ini' => 'etc/states.ini' },
   conf_files => {'etc/log4perl.conf' => 'etc/log4perl.conf',
                  'etc/qc_config.json' => 'etc/qc_config.json',
                  'etc/illuminus_prefilter.json' => 'etc/illuminus_prefilter.json',
                  'etc/zcall_prefilter.json' => 'etc/zcall_prefilter.json',
                  'etc/reportIntro.tex' => 'etc/reportIntro.tex',
                  'etc/x_pseudoautosomal.txt' => 'etc/x_pseudoautosomal.txt',
                  'etc/pipeline_ddl.sql' => 'etc/pipeline_ddl.sql'},
   R_files => {
       '../r/bin/check_xhet_gender.R' => 'bin/check_xhet_gender.R',
       '../r/bin/heatmapCrHetDensity.R' => 'bin/heatmapCrHetDensity.R',
       '../r/bin/plotCombinedFails.R' => 'bin/plotCombinedFails.R',
       '../r/bin/plotCrHetDensity.R' => 'bin/plotCrHetDensity.R',
       '../r/bin/plotCrPlate.R' => 'bin/plotCrPlate.R',
       '../r/bin/plotHetPlate.R' => 'bin/plotHetPlate.R',
       '../r/bin/plotMagnitudePlate.R' => 'bin/plotMagnitudePlate.R',
       '../r/bin/plotIndividualFails.R' => 'bin/plotIndividualFails.R',
       '../r/bin/plotXYdiffDensity.R' => 'bin/plotXYdiffDensity.R',
       '../r/bin/plotXYdiffPlate.R' => 'bin/plotXYdiffPlate.R',
       '../r/bin/scatter_plot_metric.R' => 'bin/scatter_plot_metric.R',
       '../r/bin/scatterPlotFails.R' => 'bin/scatterPlotFails.R'},
   install_path => {'etc' => 'etc'}
  );

$build->add_build_element('conf');
$build->add_build_element('ini');
$build->add_build_element('R');

$build->create_build_script;
