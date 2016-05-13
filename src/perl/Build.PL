
use utf8;

use strict;
use warnings;

# These paths are propagated to @INC in the build script. The 't'
# directory is added because the Test::Class tests are there.
use lib qw(. t);

use WTSI::DNAP::Utilities::Build;
use Build;

my $build = Build->new
  (module_name     => 'WTSI::Genotyping',
   dist_author     => ['Iain Bancarz <ib5@sanger.ac.uk>',
                       'Keith James <kdj@sanger.ac.uk>'],
   dist_abstract   => 'WTSI genotyping data management tools',
   dist_version    => Build->report_version,,
   license         => 'gpl',
   configure_requires => {
                          'Module::Build'   => 0.42
                         },
   test_requires      => {
                          'TAP::Harness'     => '>= 3.30',
                          'Test::Class'      => '>= 0.41',
                          'Test::MockObject' => 0,
                          'Test::More'       => '>= 0.98',
                          'Test::Exception'  => '>= 0.32',
                          'Test::Compile'    => 0
                         },
   requires          => {
                         'Config::IniFiles'                    => '>= 2.8.6',
                         'DBI'                                 => '>= 1.63',
                         'DBIx::Class'                         => '>= 0.082',
                         'Data::Dump'                          => '>= 1.22',
                         'DateTime'                            => '>= 1.18',
                         'List::AllUtils'                      => '>= 0.09',
                         'Log::Log4perl'                       => '>= 1.46',
                         'Moose'                               => '>= 2.1',
                         'MooseX::Types'                       => '>= 0.45',
                         'npg_tracking::data::reference::find' => '>= 84.8',
                         'Set::Scalar'                         => '>= 1.29',
                         'Text::CSV'                           => '>= 1.33',
                         'Try::Tiny'                           => '>= 0.22',
                         'URI'                                 => '>= 1.67',
                         'WTSI::DNAP::Warehouse::Schema'       => '>= 1.1',
                         'WTSI::NPG::iRODS'                    => '>= 0.15.0'
                        },
   recommends        => {
                         'UUID' => '>= 0.24',
                        },
   ini_files => {'etc/genders.ini'   => 'etc/genders.ini',
                 'etc/methods.ini'   => 'etc/methods.ini',
                 'etc/pipeline.ini'  => 'etc/pipeline.ini',
                 'etc/relations.ini' => 'etc/relations.ini',
                 'etc/snpsets.ini'   => 'etc/snpsets.ini',
                 'etc/states.ini'    => 'etc/states.ini' },
   conf_files => {'etc/log4perl.conf'            => 'etc/log4perl.conf',
                  'etc/qc_config.json'           => 'etc/qc_config.json',
                  'etc/ready_qc_fluidigm.json'   => 'etc/ready_qc_fluidigm.json',
                  'etc/ready_qc_sequenom.json'   => 'etc/ready_qc_sequenom.json',
                  'etc/illuminus_prefilter.json' => 'etc/illuminus_prefilter.json',
                  'etc/zcall_prefilter.json'  => 'etc/zcall_prefilter.json',
                  'etc/reportIntro.tex'       => 'etc/reportIntro.tex',
                  'etc/x_pseudoautosomal.txt' => 'etc/x_pseudoautosomal.txt',
                  'etc/pipeline_ddl.sql'      => 'etc/pipeline_ddl.sql'},
   R_files => {
       '../r/bin/check_xhet_gender.R'   => 'bin/check_xhet_gender.R',
       '../r/bin/heatmapCrHetDensity.R' => 'bin/heatmapCrHetDensity.R',
       '../r/bin/plotCombinedFails.R'   => 'bin/plotCombinedFails.R',
       '../r/bin/plotCrHetDensity.R'    => 'bin/plotCrHetDensity.R',
       '../r/bin/plotCrPlate.R'         => 'bin/plotCrPlate.R',
       '../r/bin/plotHetPlate.R'        => 'bin/plotHetPlate.R',
       '../r/bin/plotMagnitudePlate.R'  => 'bin/plotMagnitudePlate.R',
       '../r/bin/plotIndividualFails.R' => 'bin/plotIndividualFails.R',
       '../r/bin/plotXYdiffDensity.R'   => 'bin/plotXYdiffDensity.R',
       '../r/bin/plotXYdiffPlate.R'     => 'bin/plotXYdiffPlate.R',
       '../r/bin/scatter_plot_metric.R' => 'bin/scatter_plot_metric.R',
       '../r/bin/scatterPlotFails.R'    => 'bin/scatterPlotFails.R'},
   install_path => {'etc' => 'etc'}
  );

$build->add_build_element('conf');
$build->add_build_element('ini');
$build->add_build_element('R');

$build->create_build_script;
