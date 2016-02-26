#-- encoding: UTF-8
#
# Copyright (c) 2012, 2016 Genome Research Ltd. All rights reserved.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

devpath = File.expand_path(File.join(File.dirname(__FILE__), '..'))
libpath = File.join(devpath, 'lib')
testpath = File.join(devpath, 'test')

$:.unshift(libpath) unless $:.include?(libpath)

require 'rubygems'
require 'test/unit'
require 'fileutils'
require 'json'

require 'genotyping'
require File.join(testpath, 'test_helper')

class TestIlluminusWorkflow < Test::Unit::TestCase
  include TestHelper
  include Genotyping
  include Genotyping::Tasks

  def initialize(name)
    super(name)
    @msg_host = Socket.gethostname
    @msg_port = 11300
  end

  def test_genotype_illuminus
    external_data = ENV['GENOTYPE_TEST_DATA']
    manifest = manifest_path
    name = 'test_genotype_illuminus'

    run_test_if((lambda { illuminus_available? && manifest } and method(:plinktools_diff_available?)), "Skipping #{name}") do
      work_dir = make_work_dir(name, data_path)
      dbfile = File.join(work_dir, name + '.db')
      run_name = 'run1'
      pipe_ini = File.join(data_path, 'genotyping.ini')

      FileUtils.copy(File.join(external_data, 'genotyping.db'), dbfile)
      fconfig = File.join(data_path, 'illuminus_test_prefilter.json')
      vcf = File.join(external_data, 'sequenom_abvc.vcf')
      plex_0 = File.join(external_data, 'W30467_snp_set_info_GRCh37.tsv')
      plex_1 = File.join(external_data, 'qc_fluidigm_snp_info_GRCh37.tsv')
      # plex_1 not needed for workflow, but tests handling multiple plex args

      args_hash = {:manifest => manifest,
                   :plex_manifest => plex_path,
                   :config => pipe_ini,
                   :filterconfig => fconfig,
                   :gender_method => 'Supplied',
                   :chunk_size => 10000,
                   :memory => 2048,
                   :queue => 'yesterday',
                   :vcf => [vcf, ],
                   :plex_manifest => [plex_0, plex_1]
      }
      args = [dbfile, run_name, work_dir, args_hash]
      timeout = 1400
      log = 'percolate.log'
      result = test_workflow(name, Genotyping::Workflows::GenotypeIlluminus,
                             timeout, work_dir, log, args)
      assert(result)

      plink_name = run_name+'.illuminus'
      stem = File.join(work_dir, plink_name)
      master = File.join(external_data, plink_name)
      equiv = plink_equivalent?(stem, master, run_name, 
                                {:work_dir => work_dir,
                                 :log_dir => work_dir})
      assert(equiv)
      Percolate.log.close

      remove_work_dir(work_dir) if (result and equiv)
    end
  end


end
