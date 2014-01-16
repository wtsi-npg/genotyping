#-- encoding: UTF-8
#
# Copyright (c) 2013 Genome Research Ltd. All rights reserved.
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

class TestSimtoolsTasks < Test::Unit::TestCase
  include TestHelper
  include Genotyping
  include Genotyping::Tasks::GenotypeCall # for mock_study
  include Genotyping::Tasks::Metadata
  include Genotyping::Tasks::Simtools

  def initialize(name)
    super(name)
    @msg_host = Socket.gethostname
    @msg_port = 11300
  end

  def setup
    Percolate.log = Logger.new(File.join(data_path, 'test_simtools_tasks.log'))
    Percolate.asynchronizer = SystemAsynchronizer.new
  end

  def data_path
    File.expand_path(File.join(File.dirname(__FILE__), '..', 'data'))
  end

  def test_gtc_to_sim
    run_test_if(method(:simtools_available?), "Skipping test_gtc_to_sim") do
      work_dir = make_work_dir('test_gtc_to_sim', data_path)

      sample_json, manifest, gtc_files = wait_for('mock_study', 60, 5) do
        mock_study('mock_study', 5, 100, {:work_dir =>  work_dir,
                                          :log_dir => work_dir})
      end

      sim_file = wait_for('test_gtc_to_sim', 60, 5) do
        gtc_to_sim(sample_json, manifest, 'mock_study.sim',
                   {:work_dir =>  work_dir,
                    :log_dir => work_dir})
      end

      sim = SIM.new(sim_file)
      assert_equal(sim_file, sim.sim_file)
      assert_equal(1, sim.version)
      assert_equal(255, sim.sample_name_size)
      assert_equal(1, sim.number_format) # unnormalized, uint16
      assert_equal(100, sim.num_probes)
      assert_equal(2, sim.num_channels)
      assert_equal(5, sim.num_samples)

      Percolate.log.close
      remove_work_dir(work_dir)
    end
  end

  def test_gtc_to_bed
    run_test_if(method(:g2i_available?), "Skipping test_gtc_to_bed") do
      work_dir = make_work_dir('test_gtc_to_bed', data_path)

      sample_json, manifest, gtc_files = wait_for('mock_study', 60, 5) do
        mock_study('mock_study', 5, 100, {:work_dir =>  work_dir,
                                          :log_dir => work_dir})
      end

      bed_file = wait_for('test_gtc_to_bed', 60, 5) do
        gtc_to_bed(sample_json, manifest, 'mock_study.bed',
                   {:work_dir =>  work_dir,
                    :log_dir => work_dir})
      end

      assert(File.exists?(bed_file))

      Percolate.log.close
      remove_work_dir(work_dir)
    end
  end

  def test_g2i_normalize
    # test the normalization side effect of g2i (gtc-to-bed executable)
    # compare output of g2i with normalized and un-normalized manifests
    manifest = ENV['BEADPOOL_MANIFEST']
    name = 'test_g2i_normalize'
    run_test_if((lambda { g2i_available? && manifest }), "Skipping test_g2i_normalize") do
      work_dir = make_work_dir(name, data_path)
      # create normalized and un-normalized copies of manifest
      manifest_raw = File.join(work_dir, File.basename(manifest))
      FileUtils.copy(manifest, manifest_raw)
      manifest_norm = File.join(work_dir, 'manifest_normalized.bpm.csv')
      args = {:work_dir => work_dir, :log_dir => work_dir}
      wait_for('normalize_manifest', 60, 5) do
        normalize_manifest(manifest_raw, manifest_norm, args)
      end    
      # generate sample json file from test pipeline DB
      dbfile = File.join(work_dir, name + '.db')
      FileUtils.copy(File.join(data_path, 'genotyping.db'), dbfile)
      run_name = 'run1'
      sample_json = File.join(work_dir, name+'_sample.json')
      wait_for('sample_intensities', 60, 5) do
        sample_intensities(dbfile, run_name, sample_json,
                           args.merge({:gender_method => "Supplied"}))
      end
      # now run gtc_to_bed twice, once with each manifest version
      prefix_raw = 'not_normalized'
      prefix_norm = 'normalized'
      bed_file_raw = wait_for('gtc_to_bed_raw', 60, 5) do
        gtc_to_bed(sample_json, manifest_raw, prefix_raw+".bed", args)
      end
      bed_file_norm = wait_for('gtc_to_bed_norm', 60, 5) do
        gtc_to_bed(sample_json, manifest_norm, prefix_norm+".bed", args)
      end
      # compare outputs
      for suffix in [".bed", ".bim", ".fam"] do
        rawfile = File.join(work_dir, prefix_raw+suffix)
        normfile = File.join(work_dir, prefix_norm+suffix)
        assert(FileUtils.compare_file(rawfile, normfile))
      end
      remove_work_dir(work_dir)
    end
  end

  def test_normalize_manifest
    run_test_if(method(:normalize_available?), "Skipping test_normalize_manifest") do
      work_dir = make_work_dir('test_normalize_manifest', data_path)

      input = File.join(data_path, 'example_manifest.bpm.csv')
      output = File.join(work_dir, 'normalized.bpm.csv')
      master = File.join(data_path, 'example_manifest_normalized.bpm.csv')

      normalized = wait_for('test_normalize_manifest', 60, 5) do
        normalize_manifest(input, output, 
                           {:work_dir => work_dir, :log_dir => work_dir})
      end

      assert(File.exists?(normalized))
      assert(FileUtils.compare_file(normalized, master))

      Percolate.log.close
      remove_work_dir(work_dir)
      
    end
  end 


end
