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

class TestGenotypeCallTasks < Test::Unit::TestCase
  include TestHelper
  include Genotyping
  include Genotyping::Tasks::GenotypeCall # for mock_study
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

      sim_file, metadata = wait_for('test_gtc_to_sim', 60, 5) do
        gtc_to_sim(sample_json, manifest, 'mock_study.sim',
                   {:work_dir =>  work_dir,
                    :log_dir => work_dir,
                    :metadata => 'chr.json'})
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

end
