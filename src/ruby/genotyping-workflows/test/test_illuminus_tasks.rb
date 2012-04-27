#-- encoding: UTF-8
#
# Copyright (c) 2012 Genome Research Ltd. All rights reserved.
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

require 'genotyping'
require File.join(testpath, 'test_helper')

class TestIlluminusTasks < Test::Unit::TestCase
  include TestHelper
  include Genotyping
  include Genotyping::Tasks::Illuminus
  include Genotyping::Tasks::GenotypeCall

  def initialize(name)
    super(name)
    @msg_host = Socket.gethostname
    @msg_port = 11300
  end

  def setup
    Percolate.log = Logger.new(File.join(data_path, 'test_illuminus_tasks.log'))
    Percolate.asynchronizer =
        LSFAsynchronizer.new(:job_arrays_dir => data_path)
  end

  def data_path
    File.expand_path(File.join(File.dirname(__FILE__), '..', 'data'))
  end

  def test_call_from_sim
    run_test_if(method(:illuminus_available?), "Skipping test_call_from_sim") do
      work_dir = make_work_dir('test_call_from_sim', data_path)

      sample_json, manifest, gtc_files = wait_for('mock_study', 60, 5) do
        mock_study('mock_study', 5, 2000, {:work_dir =>  work_dir,
                                           :log_dir => work_dir})
      end

      sim_file = wait_for('gtc_to_sim', 60, 5) do
        gtc_to_sim(sample_json, manifest, 'mock_study.sim',
                   {:work_dir =>  work_dir,
                    :log_dir => work_dir})
      end

      call_file1 = wait_for('test_call_from_sim', 120, 5) do
        call_from_sim(sim_file, sample_json, manifest, 'mock_study1.call',
                      {:work_dir =>  work_dir,
                       :log_dir => work_dir,
                       :start => 0,
                       :end => 1000})
      end

      call_file2 = wait_for('test_call_from_sim', 120, 5) do
        call_from_sim(sim_file, sample_json, manifest, 'mock_study2.call',
                      {:work_dir =>  work_dir,
                       :log_dir => work_dir,
                       :start => 1000,
                       :end => 2000},
                      :queue => :small)
      end

      assert(File.exist?(call_file1))
      assert_equal(1001 , File.open(call_file1) { |file| file.readlines.size })

      assert(File.exist?(call_file2))
      assert_equal(1001 , File.open(call_file2) { |file| file.readlines.size })

      Percolate.log.close
      remove_work_dir(work_dir)
    end
  end

  def test_call_from_sim_p
    run_test_if(method(:illuminus_available?), "Skipping test_call_from_sim_p") do
      work_dir = make_work_dir('test_call_from_sim_p', data_path)

      sample_json, manifest, gtc_files = wait_for('mock_study', 60, 5) do
        mock_study('mock_study', 5, 2000, {:work_dir =>  work_dir,
                                           :log_dir => work_dir})
      end

      sim_file = wait_for('gtc_to_sim', 60, 5) do
        gtc_to_sim(sample_json, manifest, 'mock_study.sim',
                   {:work_dir =>  work_dir,
                    :log_dir => work_dir})
      end

      call_files1 = wait_for('test_call_from_sim_p', 120, 5) do
        call_from_sim_p(sim_file, sample_json, manifest, 'mock_study1.call',
                        {:work_dir =>  work_dir,
                         :log_dir => work_dir,
                         :start => 0,
                         :end => 2000,
                         :size => 100,
                         :group_size => 5},
                        :queue => :small)
      end

      assert_equal(20, call_files1.size)
      call_files1.each do |file|
        assert(File.exist?(file))
        assert_equal(101 , File.open(file) { |f| f.readlines.size })
      end

      Percolate.log.close
      remove_work_dir(work_dir)
    end
  end
end
