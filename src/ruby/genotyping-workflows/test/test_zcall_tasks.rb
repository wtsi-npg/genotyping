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
require 'json'

require 'genotyping'
require File.join(testpath, 'test_helper')

class TestZCallTasks < Test::Unit::TestCase
  include TestHelper
  include Genotyping
  include Genotyping::Tasks::ZCall

  # TODO Create mock study with compatible EGT file
  # EGT file format is undocumented
  # Maybe start with 'real' EGT & BPM.CSV files and concoct fake GTC files
  # Would replace mock_study function of genotype_call (LISP code)
  #
  # For now, just use test data for Python code
  # These GTC files are *not* authorised for public release!
  
  def initialize(name)
    super(name)
    @msg_host = Socket.gethostname
    @msg_port = 11300
    data_dir = '/nfs/gapi/data/genotype/zcall_test/'
    @egt = data_dir+'HumanExome-12v1.egt'
    @manifest = data_dir+'HumanExome-12v1_A.bpm.csv'
    @threshold_json = data_dir+'thresholds.json'
    @thresholds_z6 = data_dir+'thresholds_HumanExome-12v1_z06.txt' 
    @sample_json = data_dir+'gtc.json'
    @merged_json = data_dir+'merged_z_evaluation.json'
    @queue = :small
  end

  def setup
    Percolate.log = Logger.new(File.join(data_path, 'test_zcall_tasks.log'))
  end

  def data_path
    File.expand_path(File.join(File.dirname(__FILE__), '..', 'data'))
  end

  def test_prepare_thresholds
    run_test_if(method(:zcall_prepare_available?), 
                "Skipping test_zcall_prepare") do
      work_dir = make_work_dir('zcall_prepare', data_path)
      Percolate.asynchronizer = LSFAsynchronizer.new(:job_arrays_dir=>work_dir)
      zstart = 6
      ztotal = 3

      t_json, t_files = wait_for('test_zcall_prepare', 180, 5) do
        prepare_thresholds(@egt, zstart, ztotal,  
                           {:work_dir => work_dir,
                             :log_dir => work_dir},
                           :queue=>@queue)
      end

      assert(File.exist?(t_json))
      assert_equal(ztotal, JSON.parse(File.read(t_json)).size)
      assert_equal(ztotal, t_files.size)
      t_files.each do |file| 
        assert(File.exist?(file))
        assert_equal(247871, File.open(file) { |f| f.readlines.size })
      end
      Percolate.log.close
      remove_work_dir(work_dir)
    end
  end

  def test_evaluate_merge_thresholds
    run_test_if(method(:zcall_evaluate_available?), 
                "Skipping test_zcall_evaluate_merge") do
      work_dir = make_work_dir('zcall_evaluate_merge', data_path)
      Percolate.asynchronizer = LSFAsynchronizer.new(:job_arrays_dir=>work_dir)
 
      metrics_path = wait_for('test_zcall_evaluate', 120, 5) do
        evaluate_thresholds(@threshold_json, @sample_json, @manifest, @egt,
                            {
                              :start => 0, :end => 8, :size => 4,
                              :work_dir => work_dir
                            },
                            :queue=>@queue)
      end

      merged_path = wait_for('test_zcall_merge', 120, 5) do
        merge_evaluation(metrics_path,  @threshold_json,
                         { :work_dir => work_dir },
                         :queue=>@queue)
      end
      merged_file = File.open(merged_path)
      assert_equal(JSON.load(merged_file),
                   JSON.load(File.open(@merged_json)))
      merged_file.close() # ensure file close before directory removal
      Percolate.log.close
      remove_work_dir(work_dir)
    end
  end

  def test_run_zcall
    run_test_if(method(:zcall_available?), "Skipping test_run_zcall") do
      work_dir = make_work_dir('test_zcall_run', data_path)
      Percolate.asynchronizer = LSFAsynchronizer.new(:job_arrays_dir=>work_dir)
      result = wait_for('test_zcall_run', 120, 5) do
        run_zcall(@thresholds_z6, @sample_json, @manifest, @egt, work_dir,
                  { :work_dir => work_dir },
                  :queue=>@queue)
      end
      cmd = 'plink --bfile '+File.join(work_dir, 'zcall')+
        ' --silent --out '+File.join(work_dir, 'plink')
      assert(system(cmd))
      Percolate.log.close
      remove_work_dir(work_dir)
    end
  end

  def test_run_zcall_array
    run_test_if(method(:zcall_available?), "Skipping test_run_zcall_array") do
      work_dir = make_work_dir('test_zcall_run_array', data_path)
      Percolate.asynchronizer = LSFAsynchronizer.new(:job_arrays_dir=>work_dir)
      result = wait_for('test_zcall_run_array', 120, 5) do
        run_zcall_array(@thresholds_z6, @sample_json, @manifest, @egt, work_dir,
                        { :start => 0, :end => 8, :size => 4,
                          :work_dir => work_dir },
                        :queue=>@queue)
      end
      Percolate.log.close
      assert_equal(2, result.size)
      ['000', '001'].collect do | x |
        cmd =  'plink --bfile '+
          File.join(work_dir, 'zcall_temp', 'samples_part_'+x)+
          ' --silent --out '+File.join(work_dir, 'plink_'+x)
        assert(system(cmd))
      end
      remove_work_dir(work_dir)
    end
  end

end # class TestZCallTasks


if __FILE__ == $0
  ztest = TestZCallTasks
end
