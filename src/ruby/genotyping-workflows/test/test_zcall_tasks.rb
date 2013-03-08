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
  #include Genotyping::Tasks::GenotypeCall

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
    @sample_json = data_dir+'gtc.json'
  end

  def setup
    Percolate.log = Logger.new(File.join(data_path, 'test_zcall_tasks.log'))
    #Percolate.asynchronizer = SystemAsynchronizer.new
    Percolate.asynchronizer = LSFAsynchronizer.new(:job_arrays_dir => data_path)
  end

  def data_path
    File.expand_path(File.join(File.dirname(__FILE__), '..', 'data'))
  end

  def test_prepare_thresholds
    run_test_if(method(:zcall_prepare_available?), 
                "Skipping test_zcall_prepare") do
      work_dir = make_work_dir('test_zcall', data_path)
      zstart = 6
      ztotal = 3

      t_json, t_files = wait_for('test_zcall_prepare', 180, 5) do
        prepare_thresholds(@egt, zstart, ztotal,  
                           {:work_dir => work_dir,
                             :log_dir => work_dir})
      end

      assert(File.exist?(t_json))
      assert_equal(3, JSON.parse(File.read(t_json)).size)
      assert_equal(3, t_files.size)
      t_files.each do |file| 
        assert(File.exist?(file))
        assert_equal(247871, File.open(file) { |f| f.readlines.size })
      end
      Percolate.log.close
      remove_work_dir(work_dir)
    end
  end

  def test_evaluate_thresholds
    run_test_if(method(:zcall_evaluate_available?), 
                "Skipping test_zcall_evaluate") do
      work_dir = make_work_dir('test_zcall', data_path)
 
      foo = wait_for('test_zcall_evaluate', 120, 5) do
        evaluate_thresholds(@threshold_json, @sample_json, @manifest, @egt,
                            {
                              :start => 0, :end => 8, :size => 4,
                              :work_dir => work_dir
                            })
      end

      Percolate.log.close
      remove_work_dir(work_dir)

    end
  end

end # class TestZCallTasks


if __FILE__ == $0
  ztest = TestZCallTasks
end
