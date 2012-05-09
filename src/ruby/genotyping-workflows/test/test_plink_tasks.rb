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

class TestPlinkTasks < Test::Unit::TestCase
  include TestHelper
  include Genotyping
  include Genotyping::Tasks::Plink

  def initialize(name)
    super(name)
    @msg_host = Socket.gethostname
    @msg_port = 11300
  end

  def setup
    Percolate.log = Logger.new(File.join(data_path, 'test_plink_tasks.log'))
    Percolate.asynchronizer = SystemAsynchronizer.new
  end

  def data_path
    File.expand_path(File.join(File.dirname(__FILE__), '..', 'data'))
  end

  def test_merge_bed
    run_test_if(method(:plink_available?), "Skipping test_merge_bed") do
      work_dir = make_work_dir('test_merge_bed', data_path)

      bed_files = (0..4).collect { |i| File.join(data_path, "mock_study1.part.#{i}.bed")  }

      merged = wait_for('test_merge_bed', 120, 5) do
        merge_bed(bed_files, 'mock_study1.bed', :work_dir => work_dir,
                  :log_dir => work_dir)
      end

      assert_equal(3, merged.size)
      merged.each do |file|
        assert(File.exist?(file))
      end

      Percolate.log.close
      remove_work_dir(work_dir)
    end
  end

end
