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

class TestFetchSampleData < Test::Unit::TestCase
  include TestHelper
  include Genotyping

  def initialize(name)
    super(name)
    @msg_host = 'hgs3b'
    @msg_port = 11301
  end

  def data_path
    File.expand_path(File.join(File.dirname(__FILE__), '..', 'data'))
  end

  def test_fetch_sample_data
    name = "test_fetch_sample_data_" + $$.to_s
    run_path = File.join(data_path, name)
    FileUtils.mkdir_p(run_path) unless File.exists?(run_path)

    dbfile = File.join(data_path, 'genotyping.db')
    manifest_path = '/nfs/wtccc/data5/genotyping/chip_descriptions/Illumina_Infinium/current'
    manifest = File.join(manifest_path, 'Human670-QuadCustom_v1_A.bpm.csv')
    run_name = 'run1'

    args = [dbfile, run_name, run_path, {:manifest => manifest}]
    timeout = 720
    log = 'percolate.log'
    result = test_workflow(name, Genotyping::Workflows::FetchSampleData,
                           timeout, run_path, log, args)
    assert(result)

    Percolate.log.close
    if result
      FileUtils.rm_rf(run_path)
    end
  end
end


