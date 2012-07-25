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

class TestWorkflows < Test::Unit::TestCase
  include TestHelper
  include Genotyping

  def initialize(name)
    super(name)
    @msg_host = Socket.gethostname
    @msg_port = 11300
  end

  def data_path
    File.expand_path(File.join(File.dirname(__FILE__), '..', 'data'))
  end

  def test_fetch_sample_data
    manifest_path = ENV['MANIFEST_PATH']
    manifest_name = ENV['BEADPOOL_MANIFEST']
    name = 'test_fetch_sample_data'

    run_test_if(lambda { manifest_path && manifest_name }, "Skipping #{name}") do
      work_dir = make_work_dir(name, data_path)
      dbfile = File.join(data_path, 'genotyping.db')
      manifest = File.join(manifest_path, manifest_name)
      run_name = 'run1'

      args = [dbfile, run_name, work_dir, {:manifest => manifest}]
      timeout = 720
      log = 'percolate.log'
      result = test_workflow(name, Genotyping::Workflows::FetchSampleData,
                             timeout, work_dir, log, args)
      assert(result)

      Percolate.log.close
      remove_work_dir(work_dir) if result
    end
  end

  def test_genotype_illuminus
    manifest_path = ENV['MANIFEST_PATH']
    manifest_name = ENV['BEADPOOL_MANIFEST']
    name = 'test_genotype_illuminus'

    run_test_if(lambda { manifest_path && manifest_name }, "Skipping #{name}") do
      work_dir = make_work_dir(name, data_path)
      dbfile = File.join(data_path, 'genotyping.db')
      manifest = File.join(manifest_path, manifest_name)
      run_name = 'run1'

      args = [dbfile, run_name, work_dir, {:manifest => manifest,
                                           :chunk_size => 500,
                                           :queue => :normal,
                                           :memory => 1024}]
      timeout = 720
      log = 'percolate.log'
      result = test_workflow(name, Genotyping::Workflows::GenotypeIlluminus,
                             timeout, work_dir, log, args)
      assert(result)

      Percolate.log.close
      remove_work_dir(work_dir) if result
    end
  end

  def test_genotype_genosnp
    manifest_path = ENV['MANIFEST_PATH']
    manifest_name = ENV['BEADPOOL_MANIFEST']
    name = 'test_genotype_genosnp'

    run_test_if(lambda { manifest_path && manifest_name }, "Skipping #{name}") do
      work_dir = make_work_dir(name, data_path)
      dbfile = File.join(data_path, 'genotyping.db')
      manifest = File.join(manifest_path, manifest_name)
      run_name = 'run1'

      args = [dbfile, run_name, work_dir, {:manifest => manifest,
                                           :chunk_size => 2,
                                           :queue => :normal,
                                           :memory => 2048}]
      timeout = 720
      log = 'percolate.log'
      result = test_workflow(name, Genotyping::Workflows::GenotypeGenoSNP,
                             timeout, work_dir, log, args)
      assert(result)

      Percolate.log.close
      remove_work_dir(work_dir) if result
    end

  end
end


