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

##########################################################
# test ground for development of the zcall workflow
# TODO merge into test_workflows.rb
##########################################################

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

class TestWorkflowZCall < Test::Unit::TestCase
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

  def test_genotype_zcall
    manifest = ENV['BEADPOOL_MANIFEST']
    egt_file = ENV['BEADPOOL_EGT']
    unless egt_file
      egt_file = '/nfs/gapi/data/genotype/zcall_test/'+
        'Human670-QuadCustom_v1_A.egt'
    end
    name = 'test_genotype_zcall'

    run_test_if(lambda { manifest }, "Manifest not found, skipping #{name}") do
      work_dir = make_work_dir(name, data_path)
      dbfile = File.join(work_dir, name + '.db')
      run_name = 'run1'

      FileUtils.copy(File.join(data_path, 'genotyping.db'), dbfile)
      args = [dbfile, run_name, work_dir, {:manifest => manifest,
                                           :egt => egt_file,
                                           :chunk_size => 12,
                                           :zstart => 5,
                                           :ztotal => 3,
                                           :memory => 2048,
                                           :select => 'lenny'}]
      timeout = 1800 # was 720
      log = 'percolate.log'
      result = test_workflow(name, Genotyping::Workflows::GenotypeZCall,
                             timeout, work_dir, log, args)
      assert(result)

      Percolate.log.close
      remove_work_dir(work_dir) if result
    end

  end
end
