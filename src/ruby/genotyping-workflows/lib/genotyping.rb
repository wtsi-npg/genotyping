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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

require 'percolate'

module Genotyping
  include Percolate
  include Tasks
  include Utilities
end

require 'json'

require 'genotyping/chromosomes'
require 'genotyping/exceptions'
require 'genotyping/utilities'
require 'genotyping/version'
require 'genotyping/sim'

require 'genotyping/tasks'
require 'genotyping/workflows'

require 'genotyping/tasks/database'
require 'genotyping/tasks/genotype_call'
require 'genotyping/tasks/illuminus'
require 'genotyping/tasks/plink'
require 'genotyping/tasks/genosnp'
require 'genotyping/tasks/quality_control'
require 'genotyping/tasks/simtools'
require 'genotyping/tasks/zcall'

require 'genotyping/workflows/fetch_sample_data'
require 'genotyping/workflows/genotype_illuminus'
require 'genotyping/workflows/genotype_genosnp'
