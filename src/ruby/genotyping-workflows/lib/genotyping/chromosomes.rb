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

module Genotyping

  CHR_X= 'X'
  CHR_Y = 'Y'
  CHR_M = 'MT'

  HETEROSOMES = [CHR_X, CHR_Y]
  MITOCHONDRIAL = CHR_M

  # Returns true if chromosome name is a heterosome.
  def heterosome?(chr_name)
    HETEROSOMES.include?(chr_name.upcase)
  end

  # Returns true if chr_name is an autosome.
  def autosome?(chr_name)
    !heterosome?(chr_name) && !mitochondrial?(chr_name)
  end

  # Returns true if chr_name is mitochondrial.
  def mitochondrial?(chr_name)
    MITOCHONDRIAL == chr_name.upcase
  end

  # Returns true if chr_name is an X chromosome.
  def x_chromosome?(chr_name)
    CHR_X == chr_name
  end

  # Returns true if chr_name is a Y chromosome.
  def y_chromosome?(chr_name)
    CHR_Y == chr_name
  end
end
