#--
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

module Genotyping

  class SIM
    MAGIC_NUMBER = 'sim'
    NUMBER_FORMATS = [0, 1]

    attr_reader :sim_file
    attr_reader :id
    attr_reader :version
    attr_reader :sample_name_size
    attr_reader :num_samples
    attr_reader :num_probes
    attr_reader :num_channels
    attr_reader :number_format

    def initialize(sim_file)
      open(sim_file, 'rb') do |sim|
        @sim_file = sim_file
        @id = sim.read(3).unpack('a3').to_s
        @version = sim.read(1).unpack('C').first
        @sample_name_size = sim.read(2).unpack('v').first
        @num_samples = sim.read(4).unpack('V').first
        @num_probes = sim.read(4).unpack('V').first
        @num_channels = sim.read(1).unpack('C').first
        @number_format = sim.read(1).unpack('C').first

        unless @id == MAGIC_NUMBER
          raise FileFormatError.new("Invalid SIM data in '#{sim_file}' (bad magic number '#{@id}')",
                                    sim_file)
        end

        unless NUMBER_FORMATS.include?(@number_format)
          raise FileFormatError.new("Unknown number format code '#{@number_format}'",
                                    sim_file)
        end
      end
    end

    def to_s
      format =
          case self.number_format
            when 0:
              '32-bit float'
            when 1:
              'scaled 16-bit int'
            else
              raise
          end

      file = File.basename(self.sim_file)

      "<#{self.id} v#{self.version} [#{format}] #{file} #{self.num_probes} probes" +
          ", #{self.num_samples} samples>"
    end
  end

end
