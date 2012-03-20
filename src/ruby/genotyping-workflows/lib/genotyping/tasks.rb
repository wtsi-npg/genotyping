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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

module Genotyping::Tasks

  # The default value for the number of file partitions to create in a single
  # directory
  DEFAULT_GROUP_SIZE = 100

  def process_task_args(args, defaults = {})
    args = defaults.merge(args)
    work_dir = args.delete(:work_dir)
    log_dir = args.delete(:log_dir)
    log_dir ||= work_dir

    [work_dir, log_dir]
  end

  # Returns an Array of Ranges which are indices for chunked
  # data processing. Given a start index, end index and a chunk size,
  # returns the relevant indices.
  def make_ranges(from, to, chunk_size)
    unless to >= from
      raise ArgumentError, "'to' (#{to}) was not > 'from' (#{from})"
    end
    num_elements = to - from

    num_ranges, partial_range = num_elements.divmod(chunk_size)
    ranges = num_ranges.times.collect do |n|
       (n * chunk_size) .. (n * chunk_size + chunk_size)
    end

    if num_ranges.zero?
      ranges << (0 .. partial_range)
    elsif partial_range.nonzero?
      ranges << (ranges.last.end .. ranges.last.end + partial_range)
    end

    ranges.collect { |range| Range.new(range.begin + from, range.end + from)  }
  end

  def partition_group(partition, group_size = 100)
    partition_index(partition).div(group_size)
  end

   def partition_group_path(partition, group_size = 100)
     dir = File.dirname(partition)
     file = File.basename(partition)
     File.join(dir, partition_group(partition, group_size).to_s, file)
  end
end
