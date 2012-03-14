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

  def process_task_args(args, defaults = {})
    args = defaults.merge(args)
    work_dir = args.delete(:work_dir)
    log_dir = args.delete(:log_dir)
    log_dir ||= work_dir

    [work_dir, log_dir]
  end

  # Returns an Array of Ranges which are indices for chunked
  # data processing. Given a total number of elements and a chunk size,
  # returns the relevant indices.
  def make_ranges(num_elementsl, chunk_size)
    num_ranges, partial_range = num_elementsl.divmod(chunk_size)
    ranges = num_ranges.times.collect do |n|
       1 + (n * chunk_size) .. (n * chunk_size + chunk_size)
    end

    if num_ranges.zero?
      ranges << (1 .. partial_range)
    elsif partial_range.nonzero?
      ranges << (1+ ranges.last.end .. ranges.last.end + partial_range)
    end

    ranges
  end
end
