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

  # The default value for the number of file partitions to create in a single
  # directory
  DEFAULT_GROUP_SIZE = 100

  # Returns args after ensuring that only elements with keys in the expected
  # Array are present.
  def ensure_valid_args(args, *valid_keys)
    unless args.is_a?(Hash)
      raise ArgumentError, "invalid args '#{args.inspect}'; must be a Hash"
    end

    invalid = args.reject { |key, val| valid_keys.include?(key) }
    unless invalid.empty?
      raise ArgumentError, "invalid arguments: #{invalid.inspect}"
    end

    args
  end

  # Merges Hash args with defaults and then deletes from arg the elements with
  # the keys :work_dir and :log_dir. Returns an Array of 3 elements: args,
  # the :work_dir value and the :log_dir value.
  def process_task_args(args, defaults = {})
    unless args.is_a?(Hash)
      raise ArgumentError, "invalid args '#{args.inspect}'; must be a Hash"
    end

    args = defaults.merge(args)
    work_dir = args.delete(:work_dir)
    log_dir = args.delete(:log_dir)
    log_dir ||= work_dir

    [args, work_dir, log_dir]
  end

  def lsf_args(args, lsf_defaults, *lsf_keys)
    unless args.is_a?(Hash)
      raise ArgumentError, "invalid args '#{args.inspect}'; must be a Hash"
    end

    lsf_args = args.reject { |k, v| !lsf_keys.include?(k) }
    lsf_defaults = lsf_defaults.merge(lsf_args)
    queue = lsf_defaults[:queue]

    if queue && queue.respond_to?(:intern)
      lsf_defaults[:queue] = queue.intern
    end

    lsf_defaults
  end

  # Returns work_dir if it is an absolute directory path (the criteria for a
  # valid working directory) or raises an error.
  def ensure_valid_work_dir(work_dir)
    unless work_dir.is_a?(String)
      raise ArgumentError, "invalid work_dir '#{work_dir}'; must be a String"
    end

    unless absolute_path?(work_dir) && File.directory?(work_dir)
      raise ArgumentError,
            "invalid work_dir '#{work_dir}'; must be an absolute directory path"
    end

    work_dir
  end

  # Returns work_dir, ensuring that it is either valid or nil.
  def maybe_work_dir(work_dir)
    if work_dir.nil?
      work_dir
    else
      ensure_valid_work_dir(work_dir)
    end
  end

  def change_extname(file_name, ext)
    dir = File.dirname(file_name)
    base = File.basename(file_name, File.extname(file_name))

    if base.start_with?(".")
      raise ArgumentError,
            "invalid file name '#{file_name}'; cannot change the extname of dotfiles"
    end

    if dir == '.'
      base + ext
    else
      File.join(dir, base) + ext
    end
  end

  # Returns an Array of Ranges which are indices for chunked
  # data processing. Given a start index, end index and a chunk size,
  # returns the relevant indices.
  # 
  # Output ranges are zero-based, closed indices. 
  # For example, with arguments (0,6,3), outputs are [(0,3), (4,6)].
  # This is consistent with the indexing convention used by simtools.
  # IMPORTANT: The evaluate_samples method in zcall.rb uses an alternative convention of zero-based, left-closed, right-open indices. So with arguments (0,6,3), the correct ranges for evaluate_samples are [(0,4), (4,7)]. Therefore, the evaluate_samples method adds 1 to the endpoint of each range.
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

    ranges.collect { 
      |range| Range.new(range.begin + from, range.end + from -1)  
    }
      
  end

  # Returns the partition group to which partition belongs (based on its index),
  # given a group size.
  def partition_group(partition, group_size = 100)
    partition_index(partition).div(group_size)
  end

  # Returns the root path of a partition group, given a group size. All the
  # partitions in that group should be placed in that directory.
  def partition_group_path(partition, group_size = 100)
    dir = File.dirname(partition)
    file = File.basename(partition)
    File.join(dir, partition_group(partition, group_size).to_s, file)
  end

  def intern_keys(hash)
    pairs = hash.collect do |key, value|
      new_key = key.respond_to?(:intern) ? key.intern : key
      [new_key, value]
    end

    Hash[pairs]
  end

end
