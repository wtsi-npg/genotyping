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

module Genotyping::Tasks

  GENOSNP = 'GenoSNP'
  GENOSNP_WRAPPER = 'genosnp.pl'
  DEFAULT_CUTOFF = 0.7

  # Returns true if the GenoSNP executable is available.
  def genosnp_available?()
    system("which #{GENOSNP} >/dev/null 2>&1")
  end

  module GenoSNP
    include Genotyping
    include Genotyping::Tasks

    # Runs a GenoSNP analysis on intensity data for a range of samples in a SIM
    # file. The range of samples is broken into chunks of the specified size
    # which are run in parallel as batch jobs.
    #
    # Arguments:
    # - sim_file (String): The SIM file name.
    # - snp_meta (String): The GenoSNP SNP annotation file name.
    # - manifest (String): The BeadPool manifest file name.
    # - output (String): The output files base name.
    # - args (Hash): Arguments for the operation.
    #
    #   :start (Fixnum): The 0-based, half-open sample index at which to start.
    #   :end (Fixnum): The 0-based, half-open SNP index at which to finish.
    #   :size (Fixnum): The number of samples to process per chunk.
    #   :cutoff (Number): The GenoSNP cutoff argument.
    #   :plink (Boolean): Enable Plink BED format output.
    #   :debug (Boolean): tee the GenoSNP text input to a file while running.
    #
    # - async (Hash): Arguments for asynchronous management.
    #
    # Returns:
    # - An array of GenoSNP or Plink output file paths.
    def call_from_sim_p(sim_file, snp_json, manifest, output, args = {}, async = {})
      args, work_dir, log_dir = process_task_args(args)

      if args_available?(sim_file, snp_json, manifest , output, work_dir)
        output = absolute_path?(output) ? output : absolute_path(output, work_dir)
        start_sample = args[:start] || 0
        end_sample = args[:end]
        plink = args[:plink]
        cutoff = args[:cutoff] || DEFAULT_CUTOFF
        debug = args.delete(:debug)

        unless start_sample.is_a?(Fixnum)
          raise TaskArgumentError.new(":start must be an integer",
                                      :argument => :start, :value => start_sample)
        end
        unless end_sample.is_a?(Fixnum)
          raise TaskArgumentError.new(":end must be an integer",
                                      :argument => :end, :value => end_sample)
        end
        unless (0 <= start_sample) && (start_sample <= end_sample)
          raise TaskArgumentError.new(":start and :end must satisfy 0 <= :start <= :end")
        end

        chunk_size = args[:size] || (end_sample - start_sample)
        unless chunk_size.is_a?(Fixnum)
          raise TaskArgumentError.new(":size must be an integer",
                                      :argument => :size, :value => chunk_size)
        end

        group_size = args[:group_size] || DEFAULT_GROUP_SIZE

        sample_ranges = make_ranges(start_sample, end_sample, chunk_size)
        simtools_args = sample_ranges.collect do |range|
          {:infile => sim_file,
           :outfile => '-',
           :man_dir => manifest, # path to file, despite the name!
           :start => range.begin,
           :end => range.end}
        end

        genosnp_wrap_args = partitions(output, sample_ranges.size).collect do |part|
          grouped_part = partition_group_path(part, group_size)
          grouped_dir = File.dirname(grouped_part)
          Dir.mkdir(grouped_dir) unless File.exist?(grouped_dir)

          {:input => "/dev/stdin",
           :snps => snp_json,
           :cutoff => cutoff,
           :output => grouped_part,
           :plink => true}
        end

        commands = simtools_args.zip(genosnp_wrap_args).collect do |sta, gwa|
         cmd = [SIMTOOLS, 'genosnp', cli_arg_map(sta, :prefix => '--')]
         cmd += ['|', 'tee', gwa[:output] + '.raw.txt'] if debug
         cmd += ['|', GENOSNP_WRAPPER, cli_arg_map(gwa, :prefix => '--')]
         cmd.flatten.join(' ')
        end

        # Job memoization keys, i corresponds to the partition index
        margs_arrays = simtools_args.zip(genosnp_wrap_args).collect { |sta, gwa|
          [work_dir, sta, gwa]
        }.each_with_index.collect { |elt, i| [i] + elt }

        # Expected call files
        suffix = plink ? '.bed' : ''
        call_partitions = genosnp_wrap_args.collect { |gwa| gwa[:output] + suffix }

        task_id = task_identity(:genosnp_from_sim_p, *margs_arrays)
        log = File.join(log_dir, task_id + '.%I.log')

        async_task_array(margs_arrays, commands, work_dir, log,
                         :post => lambda { |i| ensure_files([call_partitions[i]],
                                                            :error => false)},
                         :result => lambda { |i| call_partitions[i] },
                         :async => async)
      end
    end

  end # module GenoSNP
end # module Genotyping::Tasks
