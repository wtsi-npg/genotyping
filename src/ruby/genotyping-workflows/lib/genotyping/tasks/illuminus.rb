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

  ILLUMINUS = 'illuminus'
  ILLUMINUS_WRAPPER = 'illuminus.pl'

  # Returns true if the Illuminus executable is available.
  def illuminus_available?()
    system("which #{ILLUMINUS} >/dev/null 2>&1")
  end

  module Illuminus
    include Genotyping
    include Genotyping::Tasks

    # Runs an Illuminus analysis on intensity data for a range of SNPs in a SIM
    # file. The range (e.g. spanning a chromsome) is broken into chunks of equal
    # size which are run in parallel as batch jobs.
    #
    # Arguments:
    # - sim_file (String): The SIM file name.
    # - sample_json (String): The sample JSON file name.
    # - manifest (String): The BeadPool manifest file name.
    # - output (String): The output files base name.
    # - args (Hash): Arguments for the operation.
    #
    #   :chromosome (String): The chromosome being worked on. Optional,
    #   used to indicate X, Y or mitochondrial chromosomes.
    #   :wga (boolean): Assume that the sample is whole-genome amplified.
    #   :start (Fixnum): The 0-based, half-open SNP index at which to start.
    #   :end (Fixnum): The 0-based, half-open SNP index at which to finish.
    #   :size (Fixnum): The number of SNPs in the range to process in on job.
    #   :plink (Boolean): Enable Plink BED format output.
    #   :debug (Boolean): tee the Illuminus text input to a file while running.
    #
    # - async (Hash): Arguments for asynchronous management.
    #
    # Returns:
    # - An array of Illuminus or Plink output file paths.
    def call_from_sim_p(sim_file, sample_json, manifest, output, args = {}, async = {})
      args, work_dir, log_dir = process_task_args(args)
      chromosome = args[:chromosome]

      if args_available?(sim_file, sample_json, manifest, chromosome , output, work_dir)
        output = absolute_path?(output) ? output : absolute_path(output, work_dir)
        start_snp = args[:start] || 0
        end_snp = args[:end]
        wga = args[:wga]
        plink = args[:plink]
        debug = args.delete(:debug)

        unless start_snp.is_a?(Fixnum)
          raise TaskArgumentError.new(":start must be an integer",
                                      :argument => :start, :value => start_snp)
        end
        unless end_snp.is_a?(Fixnum)
          raise TaskArgumentError.new(":end must be an integer",
                                      :argument => :end, :value => end_snp)
        end
        unless (0 <= start_snp) && (start_snp <= end_snp)
          raise TaskArgumentError.new(":start and :end must satisfy 0 <= :start <= :end")
        end

        chunk_size = args[:size] || (end_snp - start_snp)
        unless chunk_size.is_a?(Fixnum)
          raise TaskArgumentError.new(":size must be an integer",
                                      :argument => :size, :value => chunk_size)
        end

        group_size = args[:group_size] || DEFAULT_GROUP_SIZE

        snp_ranges = make_ranges(start_snp, end_snp, chunk_size)
        simtools_args = snp_ranges.collect do |range|
          {:infile => sim_file,
           :outfile => '-',
           :man_dir => manifest, # path to file, despite the name!
           :start => range.begin,
           :end => range.end }
        end

        illuminus_wrap_args = partitions(output, snp_ranges.size).collect do |part|
          grouped_part = partition_group_path(part, group_size)
          grouped_dir = File.dirname(grouped_part)
          Dir.mkdir(grouped_dir) unless File.exist?(grouped_dir)

          {:chr => chromosome,
           :samples => sample_json,
           :wga => wga,
           :output => grouped_part,
           :plink => plink,
           :verbose => false}
        end

        commands = simtools_args.zip(illuminus_wrap_args).collect do |gca, iwa|
          cmd = [SIMTOOLS, 'illuminus',
                 cli_arg_map(gca, :prefix => '--')]
          cmd += ['|', 'tee', iwa[:output] + '.iln'] if debug
          cmd += ['|', ILLUMINUS_WRAPPER, cli_arg_map(iwa, :prefix => '--')]
          cmd.flatten.join(' ')
        end

        # Job memoization keys, i corresponds to the partition index
        margs_arrays = simtools_args.zip(illuminus_wrap_args).collect { |gca, iwa|
          [work_dir, gca, iwa]
        }.each_with_index.collect { |elt, i| [i] + elt }

        # Expected call files
        suffix = plink ? '.bed' : ''
        call_partitions = illuminus_wrap_args.collect { |iwa| iwa[:output] + suffix }

        task_id = task_identity(:illuminus_from_sim_p, *margs_arrays)
        log = File.join(log_dir, task_id + '.%I.log')

        async_task_array(margs_arrays, commands, work_dir, log,
                         :post => lambda { |i| ensure_files([call_partitions[i]],
                                                            :error => false)},
                         :result => lambda { |i| call_partitions[i] },
                         :async => async)
      end
    end

  end # module Illuminus
end # module Genotyping::Tasks
