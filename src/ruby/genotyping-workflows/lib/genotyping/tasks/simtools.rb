#-- encoding: UTF-8
#
# Copyright (c) 2013 Genome Research Ltd. All rights reserved.
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

  SIMTOOLS = 'simtools'
  NORMALIZE_BPM = 'normalize_manifest'
  G2I = 'g2i'

  def simtools_available?()
    system("which #{SIMTOOLS} >/dev/null 2>&1")
  end

  def normalize_available?()
    system("which #{NORMALIZE_BPM} >/dev/null 2>&1")
  end

  def g2i_available?()
    system("which #{G2I} >/dev/null 2>&1")
  end

  module Simtools
    include Genotyping::Tasks

    # Collates intensity data from multiple GTC format files into a single SIM
    # format file with JSON annotation of chromosome boundaries.
    # Replaces GenotypeCall::gtc_to_sim
    #
    # Arguments:
    # - input (String): A JSON file specifying sample URIs and GTC file paths.
    # - manifest (String): The BeadPool manifest file name.
    # - output (String): The SIM file name.
    # - args (Hash): Arguments for the operation.
    #
    #   :normalize (Boolean): Normalize the intensities. Should be false for
    #   the GenoSNP caller.
    #
    # - async (Hash): Arguments for asynchronous management.
    #
    # Returns:
    #   - The SIM file path.
    def gtc_to_sim(input, manifest, output, args = {}, async = {})
      args, work_dir, log_dir = process_task_args(args)

      if args_available?(input, manifest, output, work_dir)
        output = absolute_path?(output) ? output : absolute_path(output, work_dir)
        expected = [output]
        
        cli_args = {:normalize => args[:normalize],
                    :infile => input,
                    :man_dir => manifest, # manifest path -- despite the name!
                    :outfile => output}

        margs = [cli_args, input, work_dir]
        task_id = task_identity(:gtc_to_sim, *margs)
        log = File.join(log_dir, task_id + '.log')

        command = [SIMTOOLS, 'create',
                   cli_arg_map(cli_args, :prefix => '--')].flatten.join(' ')
        # do not substitute dash - for underscore _ in man_dir argument

        async_task(margs, command, work_dir, log,
                   :post => lambda { ensure_files(expected, :error => false) },
                   :result => lambda { output },
                   :async => async)
      end
    end


    # normalize the .bpm.csv manifest to Illumina TOP strand
    #
    # Arguments:
    # - input (String): An un-normalized .bpm.csv manifest file
    # - output (String): Filename for the normalized output .bpm.csv
    # - args (Hash): Arguments for the operation.
    # - async (Hash): Arguments for asynchronous management.
    #
    # Returns:
    # - The normalized file path
    #
    def normalize_manifest(input, output, args = {}, async = {})
      args, work_dir, log_dir = process_task_args(args)
      if args_available?(input, output, work_dir)
        output = absolute_path?(output) ? output : absolute_path(output, work_dir)
        expected = [output]
        cli_args = {:infile => input,
                    :outfile => output}
        margs = [cli_args, input, work_dir]
        task_id = task_identity(:normalize_manifest, *margs)
        log = File.join(log_dir, task_id + '.log')
        
        command = [NORMALIZE_BPM, 'create',
                   cli_arg_map(cli_args, :prefix => '--')].flatten.join(' ')

        async_task(margs, command, work_dir, log,
                   :post => lambda { ensure_files(expected, :error => false)},
                   :result => lambda { output },
                   :async => async)
      end
    end



    # Collates GenCall genotype call data from multiple GTC format files into a
    # single Plink BED format file.
    #
    # Arguments:
    # - input (String): A JSON file specifying sample URIs and GTC file paths.
    # - manifest (String): The BeadPool manifest file name.
    # - output (String): The BED file name.
    # - args (Hash): Arguments for the operation.
    #
    # - async (Hash): Arguments for asynchronous management.
    #
    # Returns:
    # - An Array containing
    #   - The BED file path.
    def gtc_to_bed(input, manifest, output, args = {}, async = {})
      args, work_dir, log_dir = process_task_args(args)

      if args_available?(input, manifest, output, work_dir)
        output = absolute_path?(output) ? output : absolute_path(output, work_dir)
        expected = [output]

        cli_args = {:i => input,
                    :b => true,
                    :d => File.dirname(manifest),
                    :o => File.basename(output, '.bed')}

        margs = [cli_args, input, work_dir]
        task_id = task_identity(:g2i, *margs)
        log = File.join(log_dir, task_id + '.log')

        command = [G2I, cli_arg_map(cli_args, :prefix => '-') { |key|
          key.gsub(/_/, '-') }].flatten.join(' ')

        async_task(margs, command, work_dir, log,
                   :post => lambda { ensure_files(expected, :error => false) },
                   :result => lambda { output },
                   :async => async)
      end
    end

  end # module Simtools
end # module Genotyping::Tasks


