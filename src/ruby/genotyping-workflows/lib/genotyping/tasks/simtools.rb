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

  def simtools_available?()
    system("which #{SIMTOOLS} >/dev/null 2>&1")
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
    # - An Array containing
    #   - The SIM file path.
    #   - The JSON chromosome annotation path.
    def gtc_to_sim(input, manifest, output, args = {}, async = {})
      args, work_dir, log_dir = process_task_args(args)

      if args_available?(input, manifest, output, work_dir)
        output = absolute_path?(output) ? output : absolute_path(output, work_dir)
        expected = [output]

        cli_args = {:normalize => args[:normalize],
                    :infile => input,
                    :man_dir => manifest, # manifest path -- despite the name!
                    :outfile => output}

        if args.has_key?(:chromosome_meta)
          chr_json = args[:chromosome_meta]
          chr_json = absolute_path(chr_json, work_dir) unless absolute_path?(chr_json)
          cli_args[:chromosome_meta] = chr_json
          expected << chr_json
        end

        if args.has_key?(:snp_meta)
          snp_json = args[:snp_meta]
          snp_json = absolute_path(snp_json, work_dir) unless absolute_path?(snp_json)
          cli_args[:snp_meta] = snp_json
          expected << snp_json
        end

        margs = [cli_args, input, work_dir]
        task_id = task_identity(:gtc_to_sim, *margs)
        log = File.join(log_dir, task_id + '.log')

        command = [SIMTOOLS, 'create',
                   cli_arg_map(cli_args, :prefix => '--')].flatten.join(' ')
        # do not substitute dash - for underscore _ in man_dir argument

        async_task(margs, command, work_dir, log,
                   :post => lambda { ensure_files(expected, :error => false) },
                   :result => lambda { expected },
                   :async => async)
      end
    end


  end # module Simtools
end # module Genotyping::Tasks


