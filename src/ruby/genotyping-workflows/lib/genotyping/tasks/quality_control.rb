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

  RUN_QC = 'run_qc.pl'

  module QualityControl
    include Genotyping::Tasks

    # Runs quality control on genotype call results.
    #
    # Arguments:
    # - input (Array): An Array of 3 filenames, being the Plink BED and
    #   corresponding BIM and FAM files.
    # - args (Hash): Arguments for the operation.
    #
    # - async (Hash): Arguments for asynchronous management.
    #
    # Returns:
    # - boolean
    def quality_control(input, args = {}, async = {})
      args, work_dir, log_dir = process_task_args(args)

      if args_available?(input, work_dir)
        bedfile = input.first
        base = File.basename(bedfile, File.extname(bedfile))

        qc_dir = File.join(work_dir, 'qc')
        Dir.mkdir(qc_dir) unless File.exist?(qc_dir)

        cli_args = args.merge({:output_dir => qc_dir})
        margs = [cli_args, base]

        command = [RUN_QC,
                   cli_arg_map(cli_args, :prefix => '--') { |key|
                     key.gsub(/_/, '-') }, base].flatten.join(' ')

        task_id = task_identity(:quality_control, *margs)
        log = File.join(log_dir, task_id + '.%I.log')

        async_task(margs, command, work_dir, log,
                   :result => lambda { true },
                   :async => async)
      end
    end

  end
end
