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
    # - dbfile (String): The SQLite database file name.
    # - input (Array): An Array of 3 filenames, being the Plink BED and
    #   corresponding BIM and FAM files.
    # - args (Hash): Arguments for the operation.
    #
    # - async (Hash): Arguments for asynchronous management.
    #
    # Returns:
    # - boolean
    def quality_control(dbfile, input, output, args = {}, async = {},
                        wait=false)
      args, work_dir, log_dir = process_task_args(args)

      if args_available?(dbfile, input, output, work_dir)
        unless (args.has_key?(:sim) && args[:sim].nil?)
          bedfile = input.first
          base = File.basename(bedfile, File.extname(bedfile))

          Dir.mkdir(output) unless File.exist?(output)

          cli_args = args.merge({:dbpath => dbfile,
                                 :output_dir => output})

          margs = [dbfile, input, output]

          command = [RUN_QC,
                     cli_arg_map(cli_args, :prefix => '--') { |key|
                       key.gsub(/_/, '-') }, base].flatten.join(' ')

          task_id = task_identity(:quality_control, *margs)
          log = File.join(log_dir, task_id + '.log')

          if wait # postcondition to check for completion of QC
            f =  File.join(work_dir, output, 'supplementary', 'finished.txt')
            finish = [ f, ]
            async_task(margs, command, work_dir, log,
                       :post => lambda {ensure_files(finish, :error => false)},
                       :result => lambda { true },
                       :async => async)
          else
            async_task(margs, command, work_dir, log,
                       :result => lambda { true },
                       :async => async)
          end
        end
      end
    end

  end
end
