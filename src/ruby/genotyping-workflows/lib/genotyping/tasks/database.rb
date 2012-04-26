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

  SAMPLE_INTENSITIES = 'sample_intensities.pl'

  module Database
    include Genotyping::Tasks

    # Extracts sample intensity file information from the pipeline database and
    # writes it to a file in JSON format.
    #
    # Arguments:
    # - dbfile (String): The SQLite database file name.
    # - run_name (String): The analysis run name as given in the pipeline
    #   database.
    # - output (String): The JSON file name.
    # - args (Hash): Arguments for the operation. Currently none.
    #
    # Returns:
    # - The SIM file path.
    def sample_intensities(dbfile, run_name, output, args = {})
      args, work_dir, log_dir = process_task_args(args)

      if args_available?(dbfile, run_name, output, work_dir)
        output = absolute_path(output, work_dir) unless absolute_path?(output)
        cli_args = args.merge({:dbfile => dbfile,
                               :run => run_name,
                               :output => output})
        margs = [cli_args, work_dir]

        command = [SAMPLE_INTENSITIES,
                   cli_arg_map(cli_args, :prefix => '--')].flatten.join(' ')

        task(margs, command, work_dir,
             :post => lambda { ensure_files([output], :error => false) },
             :result => lambda { output })
      end
    end

  end
end
