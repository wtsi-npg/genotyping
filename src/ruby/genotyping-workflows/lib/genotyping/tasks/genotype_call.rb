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

  GENOTYPE_CALL = 'genotype-call'

  def genotype_call_available?()
    system("which #{GENOTYPE_CALL} >/dev/null 2>&1")
  end

  def genotype_call_gtc_to_sim(gtc_files, manifest, output, args = {}, async = {})
    work_dir, log_dir = process_task_args(args)

    if args_available?(gtc_files, manifest, output, work_dir)
      unless absolute_path?(output)
        output = absolute_path(output, work_dir)
      end

      cli_args = {}
      cli_args[:chromosome] = args[:chromosome]
      cli_args[:manifest] = manifest
      cli_args[:output] = output

      margs = [work_dir, cli_args, gtc_files]
      task_id = task_identity(:genotype_call_gtc_to_sim, *margs)
      log = File.join(log_dir, task_id + '.log')

      command = [GENOTYPE_CALL, 'gtc-to-sim',
                 cli_arg_map(cli_args,
                             :prefix => '--'), *gtc_files].flatten.join(' ')

      async_task(margs, command, work_dir, log,
                 :post => lambda { ensure_files([output], :error => false) },
                 :result => lambda { output },
                 :async => async)
    end
  end
end
