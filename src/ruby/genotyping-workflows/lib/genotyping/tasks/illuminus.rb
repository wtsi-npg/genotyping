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

  ILLUMINUS = 'illuminus'
  ILLUMINUS_WRAPPER = 'illuminus.pl'

  def illuminus_available?()
    system("which #{ILLUMINUS} >/dev/null 2>&1")
  end

  module Illuminus
    include Genotyping
    include Genotyping::Tasks

    ## We need a way of producing a tree of directories for chunked parallel jobs
    ## to run in, so that only 100-ish files are in each directory.
    #
    # E.g. 10000 parallel jobs:
    #
    #  lv1 10 dirs x lv2 10 dirs x 100 files
    #
    # You can calculate the file bin (directory) from the job index

    def call_from_sim_p(sim_file, manifest, names, output, args = {}, async = {})
      work_dir, log_dir = process_task_args(args)

      if args_available?(sim_file, manifest, names, output, work_dir)
        unless absolute_path?(output)
          output = absolute_path(output, work_dir)
        end

        #start_snp = args[:start] || 0
        #end_snp = args[:end]
        #unless start_snp.is_a?(Fixnum)
        #  raise TaskArgumentError.new(":start must be an integer",
        #                              :argument => :start, :value => start_snp)
        #end
        #unless end_snp.is_a?(Fixnum)
        #  raise TaskArgumentError.new(":end must be an integer",
        #                              :argument => :end, :value => end_snp)
        #end
        #unless (0 <= start_snp) && (start_snp <= end_snp)
        #  raise TaskArgumentError.new(":start and :end must satisfy 0 <= :start <= :end")
        #end

        chunk_size = args[:size] || (end_snp - start_snp)
        unless chunk_size.is_a?(Fixnum)
          raise TaskArgumentError.new(":size must be an integer",
                                      :argument => :size, :value => chunk_size)
        end

        # Calculate the chunks from start/end/chunk-size



        async_task_array(margs_arrays, commands, work_dir, log, args = {})
      end
    end

    def call_from_sim(sim_file, manifest, names, output, args = {}, async = {})
      work_dir, log_dir = process_task_args(args)

      if args_available?(sim_file, manifest, names, output, work_dir)
        unless absolute_path?(output)
          output = absolute_path(output, work_dir)
        end

        start_snp = args[:start] || 0
        end_snp = args[:end]

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

        genotype_call_args = {:input => sim_file,
                              :output => 'stdout',
                              :manifest => manifest,
                              :start => start_snp,
                              :end => end_snp}

        illuminus_wrap_args = {:columns => names,
                               :output => output}

        command = [GENOTYPE_CALL, 'sim-to-illuminus',
                   cli_arg_map(genotype_call_args,
                               :prefix => '--'), '|', ILLUMINUS_WRAPPER,
                   cli_arg_map(illuminus_wrap_args,
                               :prefix => '--')].flatten.join(' ')

        margs = [work_dir, genotype_call_args, illuminus_wrap_args]
        task_id = task_identity(:illuminus_sim_to_gcf, *margs)
        log = File.join(log_dir, task_id + '.log')

        async_task(margs, command, work_dir, log,
                   :post => lambda { ensure_files([output], :error => false) },
                   :result => lambda { output },
                   :async => async)
      end
    end

  end

end
