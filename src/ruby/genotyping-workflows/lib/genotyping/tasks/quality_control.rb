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
  CHECK_IDENTITY_BED = 'check_identity_bed.pl'

  module QualityControl
    include Genotyping::Tasks

    # Runs quality control on genotype call results.
    # With appropriate args, will exclude failing samples from pipeline DB.
    #
    # Arguments:
    # - dbfile (String): The SQLite database file name.
    # - input (Array): An Array of 3 filenames, being the Plink BED and
    #   corresponding BIM and FAM files.
    # - plex_manifest (String): Path to the QC plex manifest file.
    # - args (Hash): Arguments for the operation.
    #
    # - async (Hash): Arguments for asynchronous management.
    #
    # Returns:
    # - boolean
    def quality_control(dbfile, input, output,
                        args = {}, async = {}, wait=false)
      args, work_dir, log_dir = process_task_args(args)

      if args_available?(dbfile, input, output, work_dir)
        unless (args.has_key?(:sim) && args[:sim].nil?)
          bedfile = input.first
          base = File.basename(bedfile, File.extname(bedfile))

          Dir.mkdir(output) unless File.exist?(output)

          cli_args = args.merge({:dbpath => dbfile,
                                 :output_dir => output,
                                 :plink => base})
          
          margs = [dbfile, input, output]

          command = [RUN_QC,
                     cli_arg_map(cli_args, :prefix => '--') { |key|
                       key.gsub(/_/, '-') }].flatten.join(' ')

          task_id = task_identity(:quality_control, *margs)
          log = File.join(log_dir, task_id + '.log')

          if wait # postcondition to check for completion of QC
            f =  File.join(output, 'supplementary', 'finished.txt')
            async_task(margs, command, work_dir, log,
                       :post => lambda {ensure_files([f,], :error => false)},
                       :result => lambda { true },
                       :async => async)
          else
            async_task(margs, command, work_dir, log,
                       :result => lambda { true },
                       :async => async)
          end
        end
      end
    end # quality_control


    # Runs the identity check stand-alone on genotype call results.
    #
    # Arguments:
    # - dbfile (String): The SQLite database file name.
    # - input (String): Path to the Plink BED file; corresponding BIM, FAM files are assumed to be present in the same directory
    # - output (String): Path to directory to write results
    # - args (Hash): Arguments for the operation.
    # - async (Hash): Arguments for asynchronous management.
    #
    # Returns:
    # - boolean
    #
    # NOTE: This is the old version of the identity check. Calls are read
    # from the SQLite pipeline database instead of a VCF file.
    #
    def check_identity(dbfile, input, output, args = {}, async = {})
      
      args, work_dir, log_dir = process_task_args(args)

      if args_available?(dbfile, input, output, work_dir)

        base = File.basename(input, File.extname(input))
        plink = File.join(File.dirname(input), base)
        Dir.mkdir(output) unless File.exist?(output)

        cli_args = args.merge({:db => dbfile,
                               :plink => plink,
                               :outdir => output})
        margs = [dbfile, input, output]
        

        command = [CHECK_IDENTITY_BED,
                   cli_arg_map(cli_args, :prefix => '--')].flatten.join(' ')
        task_id = task_identity(:check_identity, *margs)
        log = File.join(log_dir, task_id + '.log')
        
        result_names = ['identity_check_failed_pairs.txt',
                        'identity_check_gt.txt',
                        'identity_check.json',
                        'identity_check_results.txt'
                       ]
        results = Array.new
        result_names.each{ |name| results.push(File.join(output, name)) }

        async_task(margs, command, work_dir, log,
                   :post => lambda {ensure_files(results, :error => false)},
                   :result => lambda { true },
                   :async => async)

      end

    end # check_identity

  end
end
