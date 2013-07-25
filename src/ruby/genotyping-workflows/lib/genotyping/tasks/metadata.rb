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
  PARSE_MANIFEST = 'write_snp_metadata.pl'
  MERGE_QC_RESULTS = 'merge_qc_results.pl'
  FILTER_SAMPLES = 'filter_samples.pl'

  module Metadata
    include Genotyping::Tasks

    # Extracts sample intensity file information from the pipeline database and
    # writes it to a file in JSON format.
    #
    # Arguments:
    # - dbfile (String): The SQLite database file name.
    # - run_name (String): The analysis run name as given in the pipeline
    #   database.
    # - output (String): The JSON file name.
    # - args (Hash): Arguments for the operation.
    #
    #   :gender_method (String): The gender determination method name given
    #   in genders.ini
    #
    # Returns:
    # - The result file path.
    def sample_intensities(dbfile, run_name, output, args = {})
      args, work_dir, log_dir = process_task_args(args)

      if args_available?(dbfile, run_name, output, work_dir)
        output = absolute_path?(output) ? output : absolute_path(output, work_dir)
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

    # Parses the .bpm.csv SNP manifest and writes two files in JSON format:
    # - Chromosome boundaries with respect to position in the (sorted) manifest
    # - SNP information: name, chromosome, position, alleles, normid
    #
    # Arguments:
    # - manifest (String): The manifest file name.
    # - chromosome (String): Filename for JSON chromosome boundaries
    # - snp (String): Filename for JSON SNP information
    # - args (Hash): Arguments for the operation.
    #
    # Returns:
    # - Array containing the (chromosome, snp) file paths
    def parse_manifest(manifest, snp, chromosomes, args = {})
      args, work_dir, log_dir = process_task_args(args)

      if args_available?(manifest, snp, chromosomes, work_dir)

        snp_path = absolute_path?(snp) ? snp : 
          absolute_path(snp, work_dir)
        chr_path = absolute_path?(chromosomes) ? chromosomes : 
          absolute_path(chromosomes, work_dir)

        cli_args = args.merge({:manifest => manifest,
                                :snp => snp_path,
                                :chromosomes => chr_path})
        margs = [cli_args, work_dir]

        command = [PARSE_MANIFEST,
                   cli_arg_map(cli_args, :prefix => '--')].flatten.join(' ')

        expected = [snp_path, chr_path]
        task(margs, command, work_dir,
             :post => lambda { ensure_files(expected, :error => false) },
             :result => lambda { expected })
      end
    end

    # merge .json summary files from QC output
    # use to combine generic QC with extra MAF/het check
    #
    # Arguments:
    # - results (Array): Array containing two paths to results .json files
    # - outfile (String): Path to .json output file
    #
    # (if required, could extend to recursively merge more than two files)

    def merge_qc_results(results, outfile, args = {})
      args, work_dir, log_dir = process_task_args(args)

      if args_available?(results, outfile, work_dir)
        if results.length!=2
          raise ArgumentError "Must have exactly 2 .json result files to merge"
        end
        input1 = results[0]
        input2 = results[1]
        cli_args = args.merge({:input1 => results[0],
                               :input2 => results[1],
                               :output => outfile})
        margs = [cli_args, work_dir]

        command = [MERGE_QC_RESULTS,
                   cli_arg_map(cli_args, :prefix => '--')].flatten.join(' ')

        task(margs, command, work_dir,
             :post => lambda { ensure_files([outfile,], :error => false) },
             :result => lambda { outfile })
      end
    end

    # filter out samples which fail QC criteria
    # apply thresholds and flag failing samples for exclusion in pipeline DB
    #
    # Arguments:
    # - in (String): Path to .json QC results file
    # - db (String): Path to pipeline SQLite DB file
    # - log (String): Path to log file with summary of pass/fail rates
    # - out (String): Path to .json file with detailed pass/fail by sample 
    #   and metric
    # - thresholds (String): Path to .json file with custom thresholds
    # - default (String): One of "zcall" or "illuminus"; applies default 
    #   thresholds
    #
    # Exactly one of (thresholds, default) must be specified.

    def filter_samples(results, dbfile, args={})

      args, work_dir, log_dir = process_task_args(args)
      if args_available?(results, dbfile, work_dir, log_dir)
        args[:out] ||= File.join(work_dir, 'prefilter_results.json')
        logpath = File.join(log_dir, 'prefilter.log')
        thresholds = args.delete(:thresholds) || nil
        illuminus = args.delete(:illuminus) || nil
        zcall = args.delete(:zcall) || nil
        cli_args = args.merge({:in => results,
                               :db => dbfile,
                               :log => logpath})
        if thresholds and (!zcall) and (!illuminus) 
          cli_args[:thresholds] = thresholds
        elsif zcall and (!thresholds) and (!illuminus)
          cli_args[:zcall] = true
        elsif illuminus and (!thresholds) and (!zcall)
          cli_args[:illuminus] = true
        else
          raise ArgumentError, "Invalid arguments to filter_samples. Must specify exactly one of: thresholds=PATH, illuminus, zcall"
        end
        margs = [results, dbfile, work_dir]
        task_id = task_identity(:filter_samples, *margs)
        command = [FILTER_SAMPLES,  
                   cli_arg_map(cli_args, :prefix => '--') { |key|
                     key.gsub(/_/, '-') }].flatten.join(' ')
        log = File.join(log_dir, task_id + '.log')
        task(margs, command, work_dir,
             :result => lambda { true })
      end
    end


  end #  module Metadata
end # module Genotyping::Tasks
