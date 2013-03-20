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

  PREPARE = 'prepareThresholds.py'
  EVALUATE = 'evaluateThresholds.py'
  MERGE = 'mergeEvaluation.py'
  CALL = 'runZCall.py'
  METRIC_INDEX = 'evaluation_metric_index.json'
  MERGED_EVALUATION = 'merged_z_evaluation.json'

  # Returns true if the given script is available.
  def script_available?(script)
    system("which #{script} >/dev/null 2>&1")
  end

  def zcall_available?()
    script_available?(CALL)
  end

  def zcall_prepare_available?()
    script_available?(PREPARE)
  end

def zcall_evaluate_available?()
    script_available?(MERGE) and script_available?(EVALUATE)
  end

  module ZCall
    include Genotyping
    include Genotyping::Tasks

    # Runs ZCall re-calling on the GTC files output by another caller 
    # (typically Illumina's GenCall software).  The range of samples is 
    # broken into chunks of the specified size which are run in parallel 
    # as batch jobs.
    #
    # Zcall runs in three steps:
    # 1) Prepare thresholds: Find thresholds for each SNP, for range of z scores
    # 2) Evaluate thresholds: Find which threshold gives best results, for
    # samples which pass QC criteria on previous caller
    # 3) Call: Apply the ZCall algorithm with the 'best' thresholds 
    # identified in step (2), to re-call any 'no-calls' in the GTC input, 
    # writing output in Plink .bed format.
    #
    # Steps (2) and (3) use batch processing, with results merged after batch 
    # completion.
    #
    # TODO
    # Use sample_intensities.pl to create .json file listing input GTC files
    # Split GTC files into chunks, use batch processing for steps 2 and 3

    ######################################################################

    # Prepares threshold.txt files for zcall
    #
    # Arguments:
    # - egt_file (String): Path to binary .egt file with expected cluster data
    # - zstart (Fixnum): Minimum z score
    # - ztotal (Fixnum): Total number of integer z scores
    # - args (Hash): Arguments for the operation.
    # - async (Hash): Arguments for asynchronous management.
    #
    # Returns:
    # - An Array containing:
    #  - threshold index (JSON) file name, giving paths to thresholds by zscore.
    #  - Array of threshold.txt file names.
    def prepare_thresholds(egt_file, zstart, ztotal, args = {}, async ={})
      args, work_dir, log_dir = process_task_args(args)

      if args_available?(egt_file, zstart, ztotal)
        
        cli_args = {:egt => egt_file,
                    :out => work_dir,
                    :zstart => zstart,
                    :ztotal => ztotal}
        margs = [cli_args, work_dir]
        task_id = task_identity(:prepare_thresholds, *margs)
        log = File.join(log_dir, task_id + '.log')

        command = [PREPARE, 
                   cli_arg_map(cli_args,
                               :prefix => '--')].flatten.join(' ')
        threshold_json = File.join(work_dir, 'threshold_index.json')
        threshold_text = []
        zscores = [6,7,8]
        zscores.each do |z|
          t = 'thresholds_HumanExome-12v1_z0'+z.to_s()+'.txt'
          threshold_text.push(File.join(work_dir, t))
        end
        expected = [threshold_json, threshold_text].flatten

        async_task(margs, command, work_dir, log,
                   :post => lambda { ensure_files(expected, :error => false) },
                   :result => lambda { [threshold_json, threshold_text] },
                   :async => async)
      end
    end

    private # private methods for internal housekeeping
    # Generate path for evaluation chunk .json output
    def get_evaluation_path(work_dir, i)
      name = "evaluation_part_%03d.json" % i # pad with leading zeroes
      File.join(work_dir, name)
    end

    # Validate start/end points and return sample range
    def get_sample_ranges(start_sample, end_sample, args)
      unless start_sample.is_a?(Fixnum)
        raise TaskArgumentError.new(":start must be an integer",
                                    :argument => :start, :value => start_sample)
      end
      unless end_sample.is_a?(Fixnum)
        raise TaskArgumentError.new(":end must be an integer",
                                    :argument => :end, :value => end_sample)
      end
      unless (0 <= start_sample) && (start_sample <= end_sample)
        raise TaskArgumentError.new(":start and :end must satisfy 0 <= :start <= :end")
      end
      
      chunk_size = args[:size] || (end_sample - start_sample)
      unless chunk_size.is_a?(Fixnum)
        raise TaskArgumentError.new(":size must be an integer",
                                    :argument => :size, :value => chunk_size)
      end

      sample_ranges = make_ranges(start_sample, end_sample, chunk_size)
      return chunk_size, sample_ranges
    end
    public # end of private methods

    # Evaluate thresholds by batch processing, then merge evaluations
    def evaluate_thresholds(threshold_json, sample_json, manifest, egt_file,
                            args = {}, async ={})
      args, work_dir, log_dir = process_task_args(args)
      if args_available?(threshold_json, sample_json, manifest, egt_file)
        start_sample = args[:start] || 0
        end_sample = args[:end]
        chunk_size, sample_ranges = 
          get_sample_ranges(start_sample, end_sample, args)
        ### construct arguments for job array
        temp_dir = File.join(work_dir, 'evaluation_temp')
        Dir.mkdir(temp_dir) unless File.exist?(temp_dir)
        evaluation_args = sample_ranges.each_with_index.collect do |range, i|
          {:thresholds => threshold_json, :bpm => manifest, :egt => egt_file,
            :gtc => sample_json, :start => range.begin, :end => range.end,
            :out => get_evaluation_path(temp_dir, i)  
          }
        end
        out_paths = evaluation_args.collect do |args|
          args[:out]
        end
        index_path = File.join(work_dir, METRIC_INDEX)
        unless File.exists?(index_path)
          JSON.dump(out_paths, File.new(index_path, mode='w'))
        end
        commands = evaluation_args.collect do |args| 
          cmd = [EVALUATE, cli_arg_map(args, :prefix => '--')]
          cmd.join(' ')
        end

        margs_arrays = evaluation_args.collect { | args |
          [work_dir, args]
        }.each_with_index.collect { |elt, i| [i] + elt }
        task_id = task_identity(:evaluate_thresholds, *margs_arrays)
        log = File.join(log_dir, task_id + '.%I.log')
        async = async_task_array(margs_arrays, commands, work_dir, log,
                                 :post => lambda { 
                                   ensure_files(out_paths, :error => false)
                                 },
                                 :result => lambda { index_path },
                                 :async => async)
        if async.include?(nil)
          result = nil
        else
          result = index_path
        end
        return result
      end
    end

    def merge_evaluation(metrics_path, thresholds_path, args = {}, async ={})
      args, work_dir, log_dir = process_task_args(args)
      if args_available?(metrics_path)
        out_path = File.join(work_dir, MERGED_EVALUATION)
        cli_args = {
          :metrics => metrics_path,
          :thresholds => thresholds_path,
          :out => out_path}
        margs = [cli_args, work_dir]
        task_id = task_identity(:merge_evaluation, *margs)
        log = File.join(log_dir, task_id + '.log')
        command = [MERGE, 
                   cli_arg_map(cli_args,
                               :prefix => '--')].flatten.join(' ')
        async_task(margs, command, work_dir, log,
                   :post => lambda {ensure_files([out_path,])},
                   :result => lambda { out_path },
                   :async => async)
      end

    end

    def run_zcall(thresholds, sample_json, manifest, egt_file, bed_path,
                  args = {}, async ={})
      # run zcall on given thresholds and samples
      args, work_dir, log_dir = process_task_args(args)
      if args_available?(thresholds, sample_json, manifest, egt_file, bed_path)
        
        ## TODO modify to split samples into chunks and use job array

        cli_args = {
          :thresholds => thresholds,
          :bpm => manifest,
          :egt => egt_file,
          :gtc => sample_json,
          :out => bed_path
        }
        margs = [cli_args, work_dir]
        task_id = task_identity(:merge_evaluation, *margs)
        log = File.join(log_dir, task_id + '.log')
        command = [CALL, 
                   cli_arg_map(cli_args,
                               :prefix => '--')].flatten.join(' ')
        async_task(margs, command, work_dir, log,
                   :post => lambda {ensure_files([bed_path,])},
                   :result => lambda { bed_path },
                   :async => async)

      end
    end
    
  end # module ZCall
end # module Genotyping::Tasks
