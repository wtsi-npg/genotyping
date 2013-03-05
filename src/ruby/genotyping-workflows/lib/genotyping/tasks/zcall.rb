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

  # Returns true if the given script is available.
  def script_available?(script)
    system("which #{script} >/dev/null 2>&1")
  end

  def zcall_available?()
    script_available?(CALL)
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

    def evaluate_thresholds(threshold_json, sample_json, manifest, egt_file,
                             args = {}, async ={})
      # evaluate thresholds by batch processing, then merge evaluations
      args, work_dir, log_dir = process_task_args(args)
    end

    def run_zcall(thresholds, sample_json, manifest, egt_file,
                  args = {}, async ={})
      # run zcall on given thresholds and samples
      args, work_dir, log_dir = process_task_args(args)
    end

  end # module ZCall
end # module Genotyping::Tasks
