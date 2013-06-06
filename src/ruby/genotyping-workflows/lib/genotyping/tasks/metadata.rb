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

  end #  module Metadata
end # module Genotyping::Tasks
