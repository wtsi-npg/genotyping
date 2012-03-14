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

require 'fileutils'
require 'timeout'

module TestHelper
  def return_available?(value)
    case value
      when Array ; value.all?
      when NilClass ; nil
      else
        true
    end
  end

  def wait_for(name, timeout, interval, &test)
    result = nil

    memoizer = Percolate.memoizer
    asynchronizer = Percolate.asynchronizer
    asynchronizer.message_host = @msg_host
    asynchronizer.message_port = @msg_port
    asynchronizer.message_queue = name + '.' + $$.to_s

    Timeout.timeout(timeout) do
      until return_available?(result) do
        result = test.call
        memoizer.update_async_memos!
        sleep(interval)
        print('#')
      end
    end

    result
  end

  def complement(predicate = nil, &block)
    predicate ||= block
    lambda { |*args| !predicate.call(*args) }
  end

  def run_test_if(predicate, msg, &test)
    if predicate.call
      test.call
    else
      $stderr.puts(msg)
    end
  end

  def make_work_dir(name, dir)
    work_dir = File.join(dir, name + '.' + $$.to_s)
    unless File.directory?(work_dir)
      Dir.mkdir(work_dir)
    end

    work_dir
   end

  def remove_work_dir(dir)
    FileUtils.rm_r(dir)
  end
end
