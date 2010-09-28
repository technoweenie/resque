require 'digest/md5'
require 'resque/failure/redis'
module Resque
  module Failure
    # A Failure backend that stores exceptions in a Redis hash. Very simple but
    # only works on Redis 2.0+.
    class RedisHash < Resque::Failure::Redis
      def save
        data = {
          :failed_at => Time.now.strftime("%Y/%m/%d %H:%M:%S"),
          :payload   => payload,
          :exception => exception.class.to_s,
          :error     => exception.to_s,
          :backtrace => Array(exception.backtrace),
          :worker    => worker.to_s,
          :queue     => queue
        }
        data = Resque.encode(data)
        id   = Digest::MD5.hexdigest(data+Time.now.usec.to_s)
        Resque.redis.pipelined do
          Resque.redis.hset(:failed_jobs, id, data)
          Resque.redis.rpush(:failed,     id)
        end
      end

      def self.all(start = 0, count = 1)
        if count == 1
          id   = Resque.redis.lindex(:failed, start)
          item = Resque.redis.hget(:failed_jobs, id)
          Resque.decode(item).update('failure_id' => id)
        else
          ids = Resque.redis.lrange(:failed, start, start+count-1)
          # HMGET is being a jerk.  Investigate!
          #items = Resque.redis.hmget(:failed_jobs, *ids)
          ids.map! do |id|
            item = Resque.redis.hget(:failed_jobs, id)
            Resque.decode(item).update('failure_id' => id)
          end
        end
      end

      def self.requeue(index)
        item = all(index)
        id   = item.delete('failure_id')
        item['retried_at'] = Time.now.strftime("%Y/%m/%d %H:%M:%S")
        Resque.redis.hset(:failed_jobs, id, Resque.encode(item))
        Job.create(item['queue'], item['payload']['class'], *item['payload']['args'])
      end
    end
  end
end
