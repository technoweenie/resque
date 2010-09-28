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
        ids = [Resque.redis.lrange(:failed, start, count)]
        ids.flatten!
        items = Resque.redis.hmget(:failed_jobs, *ids) || []
        items.map! do |item|
          Resque.decode(item)
        end
        items.compact!
        items
      end

      def self.requeue(index)
        id   = all(index)
        item = Resque.redis.hget(:failed_jobs, id)
        item = Resque.decode(item)
        item['retried_at'] = Time.now.strftime("%Y/%m/%d %H:%M:%S")
        Resque.redis.hset(:failed_jobs, id, Resque.encode(item))
        Job.create(item['queue'], item['payload']['class'], *item['payload']['args'])
      end
    end
  end
end
