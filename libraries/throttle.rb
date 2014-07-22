
require 'chef/log'
require 'chef/event_dispatch/base'

module ChefThrottle
  class Log
    def initialize(name)
      @name = name
    end

    def log(level)
      Chef::Log.send(level) { "#{@name}: #{yield}" }
    end

    [:debug, :info, :warn, :error, :fatal].each do |lvl|
      define_method(lvl) { |&blk| log(lvl, &blk) }
    end
  end

  class EventHandler < Chef::EventDispatch::Base
    attr_accessor :shared_latch

    def converge_start(run_context)
      self.shared_latch = SharedLatch.new(run_context.node)
      shared_latch.lock_if_enabled
    end

    def converge_complete
      shared_latch.unlock_if_enabled
    end
  end

  class SharedLatch
    attr_accessor :chef_node

    def initialize(chef_node)
      self.chef_node = chef_node
      @latch = nil
    end

    def latch
      @latch ||= ZookeeperLatch.new(chef_node[:chef_throttle][:config_string], lock_path, limit, host)
    end

    def lock_path
      chef_node[:chef_throttle][:lock_path] || "/queue"
    end

    def limit
      chef_node[:chef_throttle][:limit] || 1
    end

    def host
      chef_node[:fqdn]
    end

    def lock_if_enabled
      if enabled?
        log.info{ "Waiting for chef_throttle lock at #{chef_node[:chef_throttle][:config_string]}" }
        latch.wait(run_on_failed_latch?)
        log.info{ "Received chef_throttle lock; proceeding" }
      else
        log.info{ "Chef throttle not enabled; proceeding" }
      end
    end

    def unlock_if_enabled
      if enabled?
        log.info{ "Releasing chef_throttle lock" }
        latch.complete
        log.info{ "Released chef_throttle lock" }
      end
    end

    def cluster_path
      [ chef_node[:chef_throttle][:cluster_path], chef_node[:chef_throttle][:cluster_name] ].join('/')
    end

    private
    def enabled?
      if @enabled.nil?
        @enabled = chef_node[:chef_throttle][:enable] && zk_available?
        log_enabled_message
      end
      @enabled
    end

    def log_enabled_message
      if @enabled
        log.info { "chef_throttle enabled" }
      else
        log.warn { "chef_throttle disabled" }
      end
    end

    def zk_available?
      if @zk_available.nil?
        begin
          require 'zk'
          @zk_available = true
        rescue LoadError
          log.warn "'zk' gem not available"
          @zk_available = false
        end
      end
      @zk_available
    end

    def run_on_failed_latch?
      # FIXME refactor test to look for falsey value instead of False
      ! chef_node[:chef_throttle][:run_on_failure].nil? && chef_node[:chef_throttle][:run_on_failure]
    end

    def log
      @log ||= Log.new(self.class)
    end

  end

  class ZookeeperLatch
    class ConnectionProblem < StandardError ; end

    attr_reader :server, :cluster_name, :limit, :lock_data, :latch
    private :latch

    def initialize(server, lock_path, limit, lock_data)
      @server = server
      @lock_path = lock_path
      @lock_data = lock_data
      @limit = limit
      @latch = Queue.new
    end

    def wait(run_on_fail = false)
      zk.on_state_change do |event|
        if run_on_fail
          log.warn { "Zookeeper connection failed; continuing without throttle..." }
          release
        else
          raise ConnectionProblem, "Zookeeper connection failed"
        end
      end
      log.info { "Creating Zookeeper node #{zk_path}" }
      zk.mkdir_p(zk_path)
      zk_node
      log.info {"created node #{zk_node_id}"}
      fetch_children
      block
    end

    def complete
      zk.delete(zk_node)
      zk.close!
    end

    private

    def zk
      @zk ||= ::ZK.new(@server)
    end

    def fetch_children
      watching = false
      children = zk.children(zk_path).select{|child| child < zk_node_id}.sort
      my_index = children.length
      log.info {"In position #{my_index}"}
      if my_index < @limit
        log.info {"My turn!"}
        release
      else
        log.info {"Waiting ..."}
        watching = true
        children.last(limit).each { |x| watch_child(x) }
      end
    rescue ZK::Exceptions::NoNode
      raise unless watching
      log.info { "Child missing. Retrying fetch children." }
      retry
    end

    def watch_child(child)
      log.info {"watching #{child}"}
      child_path = "#{zk_path}/#{child}"
      zk.register(child_path) do |event|
        log.info {"saw change #{event} for #{child}"}
        fetch_children
      end
      begin
        zk.get(child_path, :watch => true)
      rescue
        log.info {"node for #{child_path} disappeared."}
        raise
      end
    end

    def zk_node
      @zk_node ||= zk_create
    end

    def zk_create
      prefix = "#{zk_path}/lock-"
      log.info { "Creating sequential, ephemeral Zookeeper node with prefix #{prefix}" }
      zk.create(prefix, @lock_data, :sequential => true, :ephemeral => true)
    end

    def zk_node_id
      zk_node.split('/').last
    end

    def zk_path
      @lock_path
    end

    def release
      latch << 1
    end

    def block
      latch.pop
    end

    def log
      @log ||= Log.new(self.class)
    end
  end
end
