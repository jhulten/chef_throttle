
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

  module ExhibitorDiscovery
    module Error ; end

    # From https://github.com/SimpleFinance/chef-zookeeper/blob/master/libraries/exhibitor_discovery.rb
    # Licensed under Apache 2
    require 'net/http'
    require 'uri'

    def discover_zookeepers(exhibitor_host)
      url = URI.join(exhibitor_host, '/exhibitor/v1/cluster/list')
      discover_zookeepers_from_url(url)
    end

    def discover_zookeepers_from_url(url)
      require 'json'

      http = Net::HTTP.new(url.host, url.port)
      http.read_timeout = http.open_timeout = 3
      JSON.parse(http.get(url.path).body)
    rescue Exception => e
      e.extend( Error )
      raise
    end

    def zk_connect_str(zookeepers, chroot = nil)
      # zookeepers: as returned from discover_zookeepers
      # chroot: optional chroot
      #
      # returns a zk connect string as used by kafka, and others
      # host1:port,...,hostN:port[/<chroot>]

      zk_connect = zookeepers["servers"].collect { |svr| "#{svr}:#{zookeepers['port']}" }.join ","
      chroot.nil? ? zk_connect : "#{zk_connect}/#{chroot}"
    end

  end

  class EventHandler < Chef::EventDispatch::Base
    attr_reader :node

    include ExhibitorDiscovery

    # Called before convergence starts
    def converge_start(run_context)
      @node = run_context.node

      if enabled?
        log.info{ "Waiting on Cluster lock..." }
        shared_latch.wait(run_on_failed_latch?)
        log.info{ "Got Cluster lock..." }
      else
        log.info{ "Chef throttle not enabled." }
      end
    end

    # Called when the converge phase is finished.
    def converge_complete
      if enabled?
        log.info{ "Releasing Cluster lock..." }
        shared_latch.complete
        log.info{ "Released Cluster lock..." }
      end
    end

    private
    def enabled?
      @enabled ||= (node.attribute?(:chef_throttle) && node[:chef_throttle][:enable] == true)
    end

    def run_on_failed_latch?
      @run_on_failed_latch ||= node[:chef_throttle][:run_on_failure] == true
    end

    def shared_latch
      @shared_latch ||= begin
        limit        = node[:chef_throttle][:limit] || 1
        host         = node.name
        lock_path    = node[:chef_throttle][:lock_path] || "/queue"
        ZookeeperLatch.new(server, lock_path, limit, host)
      end
    end

    def chroot
      @chroot ||= "#{node[:chef_throttle][:cluster_path]}/#{node[:chef_throttle][:cluster_name]}".gsub(/^\//, "")
    end

    def has_server?
      @has_server ||= (node.attribute?(:chef_throttle) && node[:chef_throttle].has_key?(:server)) &&
          !!node[:chef_throttle][:server]
    end

    def server
      if has_server?
        connect_data = {"servers" => [ node[:chef_throttle][:server] ], "port" => 2181}
        zk_connect_str(connect_data, chroot)
      else
        zk_connect_str(discover_zookeepers(node[:chef_throttle][:exhibitor] || "" ), chroot)
      end
    rescue ExhibitorDiscovery::Error => e
      log.warn { "Could not discover ZK connect string from Exhibitor: #{e.message}" }
      log.warn { "Define either node[:chef_throttle][:server] (for static config) or node[:chef_throttle][:exhibitor] (for exhibitor discovery)" }
      if run_on_failed_latch?
        log.warn { "Continuing WITHOUT throttle..." }
      else
        log.fatal { "CANCELLING RUN: Throttle mechanism not available." }
        raise e
      end
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
          log.warn { "Zookeeper connection failed and :run_on_failure set to true. Continuing..." }
          release
        else
          raise ConnectionProblem, "Zookeeper connection failed and :run_on_failure set to false."
        end
      end
      zk.mkdir_p(zk_path)
      zk_node
      log.info {"created node #{zk_node_id}"}
      fetch_children
      block
    end

    def complete
      zk.delete(zk_node)
    end

    private

    def zk
      @zk ||= ::ZK::Client.new(@server)
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
      @node ||= zk.create("#{zk_path}/lock-", "#{@lock_data}", :sequential => true, :ephemeral => true)
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
