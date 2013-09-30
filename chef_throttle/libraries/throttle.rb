
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
    # From https://github.com/SimpleFinance/chef-zookeeper/blob/master/libraries/exhibitor_discovery.rb
    # Licensed under Apache 2
    require 'net/http'
    require 'uri'

    class ExhibitorError < StandardError
    end


    def discover_zookeepers(exhibitor_host)
      require 'json'
      url = URI.join(exhibitor_host, '/exhibitor/v1/cluster/list')
      begin
        http = Net::HTTP.new(url.host, url.port)
        http.read_timeout = http.open_timeout = 3
        JSON.parse(http.get(url.path).body)
      rescue StandardError => reason
        raise ExhibitorError, reason
      end
    end

    def zk_connect_str(zookeepers, chroot = nil)
      # zookeepers: as returned from discover_zookeepers
      # chroot: optional chroot
      #
      # returns a zk connect string as used by kafka, and others
      # host1:port,...,hostN:port[/<chroot>]

      zk_connect = zookeepers["servers"].collect { |server| "#{server}:#{zookeepers['port']}" }.join ","
      if not chroot.nil?
        zk_connect += "/#{chroot}"
      end
      zk_connect
    end
  end

  class EventHandler < Chef::EventDispatch::Base
    attr_accessor :node

    include ExhibitorDiscovery

    # Called before convergence starts
    def converge_start(run_context)
      node = run_context.node

      if enabled?
        log.info "Waiting on Cluster lock..."
        shared_latch.wait
        log.info "Got Cluster lock..."
      else 
        log.info "Chef throttle not enabled."
      end
    end

    # Called when the converge phase is finished.
    def converge_complete
      if enabled?
        log.info "Releasing Cluster lock..."
        shared_latch.complete
        log.info "Released Cluster lock..."
      end
    end

    private
    def enabled?
      @enabled ||= (node.attribute?(:chef_throttle) && node[:chef_throttle][:enable] == true)
    end

    def shared_latch
      @shared_latch ||= begin 
                          begin
                            server       = node[:chef_throttle][:server] || zk_connect_str(discover_zookeepers(node[:chef_throttle][:exhibitor] || "" ))
                          rescue ExhibitorError
                            log.error { "Could not discover ZK connect string from Exhibitor" }
                            log.fatal { "define either node[:chef_throttle][:server] (for static config) or node[:chef_throttle][:exhibitor] (for exhibitor discovery)" }
                          end
                          cluster_name = node[:chef_throttle][:cluster_name] || "default_cluster"
                          limit        = node[:chef_throttle][:limit] || 1
                          host         = node.name
                          ZookeeperLatch.new(server, cluster_name, limit, host)
                        end
    end

    def log
      @log ||= Log.new(self.class)
    end

  end

  class ZookeeperLatch
    attr_reader :server, :cluster_name, :limit, :lock_data

    def initialize(server, cluster_name, limit, lock_data)
      @server = server
      @cluster_name = cluster_name
      @lock_data = lock_data
      @limit = limit
    end

    def wait
      zk.on_state_change do |event|
        release
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
      children = zk.children(zk_path).sort
      my_index = children.index(node_id)
      log.info {"In position #{my_index}"}
      if my_index < @limit
        log.info {"My turn!"}
        release
      else
        log.info {"Waiting ..."}
        children[my_index - limit, limit].each { |x| watch_child(x) }
      end
    end

    def watch_child(child)
      log.info {"watching #{child}"}
      child_path = "#{zk_path}/#{child}"
      zk.register(child_path) do |event|
        log.info {"saw change #{event} for #{child}"}
        fetch_children
      end
      zk.get(child_path, :watch => true)
    end

    def zk_node
      @node ||= zk.create("#{zk_path}/lock-", "#{@lock_data}", :sequential => true, :ephemeral => true)
    end

    def zk_node_id
      zk_node.split('/').last
    end

    def zk_path
      "/chef_throttle/clusters/#{@cluster_name}/queue"
    end

    def latch
      @queue ||= Queue.new
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


