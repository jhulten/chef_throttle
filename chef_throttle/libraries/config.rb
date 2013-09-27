require "./throttle.rb"

begin
  require 'zk'
  Chef::Log.info 'Loading Chef Zookeeper throttle'

  Chef::Config[:event_handlers] ||= []
  Chef::Config[:event_handlers] << ChefThrottle::EventHandler.new 

rescue LoadError => e
   Chef::Log.warn "ZK gem not available at start. Chef throttle not engaged."
end
