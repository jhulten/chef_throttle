require 'wp-cookbook'

setup_berkshelf(File.join(File.dirname(__FILE__), '..'))

def event_handler_classes(chef_run)
  chef_run
    .run_context
    .events
    .instance_variable_get(:@subscribers)
    .map{ |c| c.class }
end
