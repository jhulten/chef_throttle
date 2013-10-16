
require_relative File.join( %w[.. chef_throttle libraries throttle] )

describe 'chef_throttle::default' do
  include_context :chef_spec_helper

  it do
    # Chefspec does not wire up event handlers for direct examination, so we
    # must extract the list of handlers and inspect it directly.
    expect(event_handler_classes(chef_run)).to include(ChefThrottle::EventHandler)
  end
end
