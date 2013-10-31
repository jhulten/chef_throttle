
require_relative File.join( %w[.. libraries throttle] )

describe 'chef_throttle::default' do
  include_context :chef_spec_helper

  it do
    expect(chef_run).to install_chef_gem 'zk'
  end
end
