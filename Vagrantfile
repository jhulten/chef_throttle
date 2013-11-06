WP_VAGRANT_SERIAL_ID = 12

def self.ip_address(offset)
 ip_range_base = 0x21212000 + WP_VAGRANT_SERIAL_ID * 8 + offset
 address_parts = [ip_range_base >> 24, ip_range_base >> 16 & 0xff, ip_range_base >> 8 & 0xff, ip_range_base & 0xff]
 address_parts * '.'
end

chef_zero_ip = ip_address(2)
chef_zero_port = '8889'

Vagrant.configure("2") do |config|
  config.vm.provider :virtualbox do |vb|
    vb.customize ["modifyvm", :id, "--natdnshostresolver1", "on"]
  end

  config.vm.define :chefzero do |chefzero|
    chefzero.vm.network :private_network, ip: chef_zero_ip
    chefzero.vm.box = "wp-chefzero"
    chefzero.vm.box_url = 'http://fs0:8888/wp-chefzero.box'

    chefzero.vm.provision :chefzero do |cz|
      cz.ip = chef_zero_ip
      cz.port = chef_zero_port
      cz.setup do |p|
        p.import_data_bag_item(:users, :global)
        p.import_berkshelf_cookbooks
      end
    end
  end

  config.vm.define :target do |target|
    target.vm.hostname = "stuff.dev.pages"
    target.vm.network :private_network, ip: ip_address(3)
    target.vm.box = "wp-precise64"
    target.vm.box_url = 'http://fs0:8888/wp-precise64.box'

    target.vm.provision :chef_client do |chef|
      chef.chef_server_url = "http://#{chef_zero_ip}:#{chef_zero_port}"
      chef.validation_key_path = Vagrant::ChefzeroPlugin.pemfile
      chef.add_recipe "wp-vagrant"

      #The recipe we actually care about.
      chef.add_recipe "chef_throttle::default"
    end
  end
end

