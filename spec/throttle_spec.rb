require_relative 'spec_helper'
require_relative File.join(*%w{.. chef_throttle libraries throttle})

module ChefThrottle
  describe Log do

    let(:name) { 'ClassName' }
    let(:it) { Log.new(name) }
    let(:message) { Proc.new {"Hey, now"} }

    [:debug, :info, :warn, :error, :fatal].each do |lvl|
      it "forwards #{lvl} to Chef::Log" do
        expect(Chef::Log).to receive(lvl) do |&blk|
          expect(blk.call).to eq("#{name}: #{message.call}")
        end
        it.send(lvl, &message)
      end
    end
  end

  class TargetLog
    def initialize(target)
      @target = target
    end

    [:debug, :info, :warn, :error, :fatal].each do |lvl|
      define_method(lvl) { |&blk| @target.log(lvl, blk.call) }
    end
  end

  describe EventHandler do
    let(:target) { double('target', log: nil) }
    let(:logger) { TargetLog.new(target) }
    let(:zk)     { double('zookeeper', wait: nil, complete: nil) }

    let(:server) { 'zk' }
    let(:cluster_name) { 'service' }
    let(:limit) { 20 }
    let(:name) { 'localhost' }
    let(:throttle_config) {{server: server, cluster_name: cluster_name, limit: limit, enable: true}}
    let(:node) { double('node', name: name, :attribute? => true, :[] => throttle_config) }
    let(:context) { double('context', node: node) }
    let(:exhibit_string) { 'somehost:9999' }

    before do
      allow(ZookeeperLatch).to receive(:new).and_return(zk, nil) # Landmine if called more than once
      allow(Log).to receive(:new).and_return logger
      md = example.metadata
      throttle_config[:run_on_failure] = true if md[:rof]
      throttle_config[:exhibitor] = exhibit_string if md[:exhibitor]

      expect(node).to receive(:attribute?).with(:chef_throttle).exactly(1).times
      expect(node).to receive(:[]).with(:chef_throttle).at_least(:once) unless example.metadata[:no_throttle]
      expect(Log).to receive(:new).with(EventHandler).exactly(1).times
    end

    describe "converge_start" do
      context "when throttle not enabled" do
        before do
          throttle_config[:enable] = false
        end

        it "logs not enabled" do
          expect(target).to receive(:log).with(:info, "Chef throttle not enabled.")
          subject.converge_start(context)
        end
      end

      context "when throttle config info not present", :no_throttle => true do
        before do
          allow(node).to receive(:attribute?).and_return(false)
        end

        it "logs not enabled" do
          expect(target).to receive(:log).with(:info, "Chef throttle not enabled.")
          subject.converge_start(context)
        end
      end

      context "when throttle is configed" do
        it "sets up the latch" do
          expect(ZookeeperLatch).to receive(:new).with(server, cluster_name, limit, name)
          subject.converge_start(context)
        end

        it "waits on the latch (and logs it)" do
          expect(target).to receive(:log).with(:info, 'Waiting on Cluster lock...').ordered
          expect(zk).to receive(:wait).with(false).ordered
          expect(target).to receive(:log).with(:info, 'Got Cluster lock...').ordered

          subject.converge_start(context)
        end

        it "passes true in the wait call if run_on_failure is true", rof: true do
          expect(zk).to receive(:wait).with(true)
          subject.converge_start(context)
        end

        context "without detailed config" do
          let(:throttle_config) {{enable: true}}
          let(:zkdt) { double('zookeeper data') }
          let(:zk_connect) { 'zookeeper.local.domain' }

          before do
            allow(subject).to receive(:discover_zookeepers).and_return(zkdt)
            expect(subject).to receive(:zk_connect_str).with(zkdt).and_return(zk_connect)
          end

          it "passes in the default values" do
            expect(ZookeeperLatch).to receive(:new).with(zk_connect, 'default_cluster', 1, name)
            subject.converge_start(context)
          end

          it "does the zookeeper discovery with an empty string if there is no :exhibitor attribute" do
            expect(subject).to receive(:discover_zookeepers).with('')
            subject.converge_start(context)
          end

          it "passes zookeeper discovery the :exhibitor attribute if present", exhibitor: true do
            expect(subject).to receive(:discover_zookeepers).with(exhibit_string)
            subject.converge_start(context)
          end
        end
      end
    end

    describe "converge_complete" do
      before do
        subject.converge_start(context)

        # Caching checks -- note converge complete is not to be called w/out a prior call to converge_start
        expect(node).to_not receive(:attribute?)
        expect(node).to_not receive(:[])
        expect(Log).to_not receive(:new)
      end

      it "sends complete to the latch (and logs it)" do
        expect(target).to receive(:log).with(:info, 'Releasing Cluster lock...')#.ordered
        expect(zk).to receive(:complete)#.ordered
        expect(target).to receive(:log).with(:info, 'Released Cluster lock...')#.ordered

        subject.converge_complete
      end
    end

    describe "server (private) when discover_zookeepers bombs" do
      let(:message) { 'fail message' }

      before do
        allow(subject).to receive(:discover_zookeepers).and_raise(ExhibitorDiscovery::ExhibitorError, message)
      end

      it "logs a bunch of stuff and re-raises" do
        expect(target).to receive(:log).with(:warn, "Could not discover ZK connect string from Exhibitor: #{message}")
        expect(target).to receive(:log).with(:warn, 'Define either node[:chef_throttle][:server] (for static config) or node[:chef_throttle][:exhibitor] (for exhibitor discovery)')
        expect(target).to receive(:log).with(:fatal, 'CANCELLING RUN: Throttle mechanism not available.')
        expect{subject.converge_start(context)}.to raise_error(ExhibitorDiscovery::ExhibitorError, message)
      end
    end
  end
end

