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
      define_method(lvl) { |&blk| @target.send(lvl, blk.call) }
    end
  end

  describe EventHandler do
    let(:target) { double('target', debug: nil, info: nil, warn: nil, error: nil, fatal: nil) }
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
          expect(target).to receive(:info).with("Chef throttle not enabled.")
          subject.converge_start(context)
        end
      end

      context "when throttle config info not present", :no_throttle => true do
        before do
          allow(node).to receive(:attribute?).and_return(false)
        end

        it "logs not enabled" do
          expect(target).to receive(:info).with("Chef throttle not enabled.")
          subject.converge_start(context)
        end
      end

      context "when throttle is configed" do
        it "sets up the latch" do
          expect(ZookeeperLatch).to receive(:new).with(server, cluster_name, limit, name)
          subject.converge_start(context)
        end

        it "waits on the latch (and logs it)" do
          expect(target).to receive(:info).with('Waiting on Cluster lock...').ordered
          expect(zk).to receive(:wait).with(false).ordered
          expect(target).to receive(:info).with('Got Cluster lock...').ordered

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
        expect(target).to receive(:info).with('Releasing Cluster lock...')#.ordered
        expect(zk).to receive(:complete)#.ordered
        expect(target).to receive(:info).with('Released Cluster lock...')#.ordered

        subject.converge_complete
      end
    end

    describe "server (private) when discover_zookeepers bombs" do
      let(:message) { 'fail message' }

      class TestError < Exception
        include ExhibitorDiscovery
      end

      before do
        throttle_config.delete(:server) # Have to get to discover_zookeeper first...
        allow(subject).to receive(:discover_zookeepers).and_raise(TestError, message)
      end

      it "logs a bunch of stuff and re-raises" do
        expect(target).to receive(:warn).with("Could not discover ZK connect string from Exhibitor: #{message}").ordered
        expect(target).to receive(:warn).with('Define either node[:chef_throttle][:server] (for static config) or node[:chef_throttle][:exhibitor] (for exhibitor discovery)').ordered
        expect(target).to receive(:fatal).with('CANCELLING RUN: Throttle mechanism not available.').ordered
        expect{subject.converge_start(context)}.to raise_error(TestError, message)
      end

      it "merely warns (and proceeds) if run_on_failure is set", rof: true do
        expect(target).to receive(:warn).with("Could not discover ZK connect string from Exhibitor: #{message}").ordered
        expect(target).to receive(:warn).with('Define either node[:chef_throttle][:server] (for static config) or node[:chef_throttle][:exhibitor] (for exhibitor discovery)').ordered
        expect(target).to receive(:warn).with('Continuing WITHOUT throttle...').ordered
        subject.converge_start(context)
      end
    end
  end

  describe ExhibitorDiscovery do
    let(:subject) do
      s = Object.new
      s.extend ExhibitorDiscovery
      s
    end

    let(:zks) { {'servers' => ['host1', 'host2', 'hostN'], 'port' => 8675} }

    describe "discover_zookeepers" do
      attr_reader :exc

      let(:zkhost) { '127.0.0.1:1234' }
      let(:host) { 'localhost' }
      let(:port) { 80 }
      let(:path) { '/my/path' }
      let(:body) { '{"json"=>"string"}' }
      let(:url) { double(:url, host: host, port: port, path: path) }
      let(:http) { double(:http, get: result, :read_timeout= => nil, :open_timeout= => nil) }
      let(:result) { double(:result, body: body) }

      before do
        [
          [JSON, :parse, zks, :json],
          [http, :get, result, :get],
          [Net::HTTP, :new, http, :http],
          [URI, :join, url, :uri],
        ].each do |object, method, result, key|
          if (ex = example.metadata[key])
            @exc = ex
            allow(object).to receive(method).and_raise(exc, exc.name)
          else
            allow(object).to receive(method).and_return(result)
          end
        end
      end

      context "happy path" do
        it "creates a url" do
          expect(URI).to receive(:join).with(zkhost, '/exhibitor/v1/cluster/list')
          subject.discover_zookeepers(zkhost)
        end

        it "creates an http connection" do
          expect(Net::HTTP).to receive(:new).with(host, port)
          subject.discover_zookeepers(zkhost)
        end

        it "sets the http timeouts" do
          expect(http).to receive(:read_timeout=).with(3)
          expect(http).to receive(:open_timeout=).with(3)
          subject.discover_zookeepers(zkhost)
        end

        it "gets the data from the zkhost" do
          expect(http).to receive(:get).with(path)
          subject.discover_zookeepers(zkhost)
        end

        it "passes the body of the response to JSON.parse" do
          expect(JSON).to receive(:parse).with(body)
          subject.discover_zookeepers(zkhost)
        end

        it "returns the results of the JSON.parse" do
          expect(subject.discover_zookeepers(zkhost)).to eq(zks)
        end
      end

      context "exceptions are tagged and released" do
        def expect_exception
          expect{subject.discover_zookeepers(zkhost)}.to raise_error { |e|
            expect(e).to be_instance_of(exc)
            expect(e.message).to eq(exc.name)
            expectation = example.metadata[:notagging] ? :to_not : :to
            expect(e).send(expectation, be_kind_of(ExhibitorDiscovery::Error))
          }
        end

        it "on URI.join", uri: ArgumentError, notagging: true do
          expect_exception
        end

        it "on Net::HTTP.new", http: ArgumentError do
          expect_exception
        end

        it "on http.get", get: Timeout::Error do
          expect_exception
        end

        it "on JSON.parse", get: ParseError do
          expect_exception
        end
      end
    end

    describe "zk_connect_str" do
      let(:zks){ {'servers' => ['host1', 'host2', 'hostN'], 'port' => 8675} }
      let(:base_string){ 'host1:8675,host2:8675,hostN:8675' }
      let(:root){ 'this/base/url' }
      it "distributes the port across the servers, joining with commas" do
        expect(subject.zk_connect_str(zks)).to eq(base_string)
      end

      it "appends a supplied root" do
        expect(subject.zk_connect_str(zks, root)).to eq("#{base_string}/#{root}")
      end
    end
  end

  describe ZookeeperLatch do
   it "does some things" 
  end


  describe "ChefThrottle self-integration" do

    let(:handler) { EventHandler.new }
    let(:connection) { double('connection', get: get_result, :read_timeout= => 3, :open_timeout= => 3) }
    let(:get_result) { double('get_result', body: zkdata.to_json) }
    let(:zkdata) { {'servers' => ['this_host', 'that.host', 'the.other.host'], 'port' => 13579} }

    before do
      %w{debug info warn error fatal}.each do |lvl|
        allow(Chef::Log).to receive(lvl)
      end

      allow(Net::HTTP).to receive(:new).and_return(connection)
    end

    it "does some things"
  end
end


