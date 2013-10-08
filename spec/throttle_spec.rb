require_relative 'spec_helper'
require 'zk'
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
    let(:cluster_path) { '/my_env/clusters' }
    let(:lock_path) { '/queue' }
    let(:limit) { 20 }
    let(:name) { 'localhost' }
    let(:throttle_config) {
      { server: server,
        cluster_name: cluster_name,
        cluster_path: cluster_path,
        limit: limit,
        lock_path: lock_path,
        enable: true}}
    let(:node) { double('node', name: name, :attribute? => true, :[] => throttle_config) }
    let(:context) { double('context', node: node) }
    let(:exhibit_string) { 'somehost:9999' }
    let(:chroot) { File.join( cluster_path, cluster_name) }
    let(:zk_connect_string) { "#{server}:2181#{chroot}" }

    before do
      allow(ZookeeperLatch).to receive(:new).and_return(zk, nil) # Landmine if called more than once
      allow(Log).to receive(:new).and_return logger
      md = example.metadata
      throttle_config[:exhibitor] = exhibit_string if md[:exhibitor]

      expect(node).to receive(:[]).with(:chef_throttle).at_least(:once) unless md[:no_throttle]
      expect(Log).to receive(:new).with(EventHandler).exactly(1).times
    end

    shared_context "run_on_failure" do
      before do
        throttle_config[:run_on_failure] = true
      end
    end

    describe "#converge_start" do
      context "when throttle is not enabled" do
        before do
          throttle_config[:enable] = false
        end

        it "logs the message 'not enabled'" do
          expect(target).to receive(:info).with("Chef throttle not enabled.")
          subject.converge_start(context)
        end
      end

      context "when throttle config info is not present", :no_throttle => true do
        before do
          allow(node).to receive(:attribute?).and_return(false)
        end

        it "logs the message 'not enabled'" do
          expect(target).to receive(:info).with("Chef throttle not enabled.")
          subject.converge_start(context)
        end
      end

      context "when throttle is configured" do
        it "sets up the latch" do
          expect(ZookeeperLatch).to receive(:new).with(zk_connect_string, lock_path, limit, name)
          subject.converge_start(context)
        end

        it "waits on the latch (and logs it)" do
          expect(target).to receive(:info).with('Waiting on Cluster lock...').ordered
          expect(zk).to receive(:wait).with(false).ordered
          expect(target).to receive(:info).with('Got Cluster lock...').ordered

          subject.converge_start(context)
        end

        context "when run_on_failure is true" do
          include_context "run_on_failure"
          
          it "passes true in the wait call" do
            expect(zk).to receive(:wait).with(true)
            subject.converge_start(context)
          end
        end

        context "without detailed config" do
          let(:throttle_config) { {enable: true} }
          let(:zkdt) { double('zookeeper data') }
          let(:zk_connect) { 'zookeeper.local.domain' }

          before do
            allow(subject).to receive(:discover_zookeepers).and_return(zkdt)
            expect(subject).to receive(:zk_connect_str).with(zkdt, '').and_return(zk_connect)
          end

          it "passes in the default values" do
            expect(ZookeeperLatch).to receive(:new).with(zk_connect, lock_path, 1, name)
            subject.converge_start(context)
          end

          context "when there is no :exhibitor attribute" do
            it "does the zookeeper discovery with an empty string" do
              expect(subject).to receive(:discover_zookeepers).with('')
              subject.converge_start(context)
            end
          end

          context "when the :exhibitor attribute is present" do
            it "passes zookeeper discovery the :exhibitor attribute", exhibitor: true do
              expect(subject).to receive(:discover_zookeepers).with(exhibit_string)
              subject.converge_start(context)
            end
          end
        end
      end
    end

    describe "#converge_complete" do
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

    describe "#server (private)" do
      context "when discover_zookeepers bombs" do
        let(:message) { 'fail message' }

        class TestError < Exception
          include ExhibitorDiscovery::Error
        end

        before do
          throttle_config.delete(:server) # Have to get to discover_zookeeper first...
          allow(subject).to receive(:discover_zookeepers).and_raise(TestError, message)
        end

        context "run_on_failure is not set" do
          it "logs a bunch of stuff and re-raises" do
            expect(target).to receive(:warn).with("Could not discover ZK connect string from Exhibitor: #{message}").ordered
            expect(target).to receive(:warn).with('Define either node[:chef_throttle][:server] (for static config) or node[:chef_throttle][:exhibitor] (for exhibitor discovery)').ordered
            expect(target).to receive(:fatal).with('CANCELLING RUN: Throttle mechanism not available.').ordered
            expect{subject.converge_start(context)}.to raise_error(TestError, message)
          end
        end

        context "run_on_failure is set" do
          include_context "run_on_failure"
          
          it "merely warns (and proceeds)" do
            expect(target).to receive(:warn).with("Could not discover ZK connect string from Exhibitor: #{message}").ordered
            expect(target).to receive(:warn).with('Define either node[:chef_throttle][:server] (for static config) or node[:chef_throttle][:exhibitor] (for exhibitor discovery)').ordered
            expect(target).to receive(:warn).with('Continuing WITHOUT throttle...').ordered
            subject.converge_start(context)
          end
        end
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

    describe "#discover_zookeepers" do
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

    describe "#zk_connect_str" do
      let(:zks){ {'servers' => ['host1', 'host2', 'hostN'], 'port' => 8675} }
      let(:base_string){ 'host1:8675,host2:8675,hostN:8675' }
      let(:root){ 'this/base/url' }

      context "without a supplied root" do
        it "distributes the port across the servers, joining with commas" do
          expect(subject.zk_connect_str(zks)).to eq(base_string)
        end
      end

      context "with a supplied root" do
        it "appends the root" do
          expect(subject.zk_connect_str(zks, root)).to eq("#{base_string}/#{root}")
        end
      end
    end
  end

  describe ZookeeperLatch do
    let(:md) { example.metadata }
    let(:subject) { ZookeeperLatch.new(server, path, limit, lock_data) }
    let(:server) { }
    let(:limit) { 2 }
    let(:lock_data) { 'fqdn.org' }

    let(:q) { double("queue", pop: nil, :<< => nil) }

    let(:client) { double("zk client", mkdir_p: nil, delete: nil) }
    let(:event) { double("event") }
    let(:zk_node) { "server/some_path/#{node_id}" }
    let(:node_id) { 5 }

    let(:path) { '/some/path/to/queue' }

    let(:target) { double('target', debug: nil, info: nil, warn: nil, error: nil, fatal: nil) }
    let(:logger) { TargetLog.new(target) }

    before do
      allow(::ZK::Client).to receive(:new).and_return(client, nil)
      allow(Log).to receive(:new).and_return(logger, nil)
      allow(Queue).to receive(:new).and_return(q)

      allow(client).to receive(:create).and_return(zk_node, nil)
    end

    describe "#wait" do

      shared_context "wait main body" do
        before do
          allow(subject).to receive(:fetch_children).and_return
        end

        it "caches the client, node, and log" do
          expect{subject.wait(*wait_args)}.to_not raise_error
          expect{subject.wait(*wait_args)}.to_not raise_error
        end

        it "creates the reservation accumulator" do
          expect(client).to receive(:mkdir_p).with(path)
          subject.wait(*wait_args)
        end

        it "makes the reservation" do
          expect(client).to receive(:create).with("#{path}/lock-", "#{lock_data}", sequential: true, ephemeral: true)
          subject.wait(*wait_args)
        end

        it "logs the node creation" do
          expect(target).to receive(:info).with("created node #{node_id}")
          subject.wait(*wait_args)
        end

        it "fetches the children" do
          expect(subject).to receive(:fetch_children)
          subject.wait(*wait_args)
        end

        it "blocks" do
          expect(q).to receive(:pop)
          subject.wait(*wait_args)
        end
      end

      context "no connection problems" do
        let(:wait_args) { [] }

        include_context "wait main body"

        before do
          allow(client).to receive(:on_state_change)
        end
      end

      context "connection problem" do
        before do
          allow(client).to receive(:on_state_change).and_yield(event)
        end

        it "raises a runtime error" do
          expect{subject.wait}.to raise_error(ZookeeperLatch::ConnectionProblem)
        end

        context "run_on_fail is true" do
          let(:wait_args) { [true] }

          it "logs the problem" do
            expect(target).to receive(:warn)
            subject.wait(*wait_args)
          end

          it "releases the node" do
            allow(subject).to receive(:fetch_children)
            expect(q).to receive(:<<)
            subject.wait(*wait_args)
          end

          include_context "wait main body"
        end
      end
    end

    describe "#complete" do
      it "caches the client and node" do
        expect{subject.complete}.to_not raise_error
        expect{subject.complete}.to_not raise_error
      end

      it "deletes the node from the client" do
        expect(client).to receive(:delete).with(zk_node)
        subject.complete
      end
    end

    describe "#fetch_children (private)" do
      let(:children) { [ 9, 1, 6, 3, 2, 4, 8, 7, 5 ].select{|c| c > (md[:time] || 0)} }
      let(:infront) { children.select{|c| c < node_id} }
      let(:watchable) { infront.select{|c| c != md[:boom] and c >= node_id - limit} }

      before do
        allow(client).to receive(:children).and_return(children.map{|c| c.to_s}, [node_id.to_s]) unless md[:nonode]
        watchable.each{|c| allow(subject).to receive(:watch_child).with(c.to_s).and_return} if watchable.length >= limit
        ex = md[:ex]
        allow(subject).to receive(:watch_child).with(md[:boom]).and_raise(md[:ex]) if md[:ex]
      end

      it "gets the children from the client" do
        expect(client).to receive(:children).with(path)
        subject.send(:fetch_children)
      end

      context "child watching under various conditions" do
        before do
          watchable.each{|c| expect(subject).to receive(:watch_child).with(c.to_s)} if watchable.length >= limit
          expect(target).to receive(:info).with("In position #{infront.length}")
        end

        it "watches only the limit number of leading children (instead of releasing)", time: 0 do
          expect(subject).to_not receive(:release)
          subject.send(:fetch_children)
        end

        it "watches if there are exactly the limit number of leading children (instead of releasing)", time: 2 do
          expect(subject).to_not receive(:release)
          subject.send(:fetch_children)
        end

        it "releases if there are fewer than the limit of leading children", time: 3 do
          expect(subject).to receive(:release) if watchable.length > limit
          subject.send(:fetch_children)
        end

        it "releases if there are no leading children", time: 4 do
          expect(subject).to receive(:release)
          subject.send(:fetch_children)
        end

        it "logs the release", time: 4 do
          expect(target).to receive(:info).with('My turn!')
          subject.send(:fetch_children)
        end

        it "logs the waiting", time: 0 do
          expect(target).to receive(:info).with('Waiting ...')
          subject.send(:fetch_children)
        end

        it "retries if a child goes missing", boom: '4', ex: ZK::Exceptions::NoNode do
          expect(client).to receive(:children).exactly(2).times
          expect(target).to receive(:info).with("Waiting ...")
          expect(target).to receive(:info).with("In position 0")
          expect(target).to receive(:info).with("My turn!")
          expect(subject).to receive(:release)
          subject.send(:fetch_children)
        end
      end

      it "does not intercept an early ZK::Exceptions::NoNode", nonode: true do
        allow(client).to receive(:children).and_raise(ZK::Exceptions::NoNode)
        expect{subject.send(:fetch_children)}.to raise_error(ZK::Exceptions::NoNode)
      end
    end

    describe "#watch_child (private)" do
      let(:child) { 'child_resource' }
      let(:child_path) { "#{path}/#{child}" }
      let(:ex) { nil }

      before do
        allow(subject).to receive(:fetch_children).and_return
      end

      shared_context "watch_child core assertions" do
        let(:sense){ ex ? :to : :to_not }

        it "logs the watch" do
          expect(target).to receive(:info).with("watching #{child}")
          expect{subject.send(:watch_child, child)}.send(sense, raise_error(*(ex ? [ex] : [])))
        end

        it "registers the child path" do
          expect(client).to receive(:register).with(child_path)
          expect{subject.send(:watch_child, child)}.send(sense, raise_error(*(ex ? [ex] : [])))
        end

        it "gets the child path" do
          expect(client).send((ex == RegBoom ? :to_not : :to), receive(:get).with(child_path, watch: true))
          expect{subject.send(:watch_child, child)}.send(sense, raise_error(*(ex ? [ex] : [])))
        end
      end

      shared_context "watch_child with zk_get okay" do
        before do
          allow(client).to receive(:get)
        end
      end

      class GetBoom < StandardError ; end

      shared_context "watch_child with zk_get raising" do
        let(:ex) { GetBoom }
        before do
          allow(client).to receive(:get).and_raise GetBoom
          expect(target).to receive(:info).with("node for #{child_path} disappeared.")
        end
      end

      shared_context "zk.register does not yield" do
        before do
          allow(client).to receive(:register)
        end

        it "does not fetch the children" do
          expect(subject).to_not receive(:fetch_children)
          expect{subject.send(:watch_child, child)}.send(sense, raise_error(*(ex ? [ex] : [])))
        end

        it "does not log the change" do
          expect(target).to_not receive(:info).with(/^saw change/)
          expect{subject.send(:watch_child, child)}.send(sense, raise_error(*(ex ? [ex] : [])))
        end

        include_context "watch_child core assertions"
      end

      shared_context "zk.register yields" do
        before do
          allow(client).to receive(:register).and_yield(event)
        end

        it "fetches the children" do
          expect(subject).to receive(:fetch_children)
          expect{subject.send(:watch_child, child)}.send(sense, raise_error(*(ex ? [ex] : [])))
        end

        it "logs the change" do
          expect(target).to receive(:info).with("saw change #{event} for #{child}")
          expect{subject.send(:watch_child, child)}.send(sense, raise_error(*(ex ? [ex] : [])))
        end

        include_context "watch_child core assertions"
      end

      context "zk.register does not yield, zk.get is okay" do
        include_context "zk.register does not yield"
        include_context "watch_child with zk_get okay"
      end

      context "zk.register does yield, zk.get is okay" do
        include_context "zk.register yields"
        include_context "watch_child with zk_get okay"
      end

      context "zk.register does not yield, zk.get bombs" do
        include_context "zk.register does not yield"
        include_context "watch_child with zk_get raising"
      end

      context "zk.register does yield, zk.get bombs" do
        include_context "zk.register yields"
        include_context "watch_child with zk_get raising"
      end

      class RegBoom < StandardError ; end

      context "zk.register bombs" do
        let(:ex) { RegBoom }
        before do
          allow(client).to receive(:register).and_raise( RegBoom )
        end

        it "bubbles the error" do
          expect{subject.watch_child(child)}.to raise_error
        end

        include_context "watch_child core assertions"
      end
    end
  end


  describe "ChefThrottle self-integration" do

    let(:target) { double('target', debug: nil, info: nil, warn: nil, error: nil, fatal: nil) }
    let(:logger) { TargetLog.new(target) }
    let(:server) { 'zk' }
    let(:cluster_name) { 'service' }
    let(:cluster_path) { '/my_env/clusters' }
    let(:lock_path) { '/queue' }
    let(:limit) { 20 }
    let(:name) { 'localhost' }
    let(:throttle_config) {
      { server: server,
        cluster_name: cluster_name,
        cluster_path: cluster_path,
        limit: limit,
        lock_path: lock_path,
        enable: true}}
    let(:node) { double('node', name: name, :attribute? => true, :[] => throttle_config) }
    let(:context) { double('context', node: node) }
    let(:handler) { EventHandler.new }
    let(:connection) { double('connection', get: get_result, :read_timeout= => 3, :open_timeout= => 3) }
    let(:get_result) { double('get_result', body: zkdata.to_json) }
    let(:zkdata) { {'servers' => ['this_host', 'that.host', 'the.other.host'], 'port' => 13579} }
    let(:client) { double("zk client", mkdir_p: nil, delete: nil) }
    let(:q) { double("queue", pop: nil, :<< => nil) }
    let(:zk_node) { "server/some_path/#{node_id}" }
    let(:node_id) { 5 }

    let(:children) { [ 9, 1, 6, 3, 2, 4, 8, 7, 5 ] }
    let(:infront) { children.select{|c| c < node_id} }
    let(:watchable) { infront.select{|c| c >= node_id - limit} }

    let(:handler) { EventHandler.new }
    before do
      %w{debug info warn error fatal}.each do |lvl|
        allow(Chef::Log).to receive(lvl)
      end

      allow(Net::HTTP).to receive(:new).and_return(connection)
      allow(ZK::Client).to receive(:new).and_return(client)
      allow(client).to receive(:create).and_return(zk_node, nil)
      allow(Queue).to receive(:new).and_return(q)
    end

    it "EventHandler#converge_start with none in front" do
      allow(client).to receive(:on_state_change).and_return
      allow(client).to receive(:children).and_return([])

      expect(client).to receive(:children)
      expect(q).to receive(:<<)
      expect(q).to receive(:pop)
      handler.converge_start(context)
    end

    it "EventHandler#converge_start with no zk server data and many in front" do
      throttle_config.delete(:server)
      throttle_config[:exhibitor] = 'http://myhost.mydomain'
      allow(client).to receive(:on_state_change).and_return
      allow(client).to receive(:children).and_return(children.map{|c| c.to_s}, [node_id.to_s])
      watchable.each{|c| allow(client).to receive(:watch_child).with(c.to_s).and_return} if watchable.length >= limit

      zk_hosts = zkdata['servers'].map{|s| "#{s}:#{zkdata['port']}" } * ','
      zk_path = "#{cluster_path}/#{cluster_name}"

      expect(ZK::Client).to receive(:new).with(zk_hosts + zk_path)
      expect(q).to receive(:<<)
      expect(q).to receive(:pop)
      handler.converge_start(context)
    end

    it "EventHandler#converge_complete" do
      allow(client).to receive(:on_state_change).and_return
      allow(client).to receive(:children).and_return([])
      handler.converge_start(context)

      expect(client).to receive(:delete)
      handler.converge_complete
    end
  end
end
