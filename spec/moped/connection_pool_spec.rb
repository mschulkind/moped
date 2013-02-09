require "spec_helper"

describe Moped::ConnectionPool do

  describe "#checkin" do

    let(:pool) do
      described_class.new
    end

    let(:thread_id) do
      Thread.current.object_id
    end

    let(:connection) do
      Moped::Connection.new("127.0.0.1", 27017, 5).tap do |conn|
        conn.pin_to(thread_id)
      end
    end

    context "when the connection is not in the pool" do

      let!(:checked_in) do
        pool.checkin(connection)
      end

      let(:connections) do
        pool.send(:connections)
      end

      it "adds the connection to the pool" do
        expect(connections.get("127.0.0.1:27017").get(thread_id)).to eq(connection)
      end

      it "adds the exact instance to the pool" do
        expect(connections.get("127.0.0.1:27017").get(thread_id)).to eql(connection)
      end
    end
  end

  describe "#checkout" do

    let(:pool) do
      described_class.new(max_size: 2)
    end

    let(:connection) do
      Moped::Connection.new("127.0.0.1", 27017, nil)
    end

    context "when the pool has no available connections" do

      let(:thread_id) do
        Thread.current.object_id
      end

      context "when the total connections is less than the max" do

        let(:checked_out) do
          pool.checkout(thread_id, connection.address)
        end

        it "returns a new connection" do
          expect(checked_out).to eq(connection)
        end

        it "pins the connection to the provided thread_id" do
          expect(checked_out.pinned_to).to eq(thread_id)
        end

        it "returns a new instance" do
          expect(checked_out).not_to eql(connection)
        end
      end

      context "when the connection pool is saturated" do

        context "when no connection is checked in while waiting" do

          before do
            2.times { pool.checkout(thread_id, connection.address) }
          end

          it "raises an error" do
            expect {
              pool.checkout(thread_id, connection.address)
            }.to raise_error(Moped::ConnectionPool::MaxReached)
          end
        end

        context "when a connection is checked in while waiting" do

          let!(:conn_one) do
            pool.checkout(thread_id, connection.address)
          end

          let!(:conn_two) do
            pool.checkout(thread_id, connection.address)
          end

          let(:thread_one) do
            Thread.new do
              expect(pool.checkout(thread_id, connection.address, 1.5)).to eq(conn_one)
            end
          end

          let(:thread_two) do
            Thread.new do
              pool.checkin(conn_one)
            end
          end

          it "returns the connection" do
            thread_one.join(1)
            thread_two.join
          end
        end
      end
    end

    context "when the pool has available connections for the thread" do

      let(:thread_id) do
        Thread.current.object_id
      end

      before do
        connection.pin_to(thread_id)
        pool.checkin(connection)
      end

      let(:checked_out) do
        pool.checkout(thread_id, connection.address)
      end

      it "returns an available connection" do
        expect(checked_out).to eq(connection)
      end

      it "returns the available instance" do
        expect(checked_out).to eql(connection)
      end
    end
  end

  describe "#max_size" do

    context "when the option is provided" do

      let(:pool) do
        described_class.new(max_size: 10)
      end

      it "returns the option" do
        expect(pool.max_size).to eq(10)
      end
    end

    context "when the option was not provided" do

      let(:pool) do
        described_class.new
      end

      it "returns the default of 5" do
        expect(pool.max_size).to eq(5)
      end
    end
  end

  describe "#saturated?" do

    let(:pool) do
      described_class.new(max_size: 2)
    end

    let(:thread_id) do
      Thread.current.object_id
    end

    let(:address) do
      "127.0.0.1:27017"
    end

    context "when the pool is below the max size" do

      before do
        pool.checkout(thread_id, address)
      end

      it "returns false" do
        expect(pool).to_not be_saturated
      end
    end

    context "when the pool is equal to the max size" do

      before do
        2.times { pool.checkout(thread_id, address) }
      end

      it "returns true" do
        expect(pool).to be_saturated
      end
    end
  end

  describe "#unpin_connections" do

    let(:pool) do
      described_class.new
    end

    let(:thread_id) do
      Thread.current.object_id
    end

    let(:address) do
      "127.0.0.1:27017"
    end

    let(:connection) do
      Moped::Connection.new("127.0.0.1", 27017, 5)
    end

    before do
      connection.pin_to(thread_id)
      pool.checkin(connection)
    end

    let!(:unpinned) do
      pool.unpin_connections(thread_id)
    end

    it "unpins associated connections from the thread" do
      expect(connection).to_not be_pinned
    end

    it "returns the connection pool" do
      expect(unpinned).to eq(pool)
    end
  end
end
