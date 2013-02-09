# encoding: utf-8
module Moped

  # The Connection Pool is used to maintain control over the number of
  # connection connections the application has, and guarantees that connections
  # are always used for the same address. It also attempts to ensure that the
  # same thread would always get the same connection, but in rare cases this is
  # not guaranteed, for example if there are more threads than the maximum
  # connection pool size or threads are hanging around that have not been
  # garbage collected. For this purpose it is always recommended to have a
  # higher pool size than thread count.
  #
  # @since 2.0.0
  class ConnectionPool

    # The default maximum number of Connections is 5.
    MAX_SIZE = 5

    # Check a Connection back into the ConnectionPool.
    #
    # @example Check a Connection in.
    #   connection_pool.checkin(connection)
    #
    # @param [ Moped::Connection ] connection The Connection to check in.
    #
    # @return [ Moped::ConnectionPool ] The ConnectionPool.
    #
    # @since 2.0.0
    def checkin(connection)
      mutex.synchronize do
        connections.get(connection.address).set(connection)
        resource.broadcast and self
      end
    end

    # Checkout a Connection from the ConnectionPool.
    #
    # @example Checkout a Connection.
    #   connection_pool.checkout(83100018, "127.0.0.1:27017")
    #
    # @param [ Integer ] thread_id The object id of the executing Thread.
    # @param [ String ] address The address for the Connection.
    # @param [ Float ] timeout The wait period, in seconds.
    #
    # @raise [ MaxReached ] If the ConnectionPool is saturated and the wait
    #   period has expired.
    #
    # @return [ Moped::Connection ] The next Connection.
    #
    # @since 2.0.0
    def checkout(thread_id, address, timeout = 0.25)
      mutex.synchronize do
        connection = connections.get(address).get(thread_id)
        return connection if connection
        if saturated?
          wait_for_available(thread_id, address, Time.now + timeout)
        else
          create_connection(thread_id, address)
        end
      end
    end

    # Instantiate the new ConnectionPool.
    #
    # @example Instantiate the new pool.
    #   Moped::ConnectionPool.new(max_size: 10)
    #
    # @param [ Hash ] options The ConnectionPool options.
    #
    # @option options [ Integer ] :max_size The maximum number of Connections.
    #
    # @since 2.0.0
    def initialize(options = {})
      @mutex, @resource = Mutex.new, ConditionVariable.new
      @connections, @instantiated, @options = Connections.new, 0, options
    end

    # Get the maximum number of Connections that are allowed in the pool.
    #
    # @example Get the maximum number of Connections.
    #   connection_pool.max_size
    #
    # @return [ Integer ] The maximum number of Connections.
    #
    # @since 2.0.0
    def max_size
      @max_size ||= @options[:max_size] || MAX_SIZE
    end

    # Returns whether or not the maximum number of Connections in the pool been
    # reached or somehow gone over its limit.
    #
    # @example Is the ConnectionPool saturated?
    #   connection_pool.saturated?
    #
    # @return [ true, false ] If the ConnectionPool is saturated.
    #
    # @since 2.0.0
    def saturated?
      instantiated >= max_size
    end

    # Unpins all the Connections that are currently pinned to the provided
    # thread id.
    #
    # @example Unpin the Connections.
    #   connection_pool.unpin_connections(13200001)
    #
    # @param [ Integer ] thread_id The instance object id of the Thread.
    #
    # @return [ Moped::ConnectionPool ] The ConnectionPool.
    #
    # @since 2.0.0
    def unpin_connections(thread_id)
      connections.unpin(thread_id) and self
    end

    # Raised when the maximum number of Connections in the pool has been
    # reached, and another Connection has not been checked back into the pool
    # in a timely enough manner.
    #
    # @since 2.0.0
    class MaxReached < RuntimeError; end

    private

    # @!attribute connections
    #   @api private
    #   @return [ Hash<String, Connection> ] The Connections in the pool.
    #   @since 2.0.0
    #
    # @!attribute instantiated
    #   @api private
    #   @return [ Integer ] The number of instantiated Connections.
    #   @since 2.0.0
    #
    # @!attribute options
    #   @api private
    #   @return [ Hash ] The options hash.
    #   @since 2.0.0
    #
    # @!attribute mutex
    #   @api private
    #   @return [ Mutex ] The Mutex for the pool.
    #   @since 2.0.0
    #
    # @!attribute resource
    #   @api private
    #   @return [ ConditionVariable ] The ConditionVariable for broadcasting.
    #   @since 2.0.0
    attr_reader :connections, :instantiated, :options, :mutex, :resource

    # Create a new instance of a Connection given the thread instance id and
    # the address to connect to.
    #
    # @api private
    #
    # @example Create the new Connection.
    #   connection_pool.create_connection(1231110001, "127.0.0.1:27017")
    #
    # @param [ Integer ] thread_id The object_id of the Thread.
    # @param [ String ] address The address in the form "host:port".
    #
    # @return [ Moped::Connection ] The new Connection.
    #
    # @since 2.0.0
    def create_connection(thread_id, address)
      host, port = address.split(":")
      connection = Connection.new(host, port.to_i, options[:timeout], {})
      connection.pin_to(thread_id)
      @instantiated += 1
      connection
    end

    # Waits for an available Connection to be returned to the pool and returns
    # it. If the deadline passes, then an exception is raised.
    #
    # @api private
    #
    # @example Wait for an available Connection to be checked in.
    #   connection_pool.wait_for_available("127.0.0.1:27017", Time.now)
    #
    # @param [ String ] address The address of the Connection.
    # @param [ Time ] deadline The Time to wait before raising an error.
    #
    # @return [ Moped::Connection ] The next available Connection.
    #
    # @since 2.0.0
    def wait_for_available(thread_id, address, deadline)
      loop do
        connection = connections.get(address).get(thread_id)
        return connection if connection
        wait = deadline - Time.now
        raise MaxReached.new if wait <= 0
        resource.wait(mutex, wait)
      end
    end

    # This inner class wraps all the Connections in the ConnectionPool and
    # provides convenience access to those Connections.
    #
    # @since 2.0.0
    class Connections

      # Get the Pinning for the provided address.
      #
      # @example Get the Pinning.
      #   connections.get("127.0.0.1:27017")
      #
      # @param [ String ] address The address in host:port form.
      #
      # @return [ Pinning ] The Pinning for the address.
      #
      # @since 2.0.0
      def get(address)
        pinnings[address] ||= Pinning.new
      end

      # Instantiate the new Connections object.
      #
      # @example Instantiate the Connections.
      #   Connections.new
      #
      # @param [ Hash<Integer, Pinning> ] pinnings The Connection pinnings.
      #
      # @since 2.0.0
      def initialize(pinnings = {})
        @pinnings = pinnings
      end

      # Unpin all Connections for the provided thread id.
      #
      # @example Unpin all the Connections.
      #   connections.unpin(1231000001)
      #
      # @param [ Integer ] thread_id The thread id to unpin for.
      #
      # @return [ Array<Pinning> ] The Pinnings.
      #
      # @since 2.0.0
      def unpin(thread_id)
        pinnings.values.each do |pinning|
          pinning.unpin(thread_id)
        end
      end

      private

      # @!attribute pinnings
      #   @api private
      #   @return [ Hash<String, Pinning> ] The Connection pinnings.
      #   @since 2.0.0
      attr_reader :pinnings

      # A Pinning represents a collection of thread_ids and their corresponding
      # pinned Connections.
      #
      # @since 2.0.0
      class Pinning

        # Get a Connection for the provided thread id. If none is available,
        # then we take an instantiated unpinned Connection.
        #
        # @example Get a Connection for the thread.
        #   pinning.get(1231000001)
        #
        # @param [ Integer ] thread_id The object_id of the Thread.
        #
        # @return [ Connection ] The pinned Connection.
        def get(thread_id)
          threads[thread_id] ||= next_unpinned(thread_id)
        end

        # Instantiate a new Pinning.
        #
        # @example Instantiate a new Pinning.
        #   Pinning.new
        #
        # @param [ Hash<Integer, Connection> ] threads The thread pinnings.
        # @param [ Array<Connection> ] unpinned A collection of unpinned
        #   Connections.
        #
        # @since 2.0.0
        def initialize(threads = {}, unpinned = [])
          @threads, @unpinned = threads, unpinned
        end

        # Set a Connection in the Pinning.
        #
        # @example Set the Connection.
        #   pinning.set(connection)
        #
        # @param [ Moped::Connection ] connection The Connection to set.
        #
        # @return [ Moped::Connection ] The Connection.
        #
        # @since 2.0.0
        def set(connection)
          threads[connection.pinned_to] = connection
        end

        # Unpin a Connection from the provided thread id.
        #
        # @example Unpin from the Thread.
        #   pinning.unpin(13201110111)
        #
        # @param [ Integer ] thread_id The Thread's object_id.
        #
        # @return [ Array<Connection> ] All the unpinned Connections.
        #
        # @since 2.0.0
        def unpin(thread_id)
          connection = threads.delete(thread_id)
          connection.unpin
          unpinned.push(connection)
        end

        private

        # @!attribute threads
        #   @api private
        #   @return [ Hash<Integer, Connection> ] The Connection pinnings.
        #   @since 2.0.0
        #
        # @!attribute unpinned
        #   @api private
        #   @return [ Array<Connection> ] Instantiated Connections that are not
        #     pinned.
        #   @since 2.0.0
        attr_reader :threads, :unpinned

        # Get an unpinned thread, pin it to the provided Thread id, and return
        # it.
        #
        # @api private
        #
        # @example Get the next unpinned Connection.
        #   pinning.nex_unpinned(11130000011)
        #
        # @param [ Integer ] thread_id The object_id of the Thread.
        #
        # @return [ Connection ] The next Connection.
        #
        # @since 2.0.0
        def next_unpinned(thread_id)
          connection = unpinned.pop
          connection.pin_to(thread_id) if connection
          connection
        end
      end
    end
  end
end
