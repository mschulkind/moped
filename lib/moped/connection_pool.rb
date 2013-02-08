# encoding: utf-8
module Moped

  # The Connection Pool is used to maintain control over the number of
  # connection connections the application has, and guarantees that connections
  # are always used for the same address. It also attempts to ensure that the
  # same thread would always get the same connection, but in rare cases this is
  # not guaranteed, for example if there are more threads than the maximum
  # connection pool size.
  #
  # @since 2.0.0
  class ConnectionPool

    # The default maximum number of Connections is 5.
    MAX_SIZE = 5

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
      @connections, @instantiated, @options = {}, 0, options
    end

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
        conns = connections[connection.address] ||= []
        conns.push(connection)
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
        conns = connections.fetch(address, [])
        return conns.first unless conns.empty?
        if saturated?
          wait_for_available(address, Time.now + timeout)
        else
          create_connection(thread_id, address)
        end
      end
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
    def wait_for_available(address, deadline)
      loop do
        conns = connections.fetch(address, [])
        return conns.first unless conns.empty?
        wait = deadline - Time.now
        raise MaxReached.new if wait <= 0
        resource.wait(mutex, wait)
      end
    end
  end
end
