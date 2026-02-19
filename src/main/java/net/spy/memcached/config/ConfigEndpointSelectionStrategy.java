package net.spy.memcached.config;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedConnection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * A configuration endpoint selection strategy which
 * holds information and lifecycle relating to the
 * configuration endpoint connection opened.
 */
public interface ConfigEndpointSelectionStrategy {
    /**
     * An endpoint is different from a connection;
     * A connection can may be composed of several endpoints
     */
    NodeEndPoint getConfigEndpoint(MemcachedClient client);
    /**
      * Delegate the responsibility of setting up a memcached connection to this class
      * as depending on strategy, a copy of this memcached connection may be required
      * to be created and kept alive
      * @param cf connection factory used to build the Memcached connection
      * @param addrs list of addresses that the connection factory should initialise connections with
      * @return a Memcached connection in which memcached operations will be enqueued to
      * @throws IOException
      */
    MemcachedConnection setupMemcachedConnection(ConnectionFactory cf, List<InetSocketAddress> addrs) throws IOException;
    void shutdownConfigConnection() throws IOException;
    MemcachedConnection getConfigConnection();
}
