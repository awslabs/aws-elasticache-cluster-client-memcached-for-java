package net.spy.memcached.config;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedConnection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class RoundRobinConfigEndpointSelectionStrategy implements ConfigEndpointSelectionStrategy {
    private int currentIndex = 0;
    private MemcachedConnection configurationConnection;

    @Override
    public NodeEndPoint getConfigEndpoint(MemcachedClient client) {
        Collection<NodeEndPoint> endpoints = client.getAvailableNodeEndPoints();
        if (endpoints.isEmpty()) {
            //If no nodes are available status, then get all the endpoints. This provides an
            //opportunity to re-resolve the hostname by recreating InetSocketAddress instance in "NodeEndPoint".getInetSocketAddress().
            endpoints = client.getAllNodeEndPoints();
        }
        currentIndex = (currentIndex + 1) % endpoints.size();
        Iterator<NodeEndPoint> iterator = endpoints.iterator();
        for (int i = 0; i < currentIndex; i++) {
            iterator.next();
        }
        return iterator.next();
    }

    /**
     * The Round Robin strategy (AWS default) reuses the same connection to retrieve configuration.
     * In this case, we only need to hold a reference to the connection to return when
     * `getConfigConnection` is called
     *
     */
    public MemcachedConnection setupMemcachedConnection(ConnectionFactory cf, List<InetSocketAddress> addrs) throws IOException {
        configurationConnection = cf.createConnection(addrs);
        return configurationConnection;
    }

    @Override
    public void shutdownConfigConnection() {
        // As the Memcached connection doubles as a configuration connection,
        // this operation is a no-op as that should be handled by the consumer of the
        // Memcached connection
    }

    @Override
    public MemcachedConnection getConfigConnection() {
        return configurationConnection;
    }
}
