package net.spy.memcached.config;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Collection;

import net.spy.memcached.UnitTestConfig;
import org.jmock.Mock;
import org.jmock.MockObjectTestCase;
import net.spy.memcached.MemcachedClientIF;
import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.DefaultConnectionFactory;

import static net.spy.memcached.UnitTestConfig.PORT_NUMBER;

public class RoundRobinConfigEndpointSelectionStrategyTest extends MockObjectTestCase {

    private RoundRobinConfigEndpointSelectionStrategy strategy;
    private Mock clientMock;
    private MemcachedClientIF client;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        strategy = new RoundRobinConfigEndpointSelectionStrategy();
        clientMock = mock(MemcachedClientIF.class);
        client = (MemcachedClientIF) clientMock.proxy();
    }

    public void testGetConfigEndpointRoundRobin() {
        List<NodeEndPoint> endpoints = new ArrayList<NodeEndPoint>();
        endpoints.add(new NodeEndPoint("host1", PORT_NUMBER));
        endpoints.add(new NodeEndPoint("host2", PORT_NUMBER));
        endpoints.add(new NodeEndPoint("host3", PORT_NUMBER));

        clientMock.expects(atLeastOnce()).method("getAvailableNodeEndPoints").will(returnValue(endpoints));

        // Round robin should start from (0 + 1) % 3 = 1
        assertEquals("host2", strategy.getConfigEndpoint(client).getHostName());
        // Next (1 + 1) % 3 = 2
        assertEquals("host3", strategy.getConfigEndpoint(client).getHostName());
        // Next (2 + 1) % 3 = 0
        assertEquals("host1", strategy.getConfigEndpoint(client).getHostName());
        // Next (0 + 1) % 3 = 1
        assertEquals("host2", strategy.getConfigEndpoint(client).getHostName());
    }

    public void testGetConfigEndpointWhenAvailableIsEmpty() {
        List<NodeEndPoint> availableEndpoints = new ArrayList<NodeEndPoint>();
        List<NodeEndPoint> allEndpoints = new ArrayList<NodeEndPoint>();
        allEndpoints.add(new NodeEndPoint("host1", PORT_NUMBER));
        allEndpoints.add(new NodeEndPoint("host2", PORT_NUMBER));

        clientMock.expects(once()).method("getAvailableNodeEndPoints").will(returnValue(availableEndpoints));
        clientMock.expects(once()).method("getAllNodeEndPoints").will(returnValue(allEndpoints));

        // Should fall back to allEndpoints and start at (0 + 1) % 2 = 1
        assertEquals("host2", strategy.getConfigEndpoint(client).getHostName());
        
        // Reset or just expect another call if we want to test next element
        clientMock.expects(once()).method("getAvailableNodeEndPoints").will(returnValue(availableEndpoints));
        clientMock.expects(once()).method("getAllNodeEndPoints").will(returnValue(allEndpoints));
        assertEquals("host1", strategy.getConfigEndpoint(client).getHostName());
    }

    public void testSetupAndGetMemcachedConnection() throws Exception {
        DefaultConnectionFactory cf = new DefaultConnectionFactory();
        List<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();
        addrs.add(new InetSocketAddress(UnitTestConfig.IPV4_ADDR, PORT_NUMBER));

        MemcachedConnection conn = strategy.setupMemcachedConnection(cf, addrs);
        assertNotNull(conn);
        assertSame(conn, strategy.getConfigConnection());
    }
}
