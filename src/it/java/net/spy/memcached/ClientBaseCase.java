/**
 * Copyright (C) 2006-2009 Dustin Sallings
 * Copyright (C) 2009-2011 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 * 
 * 
 * Portions Copyright (C) 2012-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * SPDX-License-Identifier: Apache-2.0
 */

package net.spy.memcached;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLContext;

import net.spy.memcached.categories.StandardTests;
import net.spy.memcached.config.NodeEndPoint;
import net.spy.memcached.ops.ConfigurationType;

import org.junit.Before;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;


/**
 * A ClientBaseCase.
 */
@Category(StandardTests.class)
public abstract class ClientBaseCase {

  protected MemcachedClient client = null;
  protected Boolean membase;
  protected Boolean moxi;

  protected void initClient() throws Exception {
    initClient(new ClientTestConnectionFactory());
  }

  protected void initClient(ConnectionFactory cf) throws Exception {
	  client = new MemcachedClient(cf, AddrUtil.getAddresses(TestConfig.IPV4_ADDR
			  + ":" + TestConfig.PORT_NUMBER));
  }

  protected Collection<String> stringify(Collection<?> c) {
    Collection<String> rv = new ArrayList<String>();
    for (Object o : c) {
      rv.add(String.valueOf(o));
    }
    return rv;
  }
  
  @BeforeClass
  public static void setUpConfigEndpoint() throws Exception {
    List<InetSocketAddress> addrs = AddrUtil.getAddresses(TestConfig.IPV4_ADDR
	     + ":" + TestConfig.PORT_NUMBER);
    MemcachedClient mem_client = new MemcachedClient(addrs);
    mem_client.set(ConfigurationType.CLUSTER.getValueWithNameSpace(), 0, "1\n" + "localhost.localdomain|" + TestConfig.IPV4_ADDR + "|" + TestConfig.PORT_NUMBER);
    Thread.sleep(1000); // Wait for the configuration to apply
  }
  
  @Before
  public void setUp() throws Exception {
    if (TestConfig.isTlsMode() && TestConfig.getInstance().getClientMode() == ClientMode.Dynamic) {
      setClusterConfigForTLS(TestConfig.PORT_NUMBER);
    }
    initClient();
  }

  @After
  public void tearDown() throws Exception {
	// get the current configuration if in 1.4.5 engine
    String current_config = null;
    Collection<NodeEndPoint> endpoints = new ArrayList<NodeEndPoint>();
    if(TestConfig.getInstance().getClientMode().equals(ClientMode.Dynamic) && 
       !TestConfig.getInstance().getEngineType().isSetConfigSupported()) {
    	current_config = getCurrentConfigAndClusterEndpoints(client, endpoints);
    }
    // Shut down, start up, flush, and shut down again. Error tests have
    // unpredictable timing issues.
    client.shutdown();
    client = null;
    initClient();
    flushPause();
    assertTrue(client.flush().get());
    // Restore the configuration on the server if in 1.4.5 engine
    if(TestConfig.getInstance().getClientMode().equals(ClientMode.Dynamic) && 
       !TestConfig.getInstance().getEngineType().isSetConfigSupported()) {
    	Thread.sleep(1000);  // Wait for flush to finish
    	restoreClusterConfig(current_config, endpoints);
    	Thread.sleep(1000); // Wait for the config to restore
    }
    client.shutdown();
    client = null;
  }

  protected String getCurrentConfigAndClusterEndpoints(MemcachedClient memcacheClient, Collection<NodeEndPoint> endpoints) {
    String current_config = null;
    if(memcacheClient == null)
        return current_config;

  	try {
  	    /* go to the config endpoint and retrieve the configuration */
  	    MemcachedClient currentClient = new MemcachedClient(new ClientTestConnectionFactory(), AddrUtil.getAddresses(TestConfig.IPV4_ADDR + ":" + TestConfig.PORT_NUMBER));
  	    current_config = currentClient.get(ConfigurationType.CLUSTER.getValueWithNameSpace()).toString();
	} catch (NullPointerException e) {
        System.err.println("NullPointer exception: " + e.getMessage());
	} catch (IOException e) {
        e.printStackTrace();
	}
    endpoints.addAll(memcacheClient.getAllNodeEndPoints());
    return current_config;
  }

  protected void restoreClusterConfig(String config, Collection<NodeEndPoint> endpoints) throws Exception {
	if(!endpoints.isEmpty() && config != null) {
	  ArrayList<MemcachedClient> clients = new ArrayList<MemcachedClient>();
      for (NodeEndPoint endpoint : endpoints) {
        List<InetSocketAddress> addrs = AddrUtil.getAddresses(endpoint.getIpAddress() + ":" + endpoint.getPort());
        MemcachedClient currentClient = staticMemcachedClient(addrs);
        currentClient.set(ConfigurationType.CLUSTER.getValueWithNameSpace(), 0, config);
        clients.add(currentClient);
      }
      Thread.sleep(2000); // wait for all configurations to apply
      for(int i = 0; i < clients.size(); i++) {
        clients.get(i).shutdown();
      }
	}
  }

  protected void setClusterConfigForTLS(int portNumber) throws Exception {
      List<InetSocketAddress> addrs = AddrUtil.getAddresses(TestConfig.IPV4_ADDR
      + ":" + portNumber);

      MemcachedClient client = staticMemcachedClient(addrs);
      client.set(ConfigurationType.CLUSTER.getValueWithNameSpace(), 0, "1\n" + "localhost.localdomain|" + TestConfig.IPV4_ADDR + "|" + portNumber);
      Thread.sleep(1000); // wait for all configurations to apply
  }

  protected static MemcachedClient staticMemcachedClient(List<InetSocketAddress> addrs) throws IOException {
    return new MemcachedClient(new ClientTestConnectionFactory() { 
      @Override
      public ClientMode getClientMode() {
        return ClientMode.Static;
      }
    }, addrs);
  }

  protected void flushPause() throws InterruptedException {
    // nothing useful
  }

  protected boolean isMoxi() {
    if (moxi != null) {
      return moxi.booleanValue();
    }
    // some tests are invalid if using moxi

    Map<SocketAddress, Map<String, String>> stats = client.getStats("proxy");
    for (Map<String, String> node : stats.values()) {
      if (node.get("basic:version") != null) {
        moxi = true;
        System.err.println("Using proxy");
        break;
      } else {
        moxi = false;
        System.err.println("Not using proxy");
      }
    }
    return moxi.booleanValue();
  }
}
