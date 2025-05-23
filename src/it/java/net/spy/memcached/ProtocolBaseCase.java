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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.categories.StandardTests;
import net.spy.memcached.compat.SyncThread;
import net.spy.memcached.config.NodeEndPoint;
import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.internal.BulkGetCompletionListener;
import net.spy.memcached.internal.BulkGetFuture;
import net.spy.memcached.internal.GetCompletionListener;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.ops.ConfigurationType;
import net.spy.memcached.ops.OperationErrorType;
import net.spy.memcached.ops.OperationException;
import net.spy.memcached.ops.StatusCode;
import net.spy.memcached.transcoders.SerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;

import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * A ProtocolBaseCase.
 */
@Category(StandardTests.class)
public abstract class ProtocolBaseCase extends ClientBaseCase {

  @Test
  public void testAssertions() {
    boolean caught = false;
    try {
      assert false;
    } catch (AssertionError e) {
      caught = true;
    }
    assertTrue("Assertions are not enabled!", caught);
  }

  @Test
  public void testGetStats() throws Exception {
    Map<SocketAddress, Map<String, String>> stats = client.getStats();
    System.out.println("Stats:  " + stats);
    assertEquals(1, stats.size());
    Map<String, String> oneStat = stats.values().iterator().next();
    assertTrue(oneStat.containsKey("curr_items"));
  }

  @Test
  public void testGetStatsSlabs() throws Exception {
    if (isMoxi()) {
      return;
    }
    // There needs to at least have been one value set or there may be
    // no slabs to check.
    client.set("slabinitializer", 0, "hi");
    Map<SocketAddress, Map<String, String>> stats = client.getStats("slabs");
    System.out.println("Stats:  " + stats);
    assertEquals(1, stats.size());
    Map<String, String> oneStat = stats.values().iterator().next();
    assertTrue(oneStat.containsKey("1:chunk_size"));
  }

  @Test
  public void testGetStatsSizes() throws Exception {
    if (isMoxi()) {
      return;
    }
    // There needs to at least have been one value set or there may
    // be no sizes to check. Note the protocol says
    // flushed/expired items may come back in stats sizes and we
    // use flush when testing, so we check that there's at least
    // one.
    client.set("sizeinitializer", 0, "hi");
    Map<SocketAddress, Map<String, String>> stats = client.getStats("sizes");
    System.out.println("Stats sizes:  " + stats);
    assertEquals(1, stats.size());
    Map<String, String> oneStat = stats.values().iterator().next();
    String noItemsSmall = oneStat.get("96");
    assertTrue(Integer.parseInt(noItemsSmall) >= 1);
  }

  @Test
  public void testDelayedFlush() throws Exception {
    String current_config = null;
    Collection<NodeEndPoint> endpoints = new ArrayList<NodeEndPoint>();
    if(TestConfig.getInstance().getClientMode().equals(ClientMode.Dynamic) &&
       !TestConfig.getInstance().getEngineType().isSetConfigSupported()) {
      current_config = getCurrentConfigAndClusterEndpoints(client, endpoints);
    }
    assertNull(client.get("test1"));
    assert client.set("test1", 5, "test1value").getStatus().isSuccess();
    assert client.set("test2", 5, "test2value").getStatus().isSuccess();
    assertEquals("test1value", client.get("test1"));
    assertEquals("test2value", client.get("test2"));
    assert client.flush(2).getStatus().isSuccess();
    Thread.sleep(2100);
    if(TestConfig.getInstance().getClientMode().equals(ClientMode.Dynamic) &&
       !TestConfig.getInstance().getEngineType().isSetConfigSupported()) {
      Thread.sleep(1000);
      restoreClusterConfig(current_config, endpoints);
      Thread.sleep(1000); // Wait for the config to restore
    }

    // Restore the server configuration for TLS enabled Dynamic mode after flush
    if (TestConfig.isTlsMode() && TestConfig.getInstance().getClientMode() == ClientMode.Dynamic) {
      setClusterConfigForTLS(TestConfig.PORT_NUMBER);
    }

    assertNull(client.get("test1"));
    assertNull(client.get("test2"));
    assert !client.asyncGet("test1").getStatus().isSuccess();
    assert !client.asyncGet("test2").getStatus().isSuccess();
  }

  @Test
  public void testNoop() {
    // This runs through the startup/flush cycle
  }

  @Test
  public void testDoubleShutdown() {
    client.shutdown();
    client.shutdown();
  }

  @Test
  public void testSimpleGet() throws Exception {
    assertNull(client.get("test1"));
    client.set("test1", 5, "test1value");
    assertEquals("test1value", client.get("test1"));
  }

  @Test
  public void testSimpleCASGets() throws Exception {
    assertNull(client.gets("test1"));
    assert client.set("test1", 5, "test1value").getStatus().isSuccess();
    assertEquals("test1value", client.gets("test1").getValue());
  }

  @Test
  public void testCAS() throws Exception {
    final String key = "castestkey";
    // First, make sure it doesn't work for a non-existing value.
    assertSame("Expected error CASing with no existing value.",
        CASResponse.NOT_FOUND, client.cas(key, 0x7fffffffffL, "bad value"));

    // OK, stick a value in here.
    assertTrue(client.add(key, 5, "original value").get());
    CASValue<?> getsVal = client.gets(key);
    assertEquals("original value", getsVal.getValue());

    // Now try it with an existing value, but wrong CAS id
    assertSame("Expected error CASing with invalid id", CASResponse.EXISTS,
        client.cas(key, getsVal.getCas() + 1, "broken value"));
    // Validate the original value is still in tact.
    assertEquals("original value", getsVal.getValue());

    // OK, now do a valid update
    assertSame("Expected successful CAS with correct id (" + getsVal.getCas()
        + ")", CASResponse.OK, client.cas(key, getsVal.getCas(), "new value"));
    assertEquals("new value", client.get(key));

    // Test a CAS replay
    assertSame("Expected unsuccessful CAS with replayed id",
        CASResponse.EXISTS, client.cas(key, getsVal.getCas(), "wrong value"));
    assertEquals("new value", client.get(key));
  }

  @Test
  public void testReallyLongCASId() throws Exception {
    String key = "this-is-my-key";
    assertSame("Expected error CASing with no existing value.",
        CASResponse.NOT_FOUND,
        client.cas(key, 9223372036854775807L, "bad value"));
  }

  @Test
  public void testExtendedUTF8Key() throws Exception {
    String key = "\u2013\u00ba\u2013\u220f\u2014\u00c4";
    assertNull(client.get(key));
    assert client.set(key, 5, "test1value").getStatus().isSuccess();
    assertEquals("test1value", client.get(key));
  }

  @Test
  public void testKeyWithSpaces() throws Exception {
    try {
      client.get("key with spaces");
      fail("Expected IllegalArgumentException getting key with spaces");
    } catch (IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testKeyLongerThan250() throws Exception {
    try {
      StringBuilder longKey = new StringBuilder();
      for (int i = 0; i < 251; i++) {
        longKey.append("a");
      }
      client.get(longKey.toString());
      fail("Expected IllegalArgumentException getting too long of a key");
    } catch (IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testKeyWithNewline() throws Exception {
    try {
      Object val = client.get("Key\n");
      fail("Expected IllegalArgumentException, got " + val);
    } catch (IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testKeyWithReturn() throws Exception {
    try {
      Object val = client.get("Key\r");
      fail("Expected IllegalArgumentException, got " + val);
    } catch (IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testKeyWithASCIINull() throws Exception {
    try {
      Object val = client.get("Key\0");
      fail("Expected IllegalArgumentException, got " + val);
    } catch (IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testInvalidKeyBlank() throws Exception {
    try {
      Object val = client.get("");
      fail("Expected IllegalArgumentException, got " + val);
    } catch (IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testGetBulkKeyWSpaces() throws Exception {
    try {
      Object val = client.getBulk("Key key2");
      fail("Expected IllegalArgumentException, got " + val);
    } catch (IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testParallelSetGet() throws Throwable {
    int cnt = SyncThread.getDistinctResultCount(10, new Callable<Boolean>() {
      public Boolean call() throws Exception {
        for (int i = 0; i < 10; i++) {
          assert client.set("test" + i, 5, "value" + i).getStatus().isSuccess();
          assertEquals("value" + i, client.get("test" + i));
        }
        for (int i = 0; i < 10; i++) {
          assertEquals("value" + i, client.get("test" + i));
        }
        return Boolean.TRUE;
      }
    });
    assertEquals(1, cnt);
  }

  @Test
  public void testParallelSetMultiGet() throws Throwable {
    int cnt = SyncThread.getDistinctResultCount(10, new Callable<Boolean>() {
      public Boolean call() throws Exception {
        for (int i = 0; i < 10; i++) {
          assert client.set("test" + i, 5, "value" + i).getStatus().isSuccess();
          assertEquals("value" + i, client.get("test" + i));
        }
        // Yes, I intentionally ran  over.
        Map<String, Object> m =
            client.getBulk("test0", "test1", "test2", "test3", "test4",
                "test5", "test6", "test7", "test8", "test9", "test10");
        for (int i = 0; i < 10; i++) {
          assertEquals("value" + i, m.get("test" + i));
        }
        return Boolean.TRUE;
      }
    });
    assertEquals(1, cnt);
  }

  @Test
  public void testParallelSetAutoMultiGet() throws Throwable {
    int cnt = SyncThread.getDistinctResultCount(10, new Callable<Boolean>() {
      public Boolean call() throws Exception {
        assert client.set("testparallel", 5, "parallelvalue").getStatus()
            .isSuccess();
        for (int i = 0; i < 10; i++) {
          assertEquals("parallelvalue", client.get("testparallel"));
        }
        return Boolean.TRUE;
      }
    });
    assertEquals(1, cnt);
  }

  @Test
  public void testAdd() throws Exception {
    assertNull(client.get("test1"));
    assert !client.asyncGet("test1").getStatus().isSuccess();
    assertTrue(client.set("test1", 5, "test1value").get());
    assertEquals("test1value", client.get("test1"));
    assert client.asyncGet("test1").getStatus().isSuccess();
    assertFalse(client.add("test1", 5, "ignoredvalue").get());
    assert !client.add("test1", 5, "ignoredvalue").getStatus().isSuccess();
    // Should return the original value
    assertEquals("test1value", client.get("test1"));
  }

  @Test
  public void testAddWithTranscoder() throws Exception {
    Transcoder<String> t = new TestTranscoder();
    assertNull(client.get("test1", t));
    assert !client.asyncGet("test1", t).getStatus().isSuccess();
    assertTrue(client.set("test1", 5, "test1value", t).get());
    assertEquals("test1value", client.get("test1", t));
    assertFalse(client.add("test1", 5, "ignoredvalue", t).get());
    assert !client.add("test1", 5, "ignoredvalue", t).getStatus().isSuccess();
    // Should return the original value
    assertEquals("test1value", client.get("test1", t));
  }

  @Test
  public void testAddNotSerializable() throws Exception {
    try {
      client.add("t1", 5, new Object());
      fail("expected illegal argument exception");
    } catch (IllegalArgumentException e) {
      assertEquals("Non-serializable object", e.getMessage());
    }
  }

  @Test
  public void testSetNotSerializable() throws Exception {
    try {
      client.set("t1", 5, new Object());
      fail("expected illegal argument exception");
    } catch (IllegalArgumentException e) {
      assertEquals("Non-serializable object", e.getMessage());
    }
  }

  @Test
  public void testReplaceNotSerializable() throws Exception {
    try {
      client.replace("t1", 5, new Object());
      fail("expected illegal argument exception");
    } catch (IllegalArgumentException e) {
      assertEquals("Non-serializable object", e.getMessage());
    }
  }

  @Test
  public void testUpdate() throws Exception {
    assertNull(client.get("test1"));
    client.replace("test1", 5, "test1value");
    assert !client.replace("test1", 5, "test1value").getStatus().isSuccess();
    assertNull(client.get("test1"));
  }

  @Test
  public void testUpdateWithTranscoder() throws Exception {
    Transcoder<String> t = new TestTranscoder();
    assertNull(client.get("test1", t));
    client.replace("test1", 5, "test1value", t);
    assert !client.replace("test1", 5, "test1value", t).getStatus().isSuccess();
    assertNull(client.get("test1", t));
  }

  @Test // Just to make sure the sequence is being handled correctly
  public void testMixedSetsAndUpdates() throws Exception {
    Collection<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
    Collection<String> keys = new ArrayList<String>();
    for (int i = 0; i < 100; i++) {
      String key = "k" + i;
      futures.add(client.set(key, 10, key));
      futures.add(client.add(key, 10, "a" + i));
      keys.add(key);
    }
    Map<String, Object> m = client.getBulk(keys);
    assertEquals(100, m.size());
    for (Map.Entry<String, Object> me : m.entrySet()) {
      assertEquals(me.getKey(), me.getValue());
    }
    for (Iterator<Future<Boolean>> i = futures.iterator(); i.hasNext();) {
      assertTrue(i.next().get());
      assertFalse(i.next().get());
    }
  }

  @Test
  public void testGetBulk() throws Exception {
    Collection<String> keys = Arrays.asList("test1", "test2", "test3");
    assertEquals(0, client.getBulk(keys).size());
    client.set("test1", 5, "val1");
    client.set("test2", 5, "val2");
    Map<String, Object> vals = client.getBulk(keys);
    assert client.asyncGetBulk(keys).getStatus().isSuccess();
    assertEquals(2, vals.size());
    assertEquals("val1", vals.get("test1"));
    assertEquals("val2", vals.get("test2"));
  }

  @Test
  public void testGetBulkVararg() throws Exception {
    assertEquals(0, client.getBulk("test1", "test2", "test3").size());
    client.set("test1", 5, "val1");
    client.set("test2", 5, "val2");
    Map<String, Object> vals = client.getBulk("test1", "test2", "test3");
    assert client.asyncGetBulk("test1", "test2", "test3").getStatus()
        .isSuccess();
    assertEquals(2, vals.size());
    assertEquals("val1", vals.get("test1"));
    assertEquals("val2", vals.get("test2"));
  }

  @Test
  public void testGetBulkVarargWithTranscoder() throws Exception {
    Transcoder<String> t = new TestTranscoder();
    assertEquals(0, client.getBulk(t, "test1", "test2", "test3").size());
    client.set("test1", 5, "val1", t);
    client.set("test2", 5, "val2", t);
    Map<String, String> vals = client.getBulk(t, "test1", "test2", "test3");
    assert client.asyncGetBulk(t, "test1", "test2", "test3").getStatus()
        .isSuccess();
    assertEquals(2, vals.size());
    assertEquals("val1", vals.get("test1"));
    assertEquals("val2", vals.get("test2"));
  }

  @Test
  public void testAsyncGetBulkVarargWithTranscoder() throws Exception {
    Transcoder<String> t = new TestTranscoder();
    assertEquals(0, client.getBulk(t, "test1", "test2", "test3").size());
    client.set("test1", 5, "val1", t);
    client.set("test2", 5, "val2", t);
    BulkFuture<Map<String, String>> vals =
        client.asyncGetBulk(t, "test1", "test2", "test3");
    assert vals.getStatus().isSuccess();
    assertEquals(2, vals.get().size());
    assertEquals("val1", vals.get().get("test1"));
    assertEquals("val2", vals.get().get("test2"));
  }

  @Test
  public void testAsyncGetBulkWithTranscoderIterator() throws Exception {
    ArrayList<String> keys = new ArrayList<String>();
    keys.add("test1");
    keys.add("test2");
    keys.add("test3");

    ArrayList<Transcoder<String>> tcs =
        new ArrayList<Transcoder<String>>(keys.size());
    for (String key : keys) {
      tcs.add(new TestWithKeyTranscoder(key));
    }

    // Any transcoders listed after list of keys should be
    // ignored.
    for (String key : keys) {
      tcs.add(new TestWithKeyTranscoder(key));
    }

    assertEquals(0, client.asyncGetBulk(keys, tcs.listIterator()).get().size());

    client.set(keys.get(0), 5, "val1", tcs.get(0));
    client.set(keys.get(1), 5, "val2", tcs.get(1));
    Future<Map<String, String>> vals =
        client.asyncGetBulk(keys, tcs.listIterator());
    assertEquals(2, vals.get().size());
    assertEquals("val1", vals.get().get(keys.get(0)));
    assertEquals("val2", vals.get().get(keys.get(1)));

    // Set with one transcoder with the proper key and get
    // with another transcoder with the wrong key.
    keys.add(0, "test4");
    Transcoder<String> encodeTranscoder =
        new TestWithKeyTranscoder(keys.get(0));
    client.set(keys.get(0), 5, "val4", encodeTranscoder).get();

    Transcoder<String> decodeTranscoder =
        new TestWithKeyTranscoder("not " + keys.get(0));
    tcs.add(0, decodeTranscoder);
    try {
      client.asyncGetBulk(keys, tcs.listIterator()).get();
      fail("Expected ExecutionException caused by key mismatch");
    } catch (java.util.concurrent.ExecutionException e) {
      // pass
    }
  }

  @Test
  public void testAvailableServers() {
    client.getVersions();
    
    Collection<SocketAddress> availableServers = client.getAvailableServers();
    
    assertTrue(availableServers.size() == 1);
    
    SocketAddress sa = availableServers.iterator().next();
    String addrString = String.valueOf(sa);
    //Exact string match is not used as the host name can have multiple names such as "localhost", "localhost:localdomain" or even blank.
    assertTrue(addrString.contains(getExpectedVersionSource()));
    
  }

  @Test
  public void testUnavailableServers() {
    client.getVersions();
    assertEquals(Collections.emptyList(), client.getUnavailableServers());
  }

  protected abstract String getExpectedVersionSource();

  @Test
  public void testGetVersions() throws Exception {
    Map<SocketAddress, String> vs = client.getVersions();
    assertEquals(1, vs.size());
    Map.Entry<SocketAddress, String> me = vs.entrySet().iterator().next();
    assertTrue(me.getKey().toString().contains(getExpectedVersionSource()));
    assertNotNull(me.getValue());
  }

  @Test
  public void testNonexistentMutate() throws Exception {
    assertEquals(-1, client.incr("nonexistent", 1));
    assert !client.asyncIncr("nonexistent", 1).getStatus().isSuccess();
    assertEquals(-1, client.decr("nonexistent", 1));
    assert !client.asyncDecr("nonexistent", 1).getStatus().isSuccess();
  }

  @Test
  public void testMutateWithDefault() throws Exception {
    assertEquals(3, client.incr("mtest", 1, 3));
    assertEquals(4, client.incr("mtest", 1, 3));
    assertEquals(3, client.decr("mtest", 1, 9));
    assertEquals(9, client.decr("mtest2", 1, 9));
  }

  @Test
  public void testMutateWithDefaultAndExp() throws Exception {
    assertEquals(3, client.incr("mtest", 1, 3, 1));
    assertEquals(4, client.incr("mtest", 1, 3, 1));
    assertEquals(3, client.decr("mtest", 1, 9, 1));
    assertEquals(9, client.decr("mtest2", 1, 9, 1));
    Thread.sleep(2000);
    assertNull(client.get("mtest"));
    assert !client.asyncGet("mtest").getStatus().isSuccess();
  }

  @Test
  public void testAsyncIncrement() throws Exception {
    String k = "async-incr";
    client.set(k, 0, "5");
    Future<Long> f = client.asyncIncr(k, 1);
    assertEquals(6, (long) f.get());
  }

  @Test
  public void testAsyncIncrementNonExistent() throws Exception {
    String k = "async-incr-non-existent";
    Future<Long> f = client.asyncIncr(k, 1);
    assertEquals(-1, (long) f.get());
  }
  
  @Test
  public void testAsyncDecrement() throws Exception {
    String k = "async-decr";
    client.set(k, 0, "5");
    Future<Long> f = client.asyncDecr(k, 1);
    assertEquals(4, (long) f.get());
  }

  @Test
  public void testAsyncDecrementNonExistent() throws Exception {
    String k = "async-decr-non-existent";
    Future<Long> f = client.asyncDecr(k, 1);
    assertEquals(-1, (long) f.get());
  }

  @Test
  public void testConcurrentMutation() throws Throwable {
    int num = SyncThread.getDistinctResultCount(10, new Callable<Long>() {
      public Long call() throws Exception {
        return client.incr("mtest", 1, 11);
      }
    });
    assertEquals(10, num);
  }

  @Test
  public void testImmediateDelete() throws Exception {
    assertNull(client.get("test1"));
    client.set("test1", 5, "test1value");
    assertEquals("test1value", client.get("test1"));
    assert client.delete("test1").getStatus().isSuccess();
    assertNull(client.get("test1"));
  }

  @Test
  public void testFlush() throws Exception {
    String current_config = null;
    Collection<NodeEndPoint> endpoints = new ArrayList<NodeEndPoint>();
    if(TestConfig.getInstance().getClientMode().equals(ClientMode.Dynamic) &&
       !TestConfig.getInstance().getEngineType().isSetConfigSupported()) {
      current_config = getCurrentConfigAndClusterEndpoints(client, endpoints);
    }
    assertNull(client.get("test1"));
    client.set("test1", 5, "test1value");
    client.set("test2", 5, "test2value");
    assertEquals("test1value", client.get("test1"));
    assertEquals("test2value", client.get("test2"));
    assertTrue(client.flush().get());
    if(TestConfig.getInstance().getClientMode().equals(ClientMode.Dynamic) &&
       !TestConfig.getInstance().getEngineType().isSetConfigSupported()) {
      Thread.sleep(1000);
      restoreClusterConfig(current_config, endpoints);
      Thread.sleep(1000); // Wait for the config to restore
    }

    // Restore the server configuration for TLS enabled Dynamic mode after flush
    if (TestConfig.isTlsMode() && TestConfig.getInstance().getClientMode() == ClientMode.Dynamic) {
      setClusterConfigForTLS(TestConfig.PORT_NUMBER);
    }
    assertNull(client.get("test1"));
    assertNull(client.get("test2"));
  }

  @Test
  public void testTouch() throws Exception {
    assertNull(client.get("touchtest"));
    assertNull(client.get("nonexistent"));
    assertTrue(client.set("touchtest", 5, "touchtest").get());
    assertTrue(client.touch("touchtest", 2).get());
    assertFalse(client.touch("nonexistent", 2).get());
  }

  @Test
  public void testGetAndTouch() throws Exception {
    assertNull(client.get("gattest"));
    assertNull(client.get("nonexistent"));
    assertTrue(client.set("gattest", 5, "value1").get());
    assertTrue(client.set("gattest2", 10, "value2").get());
    assertEquals("value1", client.getAndTouch("gattest", 2).getValue());
    assertEquals("value2", client.getAndTouch("gattest2", 20).getValue());
    assertNull(client.getAndTouch("nonexistent", 2));
  }

  @Test
  public void testGracefulShutdown() throws Exception {
    for (int i = 0; i < 1000; i++) {
      client.set("t" + i, 10, i);
    }
    assertTrue("Couldn't shut down within five seconds",
        client.shutdown(5, TimeUnit.SECONDS));

    // Get a new client
    initClient();
    Collection<String> keys = new ArrayList<String>();
    for (int i = 0; i < 1000; i++) {
      keys.add("t" + i);
    }
    Map<String, Object> m = client.getBulk(keys);
    assertEquals(1000, m.size());
    for (int i = 0; i < 1000; i++) {
      assertEquals(i, m.get("t" + i));
    }
  }

  protected void syncGetTimeoutsInitClient() throws Exception {
    initClient(new ClientTestConnectionFactory() {
      @Override
      public long getOperationTimeout() {
        return 2;
      }

      @Override
      public int getTimeoutExceptionThreshold() {
        return 1000000;
      }
    });
  }

  @Test
  public void testSyncGetTimeouts() throws Exception {
    final String key = "timeoutTestKey";
    final String value = "timeoutTestValue";

    int j = 0;
    boolean set = false;

    // Do not execute this for CI
    if (TestConfig.isCITest()) {
      return;
    }

    do {
      set = client.set(key, 0, value).get();
      j++;
    } while (!set && j < 10);
    assert set;

    // Shutting down the default client to get one with a short timeout.
    assertTrue("Couldn't shut down within five seconds",
        client.shutdown(5, TimeUnit.SECONDS));

    syncGetTimeoutsInitClient();
    Thread.sleep(100); // allow connections to be established

    int i = 0;
    GetFuture<Object> g = null;
    try {
      for (i = 0; i < 1000000; i++) {
        g = client.asyncGet(key);
        g.get();
      }
      throw new Exception("Didn't get a timeout.");
    } catch (Exception e) {
      assert !g.getStatus().isSuccess();
      System.err.println("Got a timeout at iteration " + i + ".");
    }
    Thread.sleep(100); // let whatever caused the timeout to pass
    try {
      if (value.equals(client.asyncGet(key).get(30, TimeUnit.SECONDS))) {
        System.err.println("Got the right value.");
      } else {
        throw new Exception("Didn't get the expected value.");
      }
    } catch (java.util.concurrent.TimeoutException timeoutException) {
      debugNodeInfo(client.getNodeLocator().getAll());
      throw new Exception("Unexpected timeout after 30 seconds waiting",
          timeoutException);
    }
  }

  protected void debugNodeInfo(Collection<MemcachedNode> nodes) {
    System.err.println("Debug nodes:");
    for (MemcachedNode node : nodes) {
      System.err.println(node);
      System.err.println("Is active? " + node.isActive());
      System.err.println("Has read operation? " + node.hasReadOp()
          + " Has write operation? " + node.hasWriteOp());
      try {
        System.err.println("Has timed out this many times: "
            + node.getContinuousTimeout());
        System.err.println("Write op: " + node.getCurrentWriteOp());
        System.err.println("Read op: " + node.getCurrentReadOp());
      } catch (UnsupportedOperationException e) {
        System.err.println("Node does not support full interface, likely read "
            + "only.");
      }
    }
  }

  @Test
  public void xtestGracefulShutdownTooSlow() throws Exception {

    // Increase the number of set commands from 10,000 to 1M to make sure 
    // there are still bytes remaining before shutdown after timing out 1 millisecond.
    for (int i = 0; i < 100000; i++) {
      client.set("t" + i, 10, i);
    }
    assertFalse("Weird, shut down too fast",
        client.shutdown(1, TimeUnit.MILLISECONDS));

    try {
      Map<SocketAddress, String> m = client.getVersions();
      fail("Expected failure, got " + m);
    } catch (IllegalStateException e) {
      assertEquals("Shutting down", e.getMessage());
    }

    // Get a new client
    initClient();
  }

  @Test
  public void testStupidlyLargeSetAndSizeOverride() throws Exception {
    Random r = new Random();
    SerializingTranscoder st = new SerializingTranscoder(Integer.MAX_VALUE);

    st.setCompressionThreshold(Integer.MAX_VALUE);

    byte[] data = new byte[21 * 1024 * 1024];
    r.nextBytes(data);

    try {
      client.set("bigobject", 60, data, st).get();
      fail("Didn't fail setting big object.");
    } catch (ExecutionException e) {
      System.err.println("Successful failure setting big object.  Object size "
          + data.length + " bytes doesn't fit.");
      e.printStackTrace();
      OperationException oe = (OperationException) e.getCause();
      assertSame(OperationErrorType.SERVER, oe.getType());
    }

    // But I should still be able to do something.
    client.set("k", 5, "Blah");
    assertEquals("Blah", client.get("k"));
  }

  @Test
  public void testStupidlyLargeSet() throws Exception {
    Random r = new Random();
    SerializingTranscoder st = new SerializingTranscoder();
    st.setCompressionThreshold(Integer.MAX_VALUE);

    byte[] data = new byte[21 * 1024 * 1024];
    r.nextBytes(data);

    try {
      client.set("bigobject", 60, data, st).get();
      fail("Didn't fail setting big object.");
    } catch (IllegalArgumentException e) {
      assertEquals("Cannot cache data larger than " + CachedData.MAX_SIZE
          + " bytes " + "(you tried to cache a " + data.length
          + " byte object)", e.getMessage());
    }

    // But I should still be able to do something.
    client.set("k", 5, "Blah");
    assertEquals("Blah", client.get("k"));
  }

  @Test
  public void testQueueAfterShutdown() throws Exception {
    client.shutdown();
    try {
      Object o = client.get("k");
      fail("Expected IllegalStateException, got " + o);
    } catch (IllegalStateException e) {
      // OK
    } finally {
      initClient(); // init for tearDown
    }
  }

  @Test
  public void testMultiReqAfterShutdown() throws Exception {
    client.shutdown();
    try {
      Map<String, ?> m = client.getBulk("k1", "k2", "k3");
      fail("Expected IllegalStateException, got " + m);
    } catch (IllegalStateException e) {
      // OK
    } finally {
      initClient(); // init for tearDown
    }
  }

  @Test
  public void testBroadcastAfterShutdown() throws Exception {
    String current_config = null;
    Collection<NodeEndPoint> endpoints = new ArrayList<NodeEndPoint>();
    if(TestConfig.getInstance().getClientMode().equals(ClientMode.Dynamic) &&
       !TestConfig.getInstance().getEngineType().isSetConfigSupported()) {
      current_config = getCurrentConfigAndClusterEndpoints(client, endpoints);
    }
    client.shutdown();
    try {
      Future<?> f = client.flush();
      fail("Expected IllegalStateException, got " + f.get());
    } catch (IllegalStateException e) {
      // OK
    } finally {
      if(TestConfig.getInstance().getClientMode().equals(ClientMode.Dynamic) &&
         !TestConfig.getInstance().getEngineType().isSetConfigSupported()) {
        Thread.sleep(1000);
         restoreClusterConfig(current_config, endpoints);
         Thread.sleep(1000); // Wait for the config to restore
      }
      initClient(); // init for tearDown
    }
  }

  @Test
  public void testABunchOfCancelledOperations() throws Exception {
    final String k = "bunchOCancel";
    Collection<Future<?>> futures = new ArrayList<Future<?>>();
    for (int i = 0; i < 1000; i++) {
      futures.add(client.set(k, 5, "xval"));
      futures.add(client.asyncGet(k));
    }
    OperationFuture<Boolean> sf = client.set(k, 5, "myxval");
    GetFuture<Object> gf = client.asyncGet(k);
    for (Future<?> f : futures) {
      f.cancel(true);
    }
    assertTrue(sf.get());
    assert sf.getStatus().isSuccess();
    assertEquals("myxval", gf.get());
    assert gf.getStatus().isSuccess();
  }

  @Test
  public void testUTF8Key() throws Exception {
    final String key = "junit.Здравствуйте."
        + System.currentTimeMillis();
    final String value = "Skiing rocks if you can find the time to go!";

    assertTrue(client.set(key, 6000, value).get());
    Object output = client.get(key);
    assertNotNull("output is null", output);
    assertEquals("output is not equal", value, output);
  }

  @Test
  public void testUTF8KeyDelete() throws Exception {
    final String key =
        "junit.Здравствуйте." + System.currentTimeMillis();
    final String value = "Skiing rocks if you can find the time to go!";

    assertTrue(client.set(key, 6000, value).get());
    assertTrue(client.delete(key).get());
    assertNull(client.get(key));
  }

  @Test
  public void testUTF8MultiGet() throws Exception {
    final String value = "Skiing rocks if you can find the time to go!";
    Collection<String> keys = new ArrayList<String>();
    for (int i = 0; i < 50; i++) {
      final String key =
          "junit.Здравствуйте." + System.currentTimeMillis() + "."
              + i;
      assertTrue(client.set(key, 6000, value).get());
      keys.add(key);
    }

    Map<String, Object> vals = client.getBulk(keys);
    assertEquals(keys.size(), vals.size());
    for (Object o : vals.values()) {
      assertEquals(value, o);
    }
    assertTrue(keys.containsAll(vals.keySet()));
  }

  @Test
  public void testUTF8Value() throws Exception {
    final String key = "junit.plaintext." + System.currentTimeMillis();
    final String value = "Здравствуйте Здравствуйте �"
        + "�дравствуйте Skiing rocks if you can find the time "
        + "to go!";

    assertTrue(client.set(key, 6000, value).get());
    Object output = client.get(key);
    assertNotNull("output is null", output);
    assertEquals("output is not equal", value, output);
  }

  @Test
  public void testAppend() throws Exception {
    final String key = "append.key";
    assertTrue(client.set(key, 5, "test").get());
    OperationFuture<Boolean> op = client.append(0, key, "ing");
    assertTrue(op.get());
    assert op.getStatus().isSuccess();
    assertEquals("testing", client.get(key));
  }

  @Test
  public void testPrepend() throws Exception {
    final String key = "prepend.key";
    assertTrue(client.set(key, 5, "test").get());
    OperationFuture<Boolean> op = client.prepend(0, key, "ing");
    assertTrue(op.get());
    assert op.getStatus().isSuccess();
    assertEquals("ingtest", client.get(key));
  }

  @Test
  public void testAppendNoSuchKey() throws Exception {
    final String key = "append.missing";
    assertFalse(client.append(0, key, "ing").get());
    assertNull(client.get(key));
  }

  @Test
  public void testPrependNoSuchKey() throws Exception {
    final String key = "prepend.missing";
    assertFalse(client.prepend(0, key, "ing").get());
    assertNull(client.get(key));
  }

  @Test
  public void testAppendWithoutCAS() throws Exception {
    final String key = "append.key";
    assertTrue(client.set(key, 5, "test").get());
    OperationFuture<Boolean> op = client.append(key, "es");
    assertTrue(op.get());
    assert op.getStatus().isSuccess();
    assertEquals("testes", client.get(key));
  }

  @Test
  public void testPrependWithoutCAS() throws Exception {
    final String key = "prepend.key";
    assertTrue(client.set(key, 5, "test").get());
    OperationFuture<Boolean> op = client.prepend(key, "es");
    assertTrue(op.get());
    assert op.getStatus().isSuccess();
    assertEquals("estest", client.get(key));
  }

  @Test
  public void testSetReturnsCAS() throws Exception {

    OperationFuture<Boolean> setOp = client.set("testSetReturnsCAS",
        0, "testSetReturnsCAS");
    setOp.get();
    assertTrue(setOp.getCas() > 0);
  }

  @Test
  public void testSetWithCallback() throws Exception {
    OperationFuture<Boolean> setOp =
        client.set("setWithCallback", 0, "content");

    final CountDownLatch latch = new CountDownLatch(1);
    setOp.addListener(new OperationCompletionListener() {
      @Override
      public void onComplete(OperationFuture<?> f) throws Exception {
        latch.countDown();
      }
    });

    assertTrue(latch.await(2, TimeUnit.SECONDS));
  }

  @Test
  public void testGetWithCallback() throws Exception {
    client.set("getWithCallback", 0, "content").get();

    GetFuture<Object> getOp = client.asyncGet("getWithCallback");

    final CountDownLatch latch = new CountDownLatch(1);
    getOp.addListener(new GetCompletionListener() {
      @Override
      public void onComplete(GetFuture<?> f) throws Exception {
        latch.countDown();
      }
    });

    assertTrue(latch.await(2, TimeUnit.SECONDS));
  }

  @Test
  public void testGetBulkWithCallback() throws Exception {
    final int items = 500;
    List<String> keysList = new ArrayList<String>(items);
    for (int i = 0; i < items; i++) {
      assertTrue(client.set("getBulkWithCallback" + i, 0, "content").get());
      keysList.add("getBulkWithCallback" + i);
    }

    BulkFuture<Map<String, Object>> asyncGetBulk =
        client.asyncGetBulk(keysList);

    final CountDownLatch latch = new CountDownLatch(1);
    asyncGetBulk.addListener(new BulkGetCompletionListener() {
      @Override
      public void onComplete(BulkGetFuture<?> f) throws Exception {
        assertEquals(items, f.get().size());
        assertTrue(f.getStatus().isSuccess());
        assertTrue(f.isDone());
        assertFalse(f.isCancelled());
        assertEquals(StatusCode.SUCCESS, f.getStatus().getStatusCode());
        latch.countDown();
      }
    });

    assertTrue(latch.await(2, TimeUnit.SECONDS));
  }

  @Test
  public void testEmptyGetBulk() throws Exception {
    Map<String, Object> bulk = client.getBulk(Collections.<String>emptyList());
    assertTrue(bulk.isEmpty());
  }

  @Test
  public void testConfigCmds(){
    //This is for basic testing for the config APIs. DR mode specific tests are implemented separately.
    if(TestConfig.getInstance().getClientMode() == ClientMode.Dynamic || !TestConfig.isElastiCacheMemcachedServer()){
      return;
    }

    final String config = "1\nlocalhost|127.0.0.1|11211";
    client.getVersions();
    Collection<NodeEndPoint> endpoints = client.getAvailableNodeEndPoints();
    InetSocketAddress sa = endpoints.iterator().next().getInetSocketAddress();
    if(TestConfig.getInstance().getEngineType().isSetConfigSupported()) {
        client.deleteConfig(sa, ConfigurationType.CLUSTER);
        assertNull(client.getConfig(sa, ConfigurationType.CLUSTER));
        client.setConfig(sa, ConfigurationType.CLUSTER, config);
        String configFromServer = (String)client.getConfig(sa, ConfigurationType.CLUSTER);
        assertEquals(config, configFromServer);
        client.deleteConfig(sa, ConfigurationType.CLUSTER);
        assertNull(client.getConfig(sa, ConfigurationType.CLUSTER));
    } else {
      String configKey = ConfigurationType.CLUSTER.getValueWithNameSpace();
        client.delete(configKey);
        assertNull(client.get(configKey));
        client.set(configKey, 0, config);
        String configFromServer = (String)client.get(configKey);
        assertEquals(config, configFromServer);
        client.delete(configKey);
        assertNull(client.get(configKey));
    }
  }

  private static class TestTranscoder implements Transcoder<String> {
    private static final int FLAGS = 238885206;

    public String decode(CachedData d) {
      assert d.getFlags() == FLAGS : "expected " + FLAGS + " got "
          + d.getFlags();
      return new String(d.getData());
    }

    public CachedData encode(String o) {
      return new CachedData(FLAGS, o.getBytes(), getMaxSize());
    }

    public int getMaxSize() {
      return CachedData.MAX_SIZE;
    }

    public boolean asyncDecode(CachedData d) {
      return false;
    }
  }

  private static class TestWithKeyTranscoder implements Transcoder<String> {
    private static final int FLAGS = 238885207;

    private final String key;

    TestWithKeyTranscoder(String k) {
      key = k;
    }

    public String decode(CachedData d) {
      assert d.getFlags() == FLAGS : "expected " + FLAGS + " got "
          + d.getFlags();

      ByteBuffer bb = ByteBuffer.wrap(d.getData());

      int keyLength = bb.getInt();
      byte[] keyBytes = new byte[keyLength];
      bb.get(keyBytes);
      String k = new String(keyBytes);

      assertEquals(key, k);

      int valueLength = bb.getInt();
      byte[] valueBytes = new byte[valueLength];
      bb.get(valueBytes);

      return new String(valueBytes);
    }

    public CachedData encode(String o) {
      byte[] keyBytes = key.getBytes();
      byte[] valueBytes = o.getBytes();
      int length = 4 + keyBytes.length + 4 + valueBytes.length;
      byte[] bytes = new byte[length];

      ByteBuffer bb = ByteBuffer.wrap(bytes);
      bb.putInt(keyBytes.length).put(keyBytes);
      bb.putInt(valueBytes.length).put(valueBytes);

      return new CachedData(FLAGS, bytes, getMaxSize());
    }

    public int getMaxSize() {
      return CachedData.MAX_SIZE;
    }

    public boolean asyncDecode(CachedData d) {
      return false;
    }
  }
}
