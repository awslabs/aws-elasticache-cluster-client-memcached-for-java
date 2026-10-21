/**
 * Copyright (C) 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package net.spy.memcached;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import junit.framework.TestCase;

/**
 * Tests the TLS read path against responses whose TLS records arrive in
 * pieces.
 *
 * Node A is reached through a proxy that can split, delay or truncate the
 * server's TLS records. Node B is reached directly. Both nodes share the
 * client's single IO thread, so a stall while reading from A shows up as a
 * timeout on B.
 */
public class TLSPartialRecordTest extends TestCase {

  private static final String STORE_PASS = "changeit";
  private static final int TLS_APPLICATION_DATA = 23;
  private static final int TAIL_BYTES = 10;

  private File keyStoreFile;
  private TlsMemcachedStub server;
  private RecordProxy proxy;
  private MemcachedClient client;
  private String keyA;
  private String keyB;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    keyStoreFile = createKeyStore();
  }

  @Override
  protected void tearDown() throws Exception {
    stopAll();
    if (keyStoreFile != null) {
      keyStoreFile.delete();
    }
    super.tearDown();
  }

  public void testPartialRecordDoesNotStallOtherNodesTls12() throws Exception {
    checkPartialRecordDoesNotStallOtherNodes("TLSv1.2");
  }

  public void testPartialRecordDoesNotStallOtherNodesTls13() throws Exception {
    if (!isSupported("TLSv1.3")) {
      return;
    }
    checkPartialRecordDoesNotStallOtherNodes("TLSv1.3");
  }

  public void testPeerCloseMidRecordReconnects() throws Exception {
    start("TLSv1.2");
    proxy.armTailFin();
    Future<Object> pendingA = client.asyncGet(keyA);
    assertTrue("proxy never sent a partial record",
        proxy.awaitPartialSent(5, TimeUnit.SECONDS));

    assertEquals("vb", client.get(keyB));

    try {
      pendingA.get(5, TimeUnit.SECONDS);
    } catch (Exception expected) {
      // The operation on the closed connection is cancelled or times out.
    }

    long deadline = System.currentTimeMillis() + 15000;
    Object value = null;
    while (value == null && System.currentTimeMillis() < deadline) {
      try {
        value = client.get(keyA);
      } catch (RuntimeException notYetReconnected) {
        Thread.sleep(200);
      }
    }
    assertEquals("client did not recover node A after the peer closed",
        "va", value);
    assertEquals("vb", client.get(keyB));
  }

  public void testLargeAndBatchedResponsesWithSplitRecords() throws Exception {
    start("TLSv1.2");
    byte[] large = new byte[200 * 1024];
    new Random(42).nextBytes(large);
    assertTrue(client.set(keyA, 0, large).get(5, TimeUnit.SECONDS));

    List<String> keysOnA = keysOnNode(proxy.getPort(), 4);
    keysOnA.remove(keyA);
    for (String k : keysOnA) {
      assertTrue(client.set(k, 0, "batch-" + k).get(5, TimeUnit.SECONDS));
    }

    proxy.setSplitAll(true);

    assertTrue("large value corrupted",
        Arrays.equals(large, (byte[]) client.get(keyA)));

    Map<String, Object> bulk = client.getBulk(keysOnA);
    assertEquals(keysOnA.size(), bulk.size());
    for (String k : keysOnA) {
      assertEquals("batch-" + k, bulk.get(k));
    }
    assertEquals("vb", client.get(keyB));
  }

  private void checkPartialRecordDoesNotStallOtherNodes(String protocol)
    throws Exception {
    start(protocol);
    proxy.armHoldTail();
    Future<Object> pendingA = client.asyncGet(keyA);
    assertTrue("proxy never sent a partial record",
        proxy.awaitPartialSent(5, TimeUnit.SECONDS));

    // Node B is healthy and must answer while A's record is incomplete.
    assertEquals("vb", client.get(keyB));

    proxy.releaseTail();
    assertEquals("va", pendingA.get(5, TimeUnit.SECONDS));
    assertEquals("vb", client.get(keyB));
  }

  private void start(String protocol) throws Exception {
    SSLContext serverContext = SSLContext.getInstance("TLS");
    KeyManagerFactory kmf =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(loadKeyStore(), STORE_PASS.toCharArray());
    serverContext.init(kmf.getKeyManagers(), null, null);

    server = new TlsMemcachedStub(serverContext, protocol);
    proxy = new RecordProxy(server.getPort());

    SSLContext clientContext = SSLContext.getInstance("TLS");
    clientContext.init(null, new TrustManager[] {new TrustAll()}, null);

    ConnectionFactory cf = new ConnectionFactoryBuilder()
        .setClientMode(ClientMode.Static)
        .setSSLContext(clientContext)
        .setSkipTlsHostnameVerification(true)
        .setOpTimeout(1000)
        .build();
    List<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();
    addrs.add(new InetSocketAddress("127.0.0.1", proxy.getPort()));
    addrs.add(new InetSocketAddress("127.0.0.1", server.getPort()));
    client = new MemcachedClient(cf, addrs);

    keyA = keysOnNode(proxy.getPort(), 1).get(0);
    keyB = keysOnNode(server.getPort(), 1).get(0);
    assertTrue(client.set(keyA, 0, "va").get(5, TimeUnit.SECONDS));
    assertTrue(client.set(keyB, 0, "vb").get(5, TimeUnit.SECONDS));
    // Warm up A so that any post-handshake records (TLSv1.3 session
    // tickets) have been delivered before a test arms the proxy.
    assertEquals("va", client.get(keyA));
  }

  private List<String> keysOnNode(int port, int count) {
    List<String> keys = new ArrayList<String>();
    for (int i = 0; keys.size() < count; i++) {
      String key = "key" + i;
      InetSocketAddress sa = (InetSocketAddress)
          client.getNodeLocator().getPrimary(key).getSocketAddress();
      if (sa.getPort() == port) {
        keys.add(key);
      }
    }
    return keys;
  }

  private void stopAll() {
    if (client != null) {
      client.shutdown(5, TimeUnit.SECONDS);
      client = null;
    }
    if (proxy != null) {
      proxy.close();
      proxy = null;
    }
    if (server != null) {
      server.close();
      server = null;
    }
  }

  private static boolean isSupported(String protocol) throws Exception {
    String[] supported = SSLContext.getDefault().getSupportedSSLParameters()
        .getProtocols();
    return Arrays.asList(supported).contains(protocol);
  }

  private KeyStore loadKeyStore() throws Exception {
    KeyStore ks = KeyStore.getInstance("JKS");
    FileInputStream in = new FileInputStream(keyStoreFile);
    try {
      ks.load(in, STORE_PASS.toCharArray());
    } finally {
      in.close();
    }
    return ks;
  }

  private static File createKeyStore() throws Exception {
    File ks = File.createTempFile("tls-partial-record", ".jks");
    ks.delete();
    File keytool = new File(System.getProperty("java.home"), "bin/keytool");
    if (!keytool.exists()) {
      keytool = new File(System.getProperty("java.home"), "../bin/keytool");
    }
    Process p = new ProcessBuilder(keytool.getPath(), "-genkeypair",
        "-alias", "test", "-keyalg", "RSA", "-keysize", "2048",
        "-dname", "CN=localhost", "-validity", "2", "-storetype", "JKS",
        "-keystore", ks.getPath(), "-storepass", STORE_PASS,
        "-keypass", STORE_PASS).redirectErrorStream(true).start();
    drain(p.getInputStream());
    assertEquals("keytool failed", 0, p.waitFor());
    return ks;
  }

  private static void drain(InputStream in) throws IOException {
    byte[] buf = new byte[1024];
    while (in.read(buf) >= 0) {
      // discard
    }
  }

  private static void closeQuietly(Socket s) {
    try {
      s.close();
    } catch (IOException e) {
      // ignore
    }
  }

  private static void closeQuietly(ServerSocket s) {
    try {
      s.close();
    } catch (IOException e) {
      // ignore
    }
  }

  private static class TrustAll implements X509TrustManager {
    public void checkClientTrusted(X509Certificate[] chain, String authType) {
    }

    public void checkServerTrusted(X509Certificate[] chain, String authType) {
    }

    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
    }
  }

  /**
   * Minimal ASCII-protocol memcached over TLS: get/gets, set, version.
   * Each response is written with a single write() so that it is sent as
   * one TLS record when it fits.
   */
  private static class TlsMemcachedStub implements Runnable {
    private final SSLServerSocket serverSocket;
    private final Map<String, byte[]> data =
        new ConcurrentHashMap<String, byte[]>();
    private final Map<String, String> flags =
        new ConcurrentHashMap<String, String>();
    private final List<Socket> sockets = new ArrayList<Socket>();

    TlsMemcachedStub(SSLContext ctx, String protocol) throws IOException {
      serverSocket = (SSLServerSocket) ctx.getServerSocketFactory()
          .createServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
      serverSocket.setEnabledProtocols(new String[] {protocol});
      Thread t = new Thread(this, "tls-memcached-stub");
      t.setDaemon(true);
      t.start();
    }

    int getPort() {
      return serverSocket.getLocalPort();
    }

    public void run() {
      while (!serverSocket.isClosed()) {
        try {
          final Socket s = serverSocket.accept();
          s.setTcpNoDelay(true);
          synchronized (sockets) {
            sockets.add(s);
          }
          Thread t = new Thread(new Runnable() {
            public void run() {
              serve(s);
            }
          }, "tls-memcached-stub-conn");
          t.setDaemon(true);
          t.start();
        } catch (IOException e) {
          return;
        }
      }
    }

    private void serve(Socket s) {
      try {
        DataInputStream in =
            new DataInputStream(new BufferedInputStream(s.getInputStream()));
        OutputStream out = s.getOutputStream();
        String line;
        while ((line = readLine(in)) != null) {
          String[] parts = line.split(" ");
          ByteArrayOutputStream resp = new ByteArrayOutputStream();
          if (parts[0].equals("get") || parts[0].equals("gets")) {
            for (int i = 1; i < parts.length; i++) {
              byte[] v = data.get(parts[i]);
              if (v != null) {
                resp.write(("VALUE " + parts[i] + " " + flags.get(parts[i])
                    + " " + v.length + "\r\n").getBytes("US-ASCII"));
                resp.write(v);
                resp.write("\r\n".getBytes("US-ASCII"));
              }
            }
            resp.write("END\r\n".getBytes("US-ASCII"));
          } else if (parts[0].equals("set")) {
            byte[] v = new byte[Integer.parseInt(parts[4])];
            in.readFully(v);
            readLine(in);
            data.put(parts[1], v);
            flags.put(parts[1], parts[2]);
            resp.write("STORED\r\n".getBytes("US-ASCII"));
          } else if (parts[0].equals("version")) {
            resp.write("VERSION 1.6.0\r\n".getBytes("US-ASCII"));
          } else {
            resp.write("ERROR\r\n".getBytes("US-ASCII"));
          }
          out.write(resp.toByteArray());
          out.flush();
        }
      } catch (IOException e) {
        // connection closed
      } finally {
        closeQuietly(s);
      }
    }

    private static String readLine(InputStream in) throws IOException {
      StringBuilder sb = new StringBuilder();
      int c;
      while ((c = in.read()) >= 0) {
        if (c == '\n') {
          int len = sb.length();
          if (len > 0 && sb.charAt(len - 1) == '\r') {
            sb.setLength(len - 1);
          }
          return sb.toString();
        }
        sb.append((char) c);
      }
      return null;
    }

    void close() {
      closeQuietly(serverSocket);
      synchronized (sockets) {
        for (Socket s : sockets) {
          closeQuietly(s);
        }
      }
    }
  }

  /**
   * TCP proxy that parses server-to-client TLS records and can deliver them
   * in pieces. Client-to-server bytes are forwarded unchanged.
   */
  private static class RecordProxy implements Runnable {
    private enum Mode { NORMAL, HOLD_TAIL, TAIL_FIN }

    private final ServerSocket listener;
    private final int targetPort;
    private final List<Socket> sockets = new ArrayList<Socket>();
    private volatile Mode mode = Mode.NORMAL;
    private volatile boolean splitAll;
    private volatile CountDownLatch partialSent = new CountDownLatch(1);
    private volatile CountDownLatch tailReleased = new CountDownLatch(1);

    RecordProxy(int targetPort) throws IOException {
      this.targetPort = targetPort;
      listener = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
      Thread t = new Thread(this, "tls-record-proxy");
      t.setDaemon(true);
      t.start();
    }

    int getPort() {
      return listener.getLocalPort();
    }

    void armHoldTail() {
      partialSent = new CountDownLatch(1);
      tailReleased = new CountDownLatch(1);
      mode = Mode.HOLD_TAIL;
    }

    void armTailFin() {
      partialSent = new CountDownLatch(1);
      mode = Mode.TAIL_FIN;
    }

    void releaseTail() {
      tailReleased.countDown();
    }

    void setSplitAll(boolean split) {
      splitAll = split;
    }

    boolean awaitPartialSent(long timeout, TimeUnit unit)
      throws InterruptedException {
      return partialSent.await(timeout, unit);
    }

    public void run() {
      while (!listener.isClosed()) {
        try {
          final Socket downstream = listener.accept();
          final Socket upstream = new Socket("127.0.0.1", targetPort);
          downstream.setTcpNoDelay(true);
          upstream.setTcpNoDelay(true);
          synchronized (sockets) {
            sockets.add(downstream);
            sockets.add(upstream);
          }
          startThread(new Runnable() {
            public void run() {
              pump(downstream, upstream);
            }
          });
          startThread(new Runnable() {
            public void run() {
              pumpRecords(upstream, downstream);
            }
          });
        } catch (IOException e) {
          return;
        }
      }
    }

    private static void startThread(Runnable r) {
      Thread t = new Thread(r, "tls-record-proxy-pump");
      t.setDaemon(true);
      t.start();
    }

    private void pump(Socket from, Socket to) {
      byte[] buf = new byte[8192];
      try {
        InputStream in = from.getInputStream();
        OutputStream out = to.getOutputStream();
        int n;
        while ((n = in.read(buf)) >= 0) {
          out.write(buf, 0, n);
          out.flush();
        }
      } catch (IOException e) {
        // closed
      } finally {
        closeQuietly(from);
        closeQuietly(to);
      }
    }

    private void pumpRecords(Socket from, Socket to) {
      try {
        DataInputStream in = new DataInputStream(from.getInputStream());
        OutputStream out = to.getOutputStream();
        byte[] header = new byte[5];
        while (true) {
          in.readFully(header);
          int length = ((header[3] & 0xff) << 8) | (header[4] & 0xff);
          byte[] record = new byte[header.length + length];
          System.arraycopy(header, 0, record, 0, header.length);
          in.readFully(record, header.length, length);

          Mode m = mode;
          boolean appData = (header[0] & 0xff) == TLS_APPLICATION_DATA;
          if (appData && m != Mode.NORMAL) {
            mode = Mode.NORMAL;
            int head = record.length - TAIL_BYTES;
            out.write(record, 0, head);
            out.flush();
            partialSent.countDown();
            if (m == Mode.TAIL_FIN) {
              closeQuietly(to);
              closeQuietly(from);
              return;
            }
            tailReleased.await(10, TimeUnit.SECONDS);
            out.write(record, head, TAIL_BYTES);
            out.flush();
          } else if (appData && splitAll) {
            int mid = record.length / 2;
            out.write(record, 0, mid);
            out.flush();
            Thread.sleep(5);
            out.write(record, mid, record.length - mid);
            out.flush();
          } else {
            out.write(record);
            out.flush();
          }
        }
      } catch (IOException e) {
        // closed
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        closeQuietly(from);
        closeQuietly(to);
      }
    }

    void close() {
      tailReleased.countDown();
      closeQuietly(listener);
      synchronized (sockets) {
        for (Socket s : sockets) {
          closeQuietly(s);
        }
      }
    }
  }
}
