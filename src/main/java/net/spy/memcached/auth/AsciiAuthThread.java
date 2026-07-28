/**
 * Copyright (C) 2006-2009 Dustin Sallings
 * Copyright (C) 2009-2014 Couchbase, Inc.
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
 */

package net.spy.memcached.auth;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;

import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.OperationFactory;
import net.spy.memcached.compat.SpyThread;
import net.spy.memcached.compat.log.Level;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StoreOperation;
import net.spy.memcached.ops.StoreType;

/**
 * A thread that does text authentication.
 */
public class AsciiAuthThread extends SpyThread {

  /**
   * If the total AUTH steps take longer than this period in milliseconds, a
   * warning will be issued instead of a debug message.
   */
  public static final int AUTH_TOTAL_THRESHOLD = 250;

  private final MemcachedConnection conn;
  private final AuthDescriptor authDescriptor;
  private final OperationFactory opFact;
  private final MemcachedNode node;

  public AsciiAuthThread(MemcachedConnection c, OperationFactory o,
                         AuthDescriptor a, MemcachedNode n) {
    conn = c;
    opFact = o;
    authDescriptor = a;
    node = n;
    start();
  }

  @Override
  public void run() {
    final AtomicBoolean done = new AtomicBoolean();
    long start = System.nanoTime();
      final CountDownLatch latch = new CountDownLatch(1);
      NameCallback nameCallback = new NameCallback("memcached");
      PasswordCallback passwordCallback = new PasswordCallback("memcached", false);
      try {
        authDescriptor.getCallback().handle(new Callback[]{nameCallback, passwordCallback});
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      String credentials = nameCallback.getName() + ' ' + new String(passwordCallback.getPassword());
      Operation op = opFact.store(StoreType.set, "ignore", 0, 0, credentials.getBytes(StandardCharsets.US_ASCII),
            new StoreOperation.Callback() {
              @Override
              public void receivedStatus(OperationStatus val) {
                if (val.isSuccess()) {
                  done.set(true);
                  node.authComplete();
                }
              }

              @Override
              public void gotData(String key, long cas) {
              }

              @Override
              public void complete() {
                latch.countDown();
              }
            });
      conn.insertOperation(node, op);

      try {
        if (!conn.isShutDown()) {
          latch.await();
        } else {
          done.set(true); // Connection is shutting down, tear.down.
        }
        Thread.sleep(100);
      } catch (InterruptedException e) {
        // we can be interrupted if we were in the
        // process of auth'ing and the connection is
        // lost or dropped due to bad auth
        Thread.currentThread().interrupt();
        if (op != null) {
          op.cancel();
        }
        done.set(true); // If we were interrupted, tear down.
      } finally {
        long diff = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        Level level = diff >= AUTH_TOTAL_THRESHOLD ? Level.WARN : Level.DEBUG;
        getLogger().log(level, String.format("ASCII authentication step took %dms on %s",
              diff, node.toString()));
      }
    }
}
