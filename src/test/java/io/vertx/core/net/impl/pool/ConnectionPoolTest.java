/*
 * Copyright (c) 2011-2021 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */
package io.vertx.core.net.impl.pool;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.impl.EventLoopContext;
import io.vertx.core.impl.VertxInternal;
import io.vertx.test.core.VertxTestBase;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConnectionPoolTest extends VertxTestBase {

  VertxInternal vertx;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    this.vertx = (VertxInternal) super.vertx;
  }

  @Test
  public void testConnect() {
    EventLoopContext context = vertx.createEventLoopContext();
    ConnectionManager mgr = new ConnectionManager();
    ConnectionPool<Connection> pool = new ConnectionPool<>(mgr, 10);
    Connection expected = new Connection();
    pool.acquire(context, onSuccess(lease -> {
      assertSame(expected, lease.get());
      assertSame(context, Vertx.currentContext());
      testComplete();
    }));
    ConnectionRequest request = mgr.assertRequest();
    assertSame(context, request.context);
    request.connect(expected);
    await();
  }

  @Test
  public void testRecycle() throws Exception {
    EventLoopContext context = vertx.createEventLoopContext();
    ConnectionManager mgr = new ConnectionManager();
    ConnectionPool<Connection> pool = new ConnectionPool<>(mgr, 10);
    Connection expected = new Connection();
    CountDownLatch latch = new CountDownLatch(1);
    pool.acquire(context, onSuccess(lease -> {
      lease.recycle();
      latch.countDown();
    }));
    ConnectionRequest request = mgr.assertRequest();
    assertSame(context, request.context);
    request.connect(expected);
    awaitLatch(latch);
    pool.acquire(context, onSuccess(lease -> {
      assertSame(expected, lease.get());
      assertSame(context, Vertx.currentContext());
      testComplete();
    }));
    await();
  }

  @Test
  public void testCapacity() throws Exception {
    EventLoopContext context = vertx.createEventLoopContext();
    ConnectionManager mgr = new ConnectionManager();
    ConnectionPool<Connection> pool = new ConnectionPool<>(mgr, 10);
    int capacity = 2;
    Connection expected = new Connection(capacity);
    CountDownLatch latch = new CountDownLatch(1);
    pool.acquire(context, onSuccess(conn -> {
      latch.countDown();
    }));
    ConnectionRequest request = mgr.assertRequest();
    request.connect(expected);
    awaitLatch(latch);
    pool.acquire(context, onSuccess(lease -> {
      assertSame(lease.get(), expected);
      testComplete();
    }));
    await();
  }

  @Test
  public void testWaiter() throws Exception {
    EventLoopContext ctx1 = vertx.createEventLoopContext();
    ConnectionManager mgr = new ConnectionManager();
    ConnectionPool<Connection> pool = new ConnectionPool<>(mgr, 1);
    Connection expected = new Connection();
    CompletableFuture<Lease<Connection>> latch = new CompletableFuture<>();
    pool.acquire(ctx1, onSuccess(latch::complete));
    ConnectionRequest request = mgr.assertRequest();
    request.connect(expected);
    Lease<Connection> lease1 = latch.get(10, TimeUnit.SECONDS);
    AtomicBoolean recycled = new AtomicBoolean();
    EventLoopContext ctx2 = vertx.createEventLoopContext();
    pool.acquire(ctx2, onSuccess(lease2 -> {
      assertSame(ctx1, Vertx.currentContext());
      assertTrue(recycled.get());
      testComplete();
    }));
    assertEquals(1, pool.waiters());
    recycled.set(true);
    lease1.recycle();
    await();
  }

  @Test
  public void testEvict() throws Exception {
    EventLoopContext ctx1 = vertx.createEventLoopContext();
    ConnectionManager mgr = new ConnectionManager();
    ConnectionPool<Connection> pool = new ConnectionPool<>(mgr, 1);
    Connection expected = new Connection();
    CompletableFuture<Lease<Connection>> latch = new CompletableFuture<>();
    pool.acquire(ctx1, onSuccess(latch::complete));
    ConnectionRequest request1 = mgr.assertRequest();
    request1.connect(expected);
    Lease<Connection> lease1 = latch.get(10, TimeUnit.SECONDS);
    AtomicBoolean evicted = new AtomicBoolean();
    Connection connection2 = new Connection();
    EventLoopContext ctx2 = vertx.createEventLoopContext();
    pool.acquire(ctx2, onSuccess(lease2 -> {
      assertSame(ctx2, Vertx.currentContext());
      assertTrue(evicted.get());
      assertSame(connection2, lease2.get());
      testComplete();
    }));
    assertEquals(1, pool.waiters());
    evicted.set(true);
    request1.listener.evict();
    ConnectionRequest request2 = mgr.assertRequest();
    request2.connect(connection2);
    await();
  }

  static class Connection {
    final int capacity;
    public Connection() {
      this(1);
    }
    public Connection(int capacity) {
      this.capacity = capacity;
    }
  }

  static class ConnectionRequest {
    final EventLoopContext context;
    final ConnectionEventListener listener;
    final Handler<AsyncResult<Connection>> handler;
    ConnectionRequest(EventLoopContext context, ConnectionEventListener listener, Handler<AsyncResult<Connection>> handler) {
      this.context = context;
      this.listener = listener;
      this.handler = handler;
    }
    void connect(Connection connection) {
      handler.handle(Future.succeededFuture(connection));
    }
  }

  class ConnectionManager implements Connector<Connection> {

    private final Queue<ConnectionRequest> requests = new ArrayBlockingQueue<>(100);

    @Override
    public void connect(EventLoopContext context, ConnectionEventListener listener, Handler<AsyncResult<Connection>> handler) {
      requests.add(new ConnectionRequest(context, listener, handler));
    }

    @Override
    public boolean isValid(Connection connection) {
      return true;
    }

    @Override
    public int capacity(Connection connection) {
      return connection.capacity;
    }

    ConnectionRequest assertRequest() {
      ConnectionRequest request = requests.poll();
      assertNotNull(request);
      return request;
    }
  }
}
