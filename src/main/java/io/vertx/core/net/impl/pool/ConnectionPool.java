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
import io.vertx.core.impl.EventLoopContext;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectionPool<C> {

  static class Slot<C> implements ConnectionEventListener {

    private final ConnectionPool<C> pool;
    private final EventLoopContext context;
    private C connection;
    private int index;
    private int capacity;

    public Slot(ConnectionPool<C> pool, EventLoopContext context, int index) {
      this.pool = pool;
      this.context = context;
      this.connection = null;
      this.capacity = 0;
      this.index = index;
    }

    @Override
    public void evict() {
      pool.lock.lock();
      try {
        capacity = 0;
        connection = null;
        Waiter<C> waiter = pool.waiters.poll();
        if (waiter != null) {
          Slot<C> slot = new Slot<>(pool, waiter.context, index);
          pool.slots[index] = slot;
          slot.connect(waiter.handler);
        } else if (pool.size > 1) {
          Slot<C> tmp = pool.slots[pool.size - 1];
          tmp.index = index;
          pool.slots[index] = tmp;
          pool.slots[pool.size - 1] = null;
          pool.size--;
        }
      } finally {
        pool.lock.unlock();
      }
    }

    public void connect(Handler<AsyncResult<Lease<C>>> handler) {
      pool.connector.connect(context, this, ar -> {
        if (ar.succeeded()) {
          pool.lock.lock();
          connection = ar.result();
          capacity = pool.connector.capacity(connection) - 1;
          if (capacity > 0) {
            if (pool.waiters.size() > 0) {
              throw new UnsupportedOperationException("Implement me");
            }
          }
          pool.lock.unlock();
          context.emit(Future.succeededFuture(new LeaseImpl<>(this)), handler);
        } else {
          // Retry ???
          System.out.println("Implement me");
        }
      });
    }
  }

  static class Waiter<C> {

    final EventLoopContext context;
    final Handler<AsyncResult<Lease<C>>> handler;

    Waiter(EventLoopContext context, Handler<AsyncResult<Lease<C>>> handler) {
      this.context = context;
      this.handler = handler;
    }
  }

  private final Connector<C> connector;

  private final Slot<C>[] slots;
  private int size;
  private final Deque<Waiter<C>> waiters = new ArrayDeque<>();

  private final Lock lock = new ReentrantLock();

  public ConnectionPool(Connector<C> connector, int maxSize) {
    this.connector = connector;
    this.slots = new Slot[maxSize];
    this.size = 0;
  }

  /**
   * Borrow a connection from the pool.
   *
   *
   * @param context the context
   * @param handler the callback
   */
  public void acquire(EventLoopContext context, Handler<AsyncResult<Lease<C>>> handler) {

    lock.lock();

    // 1. Try reuse a existing connection with the same context
    for (int i = 0;i < size;i++) {
      Slot<C> slot = slots[i];
      if (slot != null && slot.context == context && slot.capacity > 0) {
        slot.capacity--;
        lock.unlock();
        context.emit(Future.succeededFuture(new LeaseImpl<>(slot)), handler);
        return;
      }
    }

    // 2. Try create connection
    if (size < slots.length) {
      Slot<C> slot = new Slot<>(this, context, size);
      slots[size++] = slot;
      lock.unlock();
      slot.connect(handler);
      return;
    }

    // 3. Try use another context
    for (Slot<C> slot : slots) {
      if (slot.capacity > 0) {
        slot.capacity--;
        lock.unlock();
        context.emit(Future.succeededFuture(new LeaseImpl<>(slot)), handler);
        return;
      }
    }

    // 4. Fall in waiters list
    waiters.add(new Waiter<>(context, handler));
    lock.unlock();
  }

  static class LeaseImpl<C> implements Lease<C> {

    private final Slot<C> slot;

    public LeaseImpl(Slot<C> slot) {
      this.slot = slot;
    }

    @Override
    public C get() {
      return slot.connection;
    }

    @Override
    public void recycle() {
      slot.pool.recycle(slot);
    }
  }

  private void recycle(Slot<C> slot) {

    lock.lock();

    if (waiters.size() > 0) {
      Waiter<C> waiter = waiters.poll();
      lock.unlock();
      slot.context.emit(Future.succeededFuture(new LeaseImpl<>(slot)), waiter.handler);
      return;
    }

    slot.capacity++;
    lock.unlock();
  }

  public int waiters() {
    lock.lock();
    try {
      return waiters.size();
    } finally {
      lock.unlock();
    }
  }
}
