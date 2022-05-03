package org.infinispan.hotrod.event.impl;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.infinispan.hotrod.impl.logging.Log;
import org.infinispan.hotrod.impl.logging.LogFactory;

public abstract class EventDispatcher<T> {
   static final Log log = LogFactory.getLog(EventDispatcher.class);
   private static final AtomicReferenceFieldUpdater<EventDispatcher, DispatcherStatus> statusUpdater
         = AtomicReferenceFieldUpdater.newUpdater(EventDispatcher.class, DispatcherStatus.class, "status");

   final Object listener;
   final byte[] listenerId;
   final SocketAddress address;
   final String cacheName;
   final Runnable cleanup;
   private volatile DispatcherStatus status = DispatcherStatus.STOPPED;

   EventDispatcher(String cacheName, Object listener, byte[] listenerId, SocketAddress address, Runnable cleanup) {
      this.listener = listener;
      this.listenerId = listenerId;
      this.address = address;
      this.cacheName = cacheName;
      this.cleanup = cleanup;
   }

   public boolean start() {
      return statusUpdater.compareAndSet(this, EventDispatcher.DispatcherStatus.STOPPED, EventDispatcher.DispatcherStatus.RUNNING);
   }

   public boolean stop() {
      boolean stopped = statusUpdater.compareAndSet(this, DispatcherStatus.RUNNING, DispatcherStatus.STOPPED);
      if (stopped && cleanup != null) {
         cleanup.run();
      }
      return stopped;
   }

   public boolean isRunning() {
      return status == DispatcherStatus.RUNNING;
   }

   public abstract CompletableFuture<Void> executeFailover();

   protected abstract void invokeEvent(T event);

   protected void invokeFailoverEvent() {}

   public SocketAddress address() {
      return address;
   }

   enum DispatcherStatus {
      STOPPED,
      RUNNING
   }
}
