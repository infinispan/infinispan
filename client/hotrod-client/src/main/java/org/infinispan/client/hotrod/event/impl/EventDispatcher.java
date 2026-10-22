package org.infinispan.client.hotrod.event.impl;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

public abstract class EventDispatcher<T> {
   static final Log log = LogFactory.getLog(EventDispatcher.class);
   private static final AtomicReferenceFieldUpdater<EventDispatcher, DispatcherStatus> statusUpdater
         = AtomicReferenceFieldUpdater.newUpdater(EventDispatcher.class, DispatcherStatus.class, "status");

   final Object listener;
   final byte[] listenerId;
   final Set<SocketAddress> addresses = ConcurrentHashMap.newKeySet();
   final String cacheName;
   final Runnable cleanup;
   private volatile DispatcherStatus status = DispatcherStatus.STOPPED;

   EventDispatcher(String cacheName, Object listener, byte[] listenerId, SocketAddress address, Runnable cleanup) {
      this.listener = listener;
      this.listenerId = listenerId;
      if (address != null) {
         this.addresses.add(address);
      }
      this.cacheName = cacheName;
      this.cleanup = cleanup;
   }

   EventDispatcher(String cacheName, Object listener, byte[] listenerId, Runnable cleanup) {
      this(cacheName, listener, listenerId, null, cleanup);
   }

   public boolean start() {
      return statusUpdater.compareAndSet(this, EventDispatcher.DispatcherStatus.STOPPED, EventDispatcher.DispatcherStatus.RUNNING);
   }

   public boolean stop() {
      boolean stopped = statusUpdater.compareAndSet(this, DispatcherStatus.RUNNING, DispatcherStatus.STOPPED);
      if (stopped) {
         addresses.clear();
         if (cleanup != null) {
            cleanup.run();
         }
      }
      return stopped;
   }

   public boolean isRunning() {
      return status == DispatcherStatus.RUNNING;
   }

   public void addAddress(SocketAddress address) {
      if (address != null) {
         addresses.add(address);
      }
   }

   public boolean removeAddress(SocketAddress address) {
      return addresses.remove(address);
   }

   public void replaceAddress(SocketAddress oldAddress, SocketAddress newAddress) {
      if (oldAddress != null) {
         removeAddress(oldAddress);
      }
      if (newAddress != null) {
         addAddress(newAddress);
      }
   }

   public Set<SocketAddress> addresses() {
      return Collections.unmodifiableSet(addresses);
   }

   public SocketAddress address() {
      Iterator<SocketAddress> it = addresses.iterator();
      return it.hasNext() ? it.next() : null;
   }

   public CompletableFuture<Void> executeFailover() {
      return executeFailover(address());
   }

   public abstract CompletableFuture<Void> executeFailover(SocketAddress failedAddress);

   protected abstract void invokeEvent(T event);

   protected void invokeFailoverEvent() {
      invokeFailoverEvent(address());
   }

   protected void invokeFailoverEvent(SocketAddress failedAddress) {}

   enum DispatcherStatus {
      STOPPED,
      RUNNING
   }
}
