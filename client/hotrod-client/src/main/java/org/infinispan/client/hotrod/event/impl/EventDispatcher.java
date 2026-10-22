package org.infinispan.client.hotrod.event.impl;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.channel.Channel;

public abstract class EventDispatcher<T> {
   static final Log log = LogFactory.getLog(EventDispatcher.class);
   private static final AtomicReferenceFieldUpdater<EventDispatcher, DispatcherStatus> statusUpdater
         = AtomicReferenceFieldUpdater.newUpdater(EventDispatcher.class, DispatcherStatus.class, "status");

   final Object listener;
   final byte[] listenerId;
   final String cacheName;
   final Runnable cleanup;
   private volatile DispatcherStatus status = DispatcherStatus.STOPPED;

   EventDispatcher(String cacheName, Object listener, byte[] listenerId, Runnable cleanup) {
      this.listener = listener;
      this.listenerId = listenerId;
      this.cacheName = cacheName;
      this.cleanup = cleanup;
   }

   public boolean start() {
      DispatcherStatus current = status;
      return (current == DispatcherStatus.STOPPED || current == DispatcherStatus.STOPPED_BY_FAILOVER)
            && statusUpdater.compareAndSet(this, current, DispatcherStatus.RUNNING);
   }

   public boolean stop() {
      return stop(DispatcherStatus.STOPPED);
   }

   boolean stopForFailover() {
      return stop(DispatcherStatus.STOPPED_BY_FAILOVER);
   }

   private boolean stop(DispatcherStatus stoppedStatus) {
      DispatcherStatus current;
      DispatcherStatus next;
      do {
         current = status;
         next = switch (current) {
            case RUNNING -> stoppedStatus;
            case REMOVING -> DispatcherStatus.TERMINATED;
            case STOPPED_BY_FAILOVER -> stoppedStatus;
            default -> current;
         };
         if (current == next) {
            return false;
         }
      } while (!statusUpdater.compareAndSet(this, current, next));
      boolean stopped = current == DispatcherStatus.RUNNING || current == DispatcherStatus.REMOVING;
      if (stopped) {
         clearAddresses();
         if (cleanup != null) {
            cleanup.run();
         }
      }
      return stopped;
   }

   void terminate() {
      statusUpdater.updateAndGet(this, current -> switch (current) {
         case RUNNING, REMOVING -> DispatcherStatus.REMOVING;
         default -> DispatcherStatus.TERMINATED;
      });
   }

   boolean isTerminated() {
      DispatcherStatus current = status;
      return current == DispatcherStatus.REMOVING || current == DispatcherStatus.TERMINATED;
   }

   public boolean isRunning() {
      DispatcherStatus current = status;
      return current == DispatcherStatus.RUNNING || current == DispatcherStatus.REMOVING;
   }

   boolean isStoppedByFailover() {
      return status == DispatcherStatus.STOPPED_BY_FAILOVER;
   }

   boolean canReconnect() {
      DispatcherStatus current = status;
      return current == DispatcherStatus.RUNNING || current == DispatcherStatus.STOPPED_BY_FAILOVER;
   }

   // Registrations accept addresses before start(), while their add-listener operation is still in flight.
   public abstract void addAddress(SocketAddress address);

   public abstract boolean removeAddress(SocketAddress address);

   public void replaceAddress(SocketAddress oldAddress, SocketAddress newAddress) {
      if (oldAddress != null) {
         removeAddress(oldAddress);
      }
      if (newAddress != null) {
         addAddress(newAddress);
      }
   }

   public abstract Set<SocketAddress> addresses();

   protected abstract void clearAddresses();

   void listenerChannelClosed(SocketAddress address, Channel channel) {
      removeAddress(address);
   }

   protected abstract void invokeEvent(T event);

   enum DispatcherStatus {
      STOPPED,
      RUNNING,
      // Stopped after losing the last address; pending reconnects must keep retrying.
      STOPPED_BY_FAILOVER,
      // Reconnects are fenced, but addresses and cleanup are retained until server unregistration completes.
      REMOVING,
      TERMINATED
   }
}
