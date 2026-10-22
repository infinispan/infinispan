package org.infinispan.client.hotrod.event.impl;

import java.net.SocketAddress;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Consumer;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.util.Util;

public class ReconnectTask implements Runnable, Consumer<Void> {
   private static final Log log = LogFactory.getLog(ReconnectTask.class);

   private final EventDispatcher<?> dispatcher;
   private final SocketAddress failedAddress;
   private ScheduledFuture<?> cancellationFuture;
   private boolean completed = false;

   public ReconnectTask(EventDispatcher<?> dispatcher, SocketAddress failedAddress) {
      this.dispatcher = dispatcher;
      this.failedAddress = failedAddress;
   }

   @Override
   public void run() {
      if (!dispatcher.isRunning()) {
         accept(null);
         return;
      }
      if (log.isTraceEnabled()) {
         log.tracef("Reconnecting client listener with id %s on %s", Util.printArray(dispatcher.listenerId), failedAddress);
      }
      dispatcher.executeFailover(failedAddress).thenAccept(this);
   }

   public synchronized void setCancellationFuture(ScheduledFuture<?> cancellationFuture) {
      this.cancellationFuture = cancellationFuture;
      if (completed) {
         cancellationFuture.cancel(false);
      }
   }

   @Override
   public synchronized void accept(Void address) {
      ScheduledFuture<?> cancellationFuture = this.cancellationFuture;
      completed = true;
      if (cancellationFuture != null) {
         cancellationFuture.cancel(false);
      }
   }
}
