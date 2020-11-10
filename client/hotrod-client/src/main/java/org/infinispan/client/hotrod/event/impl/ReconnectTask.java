package org.infinispan.client.hotrod.event.impl;

import java.util.concurrent.ScheduledFuture;
import java.util.function.Consumer;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.util.Util;

public class ReconnectTask implements Runnable, Consumer<Short> {
   private static final Log log = LogFactory.getLog(ReconnectTask.class);

   private final EventDispatcher dispatcher;
   private ScheduledFuture<?> cancellationFuture;
   private boolean completed = false;

   public ReconnectTask(EventDispatcher dispatcher) {
      this.dispatcher = dispatcher;
   }

   @Override
   public void run() {
      if (log.isTraceEnabled()) {
         log.tracef("Reconnecting client listener with id %s", Util.printArray(dispatcher.listenerId));
      }
      dispatcher.executeFailover().thenAccept(this);
   }

   public synchronized void setCancellationFuture(ScheduledFuture<?> cancellationFuture) {
      this.cancellationFuture = cancellationFuture;
      if (completed) {
         cancellationFuture.cancel(false);
      }
   }

   @Override
   public synchronized void accept(Short status) {
      ScheduledFuture<?> cancellationFuture = this.cancellationFuture;
      completed = true;
      if (cancellationFuture != null) {
         cancellationFuture.cancel(false);
      }
   }
}
