package org.infinispan.query.impl.massindex;

import java.util.concurrent.CompletableFuture;

import org.hibernate.search.util.common.impl.Futures;

import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class MassIndexerProgressState {

   private static final Log LOG = LogFactory.getLog(IndexUpdater.class, Log.class);

   private final MassIndexerProgressNotifier notifier;

   private CompletableFuture<?> lastFuture = CompletableFuture.completedFuture( null );

   public MassIndexerProgressState(MassIndexerProgressNotifier notifier) {
      this.notifier = notifier;
   }

   public void addItem(Object key, Object value, CompletableFuture<?> future) {
      // This is what HS5 currently does, but introduce chunking could be a good idea.
      lastFuture = future.whenComplete((result, exception) -> {
         if (exception != null) {
            notifier.notifyEntityIndexingFailure(value.getClass(), key, exception);
         } else {
            notifier.notifyDocumentsAdded(1);
         }
      }).thenCombine(lastFuture, (ignored1, ignored2) -> null);
   }

   public void waitForAsyncCompletion() {
      try {
         Futures.unwrappedExceptionGet(lastFuture);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw LOG.interruptedWhileWaitingForRequestCompletion(e);
      }
   }
}
