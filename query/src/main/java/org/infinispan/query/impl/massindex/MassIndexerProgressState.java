package org.infinispan.query.impl.massindex;

import java.util.concurrent.CompletableFuture;

import org.hibernate.search.util.common.impl.Futures;
import org.infinispan.commons.util.ProgressTracker;
import org.infinispan.query.core.impl.Log;

class MassIndexerProgressState {

   private static final Log LOG = Log.getLog(IndexUpdater.class);

   private final MassIndexerProgressNotifier notifier;
   private final ProgressTracker progressTracker;

   private CompletableFuture<?> lastFuture = CompletableFuture.completedFuture( null );

   public MassIndexerProgressState(MassIndexerProgressNotifier notifier, ProgressTracker progressTracker) {
      this.notifier = notifier;
      this.progressTracker = progressTracker;
   }

   public void addItem(Object key, Object value, CompletableFuture<?> future) {
      // This is what HS5 currently does, but introduce chunking could be a good idea.
      lastFuture = future.whenComplete((result, exception) -> {
         if (exception != null) {
            notifier.notifyEntityIndexingFailure(value.getClass(), key, exception);
         } else {
            notifier.notifyDocumentsAdded(1);
         }
         progressTracker.removeTasks(1);
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
