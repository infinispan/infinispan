package org.infinispan.xsite.irac;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import org.infinispan.commons.util.IntSet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.status.DefaultTakeOfflineManager;

/**
 * A response collector for an asynchronous cross site requests.
 * <p>
 * Multiple keys are batched together in a single requests. The remote site sends a {@link IntSet} back where if bit
 * {@code n} is set, it means the {@code n}th key in the batch failed to be applied (example, lock failed to be
 * acquired), and it needs to be retried.
 * <p>
 * If an {@link Exception} is received (example, timed-out waiting for the remote site ack), it assumes all keys in the
 * batch aren't applied, and they are retried.
 * <p>
 * When the response (or exception) is received, {@link IracResponseCompleted#onResponseCompleted(IracBatchSendResult, Collection)} is invoked with the global result in {@link IracBatchSendResult} and a collection
 * with all the successfully applied keys. Once the listener finishes execution, the {@link CompletableFuture} completes
 * (completed value not relevant, and it is never completed exceptionally).
 *
 * @author Pedro Ruivo
 * @since 12
 */
public class IracResponseCollector extends CompletableFuture<Void> implements BiConsumer<IntSet, Throwable> {

   private static final Log log = LogFactory.getLog(IracResponseCollector.class);
   private final IracXSiteBackup backup;
   private final String cacheName;
   private final Collection<IracManagerKeyState> batch;
   private final IracResponseCompleted listener;

   public IracResponseCollector(String cacheName, IracXSiteBackup backup, Collection<IracManagerKeyState> batch, IracResponseCompleted listener) {
      this.cacheName = cacheName;
      this.backup = backup;
      this.batch = batch;
      this.listener = listener;
   }

   @Override
   public void accept(IntSet rspIntSet, Throwable throwable) {
      boolean trace = log.isTraceEnabled();
      if (throwable != null) {
         IracBatchSendResult result;
         if (DefaultTakeOfflineManager.isCommunicationError(throwable)) {
            //in case of communication error, we need to back-off.
            backup.enableBackOff();
            result = IracBatchSendResult.BACK_OFF_AND_RETRY;
         } else {
            //don't overwrite communication errors
            backup.resetBackOff();
            result = IracBatchSendResult.RETRY;
         }
         if (backup.logExceptions()) {
            log.warnXsiteBackupFailed(cacheName, backup.getSiteName(), throwable);
         } else if (trace) {
            log.tracef(throwable, "[IRAC] Encountered issues while backing up data for cache %s to site %s", cacheName, backup.getSiteName());
         }

         batch.forEach(IracManagerKeyState::retry);
         notifyAndComplete(result, Collections.emptyList());
         return;
      }

      if (trace) {
         log.tracef("[IRAC] Received response from site %s for cache %s: %s", backup.getSiteName(), cacheName, rspIntSet);
      }

      backup.resetBackOff();

      // Everything is good.
      if (rspIntSet == null || rspIntSet.isEmpty()) {
         for (IracManagerKeyState state : batch) {
            state.successFor(backup);
         }
         notifyAndComplete(IracBatchSendResult.OK, batch);
         return;
      }

      // Handle failed keys.
      int index = 0;
      List<IracManagerKeyState> successfulSent = new ArrayList<>(batch.size());
      for (IracManagerKeyState state : batch) {
         if (rspIntSet.contains(index++)) {
            state.retry();
            continue;
         }
         state.successFor(backup);
         successfulSent.add(state);
      }
      notifyAndComplete(IracBatchSendResult.RETRY, successfulSent);
   }

   private void notifyAndComplete(IracBatchSendResult result, Collection<IracManagerKeyState> successfulSent) {
      listener.onResponseCompleted(result, successfulSent);
      complete(null);
   }

   @FunctionalInterface
   public interface IracResponseCompleted {
      void onResponseCompleted(IracBatchSendResult result, Collection<IracManagerKeyState> successfulSent);
   }
}
