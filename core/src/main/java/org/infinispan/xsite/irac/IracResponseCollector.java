package org.infinispan.xsite.irac;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.util.concurrent.CountDownRunnable;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.status.DefaultTakeOfflineManager;

import net.jcip.annotations.GuardedBy;

/**
 * A response collector for an asynchronous cross site requests.
 * <p>
 * Multiple keys are batched together in a single requests. The remote site sends a {@link BitSet} back where if bit
 * {@code n} is set, it means the {@code n}th key in the batch failed to be applied (example, lock failed to be
 * acquired), and it needs to be retried.
 * <p>
 * If an {@link Exception} is received (example, timed-out waiting for the remote site ack), it assumes all keys in the
 * batch aren't applied, and they are retried.
 * <p>
 * When all responses (or exceptions) are received, {@link IracResponseCompleted#onResponseCompleted(IracBatchSendResult,
 * Collection)} is invoked with the global result in {@link IracBatchSendResult} and a collection with all the
 * successfully applied keys. Also, the {@link CompletableFuture} returned by {@link #freeze()} is completed (completed
 * value not relevant, and it is never completed exceptionally).
 *
 * @author Pedro Ruivo
 * @since 12
 */
public class IracResponseCollector implements Runnable {

   private static final Log log = LogFactory.getLog(IracResponseCollector.class);
   private static final AtomicReferenceFieldUpdater<IracResponseCollector, IracBatchSendResult> RESULT_UPDATED = AtomicReferenceFieldUpdater.newUpdater(IracResponseCollector.class, IracBatchSendResult.class, "result");

   private volatile IracBatchSendResult result = IracBatchSendResult.OK;
   private volatile boolean exceptionReceived;
   @GuardedBy("failedKeys")
   private final IntSet failedKeys;
   private final Collection<IracManagerKeyState> batch;
   private final IracResponseCompleted listener;
   private final CountDownRunnable countDownRunnable;
   private final CompletableFuture<Void> completableFuture = new CompletableFuture<>();

   public IracResponseCollector(Collection<IracManagerKeyState> batch, IracResponseCompleted listener) {
      this.batch = batch;
      this.listener = listener;
      countDownRunnable = new CountDownRunnable(this);
      failedKeys = IntSets.mutableEmptySet(batch.size());
   }

   public void dependsOn(XSiteBackup backup, CompletionStage<? extends IntSet> request) {
      countDownRunnable.increment();
      request.whenComplete((bitSet, throwable) -> onResponse(backup, bitSet, throwable));
   }

   public CompletionStage<Void> freeze() {
      countDownRunnable.freeze();
      return completableFuture;
   }

   private void onResponse(XSiteBackup backup, IntSet rspIntSet, Throwable throwable) {
      boolean trace = log.isTraceEnabled();
      try {
         if (throwable != null) {
            exceptionReceived = true;
            if (DefaultTakeOfflineManager.isCommunicationError(throwable)) {
               //in case of communication error, we need to back-off.
               RESULT_UPDATED.set(this, IracBatchSendResult.BACK_OFF_AND_RETRY);
            } else if (result == IracBatchSendResult.OK) {
               //don't overwrite communication errors
               RESULT_UPDATED.compareAndSet(this, IracBatchSendResult.OK, IracBatchSendResult.RETRY);
            }
         } else {
            if (trace) {
               log.tracef("[IRAC] Received response from site %s (%d missing): %s", backup.getSiteName(), countDownRunnable.missing(), rspIntSet);
            }
            mergeIntSetResult(rspIntSet);
            // if some keys failed to apply, we need to retry.
            if (!rspIntSet.isEmpty() && result == IracBatchSendResult.OK) {
               RESULT_UPDATED.compareAndSet(this, IracBatchSendResult.OK, IracBatchSendResult.RETRY);
            }
         }
      } finally {
         countDownRunnable.decrement();
      }
   }

   @Override
   public void run() {
      if (exceptionReceived) {
         batch.forEach(IracManagerKeyState::retry);
         listener.onResponseCompleted(result, Collections.emptyList());
         completableFuture.complete(null);
         return;
      }
      Collection<IracManagerKeyState> successfulSent = new ArrayList<>(batch.size());
      int index = 0;
      for (IracManagerKeyState state : batch) {
         if (hasKeyFailed(index)) {
            state.retry();
         } else if (state.done()) {
            successfulSent.add(state);
         }
      }
      listener.onResponseCompleted(result, successfulSent);
      completableFuture.complete(null);
   }

   private void mergeIntSetResult(IntSet rsp) {
      synchronized (failedKeys) {
         failedKeys.addAll(rsp);
      }
   }

   private boolean hasKeyFailed(int index) {
      synchronized (failedKeys) {
         return failedKeys.contains(index);
      }
   }

   @FunctionalInterface
   public interface IracResponseCompleted {
      void onResponseCompleted(IracBatchSendResult result, Collection<IracManagerKeyState> successfulSent);
   }
}
