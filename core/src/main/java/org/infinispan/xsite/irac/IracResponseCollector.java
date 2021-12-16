package org.infinispan.xsite.irac;

import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.infinispan.util.concurrent.CountDownRunnable;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.status.DefaultTakeOfflineManager;

/**
 * A response collector for a single key update.
 * <p>
 * This class extends {@link CompletableFuture}. It is completed when all sites involved in the updated have replied (or
 * timed-out). There are 3 possible outcomes enumerated by {@link Result}.
 *
 * @author Pedro Ruivo
 * @since 12
 */
public class IracResponseCollector implements Runnable {

   private static final Log log = LogFactory.getLog(IracResponseCollector.class);
   private static final AtomicReferenceFieldUpdater<IracResponseCollector, Result> RESULT_UPDATER = newUpdater(IracResponseCollector.class, Result.class, "result");

   private volatile Result result = Result.OK;
   private final IracManagerKeyState state;
   private final CountDownRunnable countDownRunnable;
   private final IracResponseCompleted notifier;

   public IracResponseCollector(IracManagerKeyState state, IracResponseCompleted notifier) {
      this.state = state;
      this.notifier = Objects.requireNonNull(notifier);
      countDownRunnable = new CountDownRunnable(this);
   }

   public void dependsOn(XSiteBackup backup, CompletionStage<Void> request) {
      countDownRunnable.increment();
      request.whenComplete((unused, throwable) -> onResponse(backup, throwable));
   }

   public void freeze() {
      countDownRunnable.freeze();
   }

   private void onResponse(XSiteBackup backup, Throwable throwable) {
      boolean trace = log.isTraceEnabled();
      try {
         if (throwable != null) {
            if (DefaultTakeOfflineManager.isCommunicationError(throwable)) {
               //in case of communication error, we need to back-off.
               RESULT_UPDATER.set(this, Result.NETWORK_EXCEPTION);
            } else {
               //don't overwrite communication errors
               RESULT_UPDATER.compareAndSet(this, Result.OK, Result.REMOTE_EXCEPTION);
            }
         } else if (trace) {
            log.tracef("Receive response from %s (%d missing). New result=%s", backup.getSiteName(), countDownRunnable.missing(), result);
         }
      } finally {
         countDownRunnable.decrement();
      }
   }


   @Override
   public void run() {
      if (log.isTraceEnabled()) {
         log.tracef("All responses received for state %s. global result is %s", state, result);
      }
      notifier.onResponseCompleted(state, result);
   }

   public enum Result {
      OK,
      REMOTE_EXCEPTION,
      NETWORK_EXCEPTION
   }

   @FunctionalInterface
   public interface IracResponseCompleted {
      void onResponseCompleted(IracManagerKeyState state, Result result);
   }
}
