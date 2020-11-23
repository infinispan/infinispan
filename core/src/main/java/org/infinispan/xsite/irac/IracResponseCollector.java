package org.infinispan.xsite.irac;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.status.DefaultTakeOfflineManager;

import net.jcip.annotations.GuardedBy;

/**
 * A response collector for a single key update.
 * <p>
 * This class extends {@link CompletableFuture}. It is completed when all sites involved in the updated have replied (or
 * timed-out). There are 3 possible outcomes enumerated by {@link Result}.
 *
 * @author Pedro Ruivo
 * @since 12
 */
public class IracResponseCollector extends CompletableFuture<IracResponseCollector.Result> implements BiConsumer<Void, Throwable> {

   private static final Log log = LogFactory.getLog(IracResponseCollector.class);
   private static final boolean trace = log.isTraceEnabled();

   @GuardedBy("this")
   private Result result = Result.OK;
   @GuardedBy("this")
   private int missing;
   @GuardedBy("this")
   private boolean frozen = false;

   public void dependsOn(CompletionStage<Void> request) {
      synchronized (this) {
         missing++;
      }
      request.whenComplete(this);
   }

   public IracResponseCollector freeze() {
      Result completeResult = null;
      synchronized (this) {
         frozen = true;
         if (missing == 0) {
            completeResult = result;
         }
         if (trace) {
            log.tracef("Freeze collector. result=%s, missing=%s", result, missing);
         }
      }
      tryComplete(completeResult);
      return this;
   }

   @Override
   public void accept(Void aVoid, Throwable throwable) {
      Result completeResult = null;
      synchronized (this) {
         Result old = result;
         if (throwable != null) {
            if (DefaultTakeOfflineManager.isCommunicationError(throwable)) {
               //in case of communication error, we need to back-off.
               result = Result.NETWORK_EXCEPTION;
            } else {
               //don't overwrite communication errors
               result = result == Result.OK ? Result.REMOTE_EXCEPTION : result;
            }
         }
         if (--missing == 0 && frozen) {
            completeResult = result;
         }
         if (trace) {
            log.tracef("Receive response. old=%s, new=%s, missing=%s", old, result, missing);
         }
      }
      tryComplete(completeResult);
   }


   private void tryComplete(Result result) {
      if (result == null) {
         return;
      }
      if (trace) {
         log.tracef("All response received: %s", result);
      }
      complete(result);
   }

   enum Result {
      OK,
      REMOTE_EXCEPTION,
      NETWORK_EXCEPTION
   }
}
