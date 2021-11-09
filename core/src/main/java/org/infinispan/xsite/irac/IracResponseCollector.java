package org.infinispan.xsite.irac;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

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
public class IracResponseCollector extends CompletableFuture<IracResponseCollector.Result> {

   private static final Log log = LogFactory.getLog(IracResponseCollector.class);

   @GuardedBy("this")
   private Result result = Result.OK;
   @GuardedBy("this")
   private int missing;
   @GuardedBy("this")
   private boolean frozen = false;
   private final String cacheName;

   public IracResponseCollector(String cacheName) {
      this.cacheName = cacheName;
   }

   public void dependsOn(IracXSiteBackup backup, CompletionStage<Void> request) {
      synchronized (this) {
         missing++;
      }
      request.whenComplete((unused, throwable) -> onResponse(backup, throwable));
   }

   public IracResponseCollector freeze() {
      Result completeResult = null;
      synchronized (this) {
         frozen = true;
         if (missing == 0) {
            completeResult = result;
         }
         if (log.isTraceEnabled()) {
            log.tracef("Freeze collector. result=%s, missing=%s", result, missing);
         }
      }
      tryComplete(completeResult);
      return this;
   }

   private void onResponse(IracXSiteBackup backup, Throwable throwable) {
      final boolean trace = log.isTraceEnabled();
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
            if (backup.logExceptions()) {
               log.warnXsiteBackupFailed(cacheName, backup.getSiteName(), throwable);
            } else if (trace) {
               log.tracef(throwable, "Encountered issues while backing up data for cache %s to site %s", cacheName, backup.getSiteName());
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
      if (log.isTraceEnabled()) {
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
