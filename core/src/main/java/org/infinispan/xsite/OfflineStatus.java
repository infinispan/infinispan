package org.infinispan.xsite;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.TakeOfflineConfiguration;
import org.infinispan.configuration.cache.TakeOfflineConfigurationBuilder;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.notification.SiteStatusListener;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

/**
 * Keeps state needed for knowing when a site needs to be taken offline.
 * <p/>
 * Thread safety: This class is updated from multiple threads so the access to it is synchronized by object's intrinsic
 * lock.
 * <p/>
 * Impl detail: As this class's state changes constantly, the equals and hashCode haven't been overridden. This
 * shouldn't affect performance significantly as the number of site backups should be relatively small (1-3).
 *
 * @author Mircea Markus
 * @author Pedro Ruivo
 * @since 5.2
 */
@ThreadSafe
public class OfflineStatus {

   private static final Log log = LogFactory.getLog(OfflineStatus.class);
   private static final boolean trace = log.isTraceEnabled();

   private final TimeService timeService;
   private final SiteStatusListener listener;
   private volatile TakeOfflineConfiguration takeOffline;
   private boolean recordingOfflineStatus = false;
   private long firstFailureTime;
   private int failureCount;
   private boolean isOffline = false;

   public OfflineStatus(TakeOfflineConfiguration takeOfflineConfiguration, TimeService timeService, SiteStatusListener listener) {
      this.takeOffline = takeOfflineConfiguration;
      this.timeService = timeService;
      this.listener = listener;
   }

   public synchronized void updateOnCommunicationFailure(long sendTimeMillis) {
      if (!recordingOfflineStatus) {
         recordingOfflineStatus = true;
         firstFailureTime = sendTimeMillis;
      }
      failureCount++;
   }

   public synchronized boolean isOffline() {
      if (isOffline)
         return true;

      if (!recordingOfflineStatus)
         return false;

      if (takeOffline.minTimeToWait() > 0) { //min time to wait is enabled
         if (!internalMinTimeHasElapsed()) return false;
      }

      if (takeOffline.afterFailures() > 0) {
         if (takeOffline.afterFailures() <= failureCount) {
            if (trace) {
               log.trace("Site is failed: min failures reached.");
            }
            listener.siteOffline();
            isOffline = true;
            return true;
         } else {
            return false;
         }
      } else {
         if (takeOffline.minTimeToWait() > 0) { //min time to wait is enabled
            if (trace) {
               log.trace("Site is failed: minTimeToWait elapsed and we don't have a min failure number to wait for.");
            }
            listener.siteOffline();
            isOffline = true;
            return true;
         } else {
            return false;
         }
      }
   }

   public synchronized boolean minTimeHasElapsed() {
      return internalMinTimeHasElapsed();
   }

   public synchronized long millisSinceFirstFailure() {
      return internalMillisSinceFirstFailure();
   }

   public synchronized boolean bringOnline() {
      return isOffline && internalReset();
   }

   public synchronized int getFailureCount() {
      return failureCount;
   }

   public synchronized boolean isEnabled() {
      return takeOffline.enabled();
   }

   /**
    * Configures the site to use the supplied configuration for determining when to take a site offline.
    * Also triggers a state reset.
    */
   public void amend(TakeOfflineConfiguration takeOffline) {
      this.takeOffline = takeOffline;
      reset();
   }

   public synchronized void reset() {
      internalReset();
   }

   public TakeOfflineConfiguration getTakeOffline() {
      return takeOffline;
   }

   public synchronized boolean forceOffline() {
      if (isOffline) {
         return false;
      }
      isOffline = true;
      listener.siteOffline();
      return true;
   }

   @Override
   public String toString() {
      return "OfflineStatus{" +
            "takeOffline=" + takeOffline +
            ", recordingOfflineStatus=" + recordingOfflineStatus +
            ", firstFailureTime=" + firstFailureTime +
            ", isOffline=" + isOffline +
            ", failureCount=" + failureCount +
            '}';
   }

   public void amend(Integer afterFailures, Long minTimeToWait) {
      TakeOfflineConfigurationBuilder builder = new TakeOfflineConfigurationBuilder(null, null);
      builder.read(getTakeOffline());
      if (afterFailures != null) {
         builder.afterFailures(afterFailures);
      }
      if (minTimeToWait != null) {
         builder.minTimeToWait(minTimeToWait);
      }
      amend(builder.create());
   }

   @GuardedBy("this")
   private boolean internalMinTimeHasElapsed() {
      long minTimeToWait = takeOffline.minTimeToWait();
      if (minTimeToWait <= 0)
         throw new IllegalStateException("Cannot invoke this method if minTimeToWait is not enabled");
      long millis = internalMillisSinceFirstFailure();
      if (millis >= minTimeToWait) {
         if (trace) {
            log.tracef("The minTimeToWait has passed: minTime=%s, timeSinceFirstFailure=%s",
                       minTimeToWait, millis);
         }
         return true;
      }
      return false;
   }

   /**
    * @return true if status changed
    */
   @GuardedBy("this")
   private boolean internalReset() {
      boolean wasOffline = isOffline;
      recordingOfflineStatus = false;
      failureCount = 0;
      isOffline = false;
      if (wasOffline) {
         listener.siteOnline();
      }
      return wasOffline;
   }

   @GuardedBy("this")
   private long internalMillisSinceFirstFailure() {
      return timeService.timeDuration(MILLISECONDS.toNanos(firstFailureTime), MILLISECONDS);
   }
}
