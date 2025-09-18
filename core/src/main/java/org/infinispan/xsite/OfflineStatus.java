package org.infinispan.xsite;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.infinispan.Cache;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.TakeOfflineConfiguration;
import org.infinispan.configuration.cache.TakeOfflineConfigurationBuilder;
import org.infinispan.globalstate.ScopedState;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.notification.SiteStatusListener;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.errorprone.annotations.ThreadSafe;

/**
 * Keeps the state needed for knowing when a site needs to be taken offline.
  * Thread safety: This class is updated from multiple threads, so the access to it is synchronized by the object's
 * intrinsic lock.
  * Impl detail: As this class's state changes constantly, the equals and hashCode haven't been overridden. This
 * shouldn't affect performance significantly as the number of site backups should be relatively small (1-3).
 *
 * @author Mircea Markus
 * @author Pedro Ruivo
 * @since 5.2
 */
@ThreadSafe
@Listener(observation = Listener.Observation.POST)
public class OfflineStatus implements CacheEventFilter<Object, Object> {

   private static final Log log = LogFactory.getLog(OfflineStatus.class);
   private static final long NO_FAILURE = -1;
   private static final int MAX_RETRIES = 3;

   private final Supplier<TimeService> timeService;
   private final SiteStatusListener listener;
   private final ScopedState key;
   private final Cache<ScopedState, Long> globalState;
   private volatile TakeOfflineConfiguration takeOffline;
   @GuardedBy("this") private long firstFailureTime = NO_FAILURE;
   @GuardedBy("this") private int failureCount = 0;
   @GuardedBy("this") private long localStatus = 0;

   private static boolean isOnline(long localStatus) {
      return localStatus % 2 == 0;
   }

   private static boolean isOffline(long localStatus) {
      return localStatus % 2 == 1;
   }

   public OfflineStatus(TakeOfflineConfiguration takeOfflineConfiguration, Supplier<TimeService> timeService, SiteStatusListener listener, ScopedState key, Cache<ScopedState, Long> globalState) {
      this.takeOffline = takeOfflineConfiguration;
      this.timeService = timeService;
      this.listener = listener;
      this.key = key;
      this.globalState = globalState;
   }

   public void start() {
      globalState.addFilteredListener(this, this, null, Set.of(CacheEntryModified.class, CacheEntryCreated.class));
      var existingStatus = globalState.get(key);
      if (existingStatus != null) {
         updateLocalStatus(existingStatus);
         return;
      }
      globalState.putIfAbsentAsync(key, 0L).thenAccept(status -> {
         if (status != null){
            updateLocalStatus(status);
         }
      });
   }

   public void stop() {
      globalState.removeListener(this);
   }

   public synchronized void updateOnCommunicationFailure(long sendTimeMillis) {
      if (firstFailureTime == NO_FAILURE) {
         firstFailureTime = sendTimeMillis;
      }
      failureCount++;
      internalUpdateStatus();
   }

   public synchronized boolean isOffline() {
      return isOffline(localStatus);
   }

   public synchronized boolean minTimeHasElapsed() {
      if (firstFailureTime == NO_FAILURE) {
         return false;
      }
      return internalMinTimeHasElapsed();
   }

   public synchronized long millisSinceFirstFailure() {
      return internalMillisSinceFirstFailure();
   }

   public synchronized CompletionStage<Boolean> bringOnline() {
      return isOffline(localStatus) ? internalReset() : CompletableFutures.completedFalse();
   }

   public synchronized int getFailureCount() {
      return failureCount;
   }

   public synchronized boolean isEnabled() {
      return takeOffline.enabled();
   }

   public synchronized void reset() {
      internalReset();
   }

   public TakeOfflineConfiguration getTakeOffline() {
      return takeOffline;
   }

   public synchronized CompletionStage<Boolean> forceOffline() {
      var status = localStatus;
      return isOffline(status) ?
            CompletableFutures.completedFalse() :
            switchGlobally(status);
   }

   @Override
   public synchronized String toString() {
      return "OfflineStatus{" +
             "takeOffline=" + takeOffline +
             ", recordingOfflineStatus=" + (firstFailureTime != NO_FAILURE) +
             ", firstFailureTime=" + firstFailureTime +
            ", isOffline=" + isOffline(localStatus) +
             ", failureCount=" + failureCount +
             '}';
   }

   public void amend(Integer afterFailures, Long minTimeToWait) {
      TakeOfflineConfigurationBuilder builder = new TakeOfflineConfigurationBuilder(null, null);
      builder.read(getTakeOffline(), Combine.DEFAULT);
      if (afterFailures != null) {
         builder.afterFailures(afterFailures);
      }
      if (minTimeToWait != null) {
         builder.minTimeToWait(minTimeToWait);
      }
      amend(builder.create());
   }

   /**
    * Configures the site to use the supplied configuration for determining when to take a site offline. Also triggers a
    * state reset.
    */
   private void amend(TakeOfflineConfiguration takeOffline) {
      this.takeOffline = takeOffline;
      reset();
   }

   @GuardedBy("this")
   private void internalUpdateStatus() {
      if (isOffline(localStatus)) {
         //already offline
         return;
      }
      boolean hasMinWait = takeOffline.minTimeToWait() > 0;
      if (hasMinWait && !internalMinTimeHasElapsed()) {
         //minTimeToWait() not elapsed yet.
         return;
      }
      long minFailures = takeOffline.afterFailures();
      if (minFailures > 0) {
         if (minFailures <= failureCount) {
            if (log.isTraceEnabled()) {
               log.tracef("Site is failed: min failures (%s) reached (count=%s).", minFailures, failureCount);
            }
            switchGlobally(localStatus);
         }
         //else, afterFailures() not reached yet.
      } else if (hasMinWait) {
         if (log.isTraceEnabled()) {
            log.trace("Site is failed: minTimeToWait elapsed and we don't have a min failure number to wait for.");
         }
         switchGlobally(localStatus);
      }
      //else, no afterFailures() neither minTimeToWait() configured
   }

   @GuardedBy("this")
   private boolean internalMinTimeHasElapsed() {
      long minTimeToWait = takeOffline.minTimeToWait();
      if (minTimeToWait <= 0)
         throw new IllegalStateException("Cannot invoke this method if minTimeToWait is not enabled");
      long millis = internalMillisSinceFirstFailure();
      if (millis >= minTimeToWait) {
         if (log.isTraceEnabled()) {
            log.tracef("The minTimeToWait has passed: minTime=%s, timeSinceFirstFailure=%s",
                  minTimeToWait, millis);
         }
         return true;
      }
      return false;
   }

   @GuardedBy("this")
   private CompletionStage<Boolean> internalReset() {
      boolean wasOffline = isOffline(localStatus);
      firstFailureTime = NO_FAILURE;
      failureCount = 0;
      if (wasOffline) {
         return switchGlobally(localStatus);
      }
      return CompletableFutures.completedFalse();
   }

   @GuardedBy("this")
   private long internalMillisSinceFirstFailure() {
      return timeService.get().timeDuration(MILLISECONDS.toNanos(firstFailureTime), MILLISECONDS);
   }

   private CompletionStage<Boolean> switchGlobally(long currentStatus) {
      var rsp = replaceInCache(currentStatus, currentStatus + 1, 0);
      // optimistically switch assuming the “replace” eventually completes
      updateLocalStatus(currentStatus + 1);
      return rsp;
   }

   private CompletionStage<Boolean> replaceInCache(long oldStatus, long newStatus, int retry) {
      return CompletionStages.handleAndCompose(
            globalState.replaceAsync(key, oldStatus, newStatus),
            (replaced, throwable) -> handleReplaceResponse(replaced, throwable, oldStatus, newStatus, retry));
   }

   private CompletionStage<Boolean> handleReplaceResponse(Boolean replaced, Throwable throwable, long oldStatus, long newStatus, int retry) {
      if (throwable != null) {
         if (retry <= MAX_RETRIES) {
            return replaceInCache(oldStatus, newStatus, retry + 1);
         }
         return CompletableFutures.completedFalse();
      }
      return replaced ? CompletableFutures.completedTrue() : CompletableFutures.completedFalse();
   }

   private void updateLocalStatus(long newStatus) {
      synchronized (this) {
         if (newStatus <= localStatus) {
            return;
         }
         localStatus = newStatus;
      }
      if (isOnline(newStatus)) {
         listener.siteOnline();
      } else {
         listener.siteOffline();
      }
   }

   @Override
   public synchronized boolean accept(Object key, Object oldValue, Metadata oldMetadata, Object newValue, Metadata newMetadata, EventType eventType) {
      return Objects.equals(key, this.key) && newValue instanceof Long newStatus && newStatus > localStatus;
   }

   @CacheEntryCreated
   @CacheEntryModified
   public void onStatusChanged(CacheEntryEvent<ScopedState, Long> event) {
      assert !event.isPre();
      assert event.getValue() != null;
      assert Objects.equals(key, event.getKey());
      updateLocalStatus(event.getValue());
   }
}
