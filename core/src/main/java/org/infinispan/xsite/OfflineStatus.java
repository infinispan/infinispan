/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.xsite;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.configuration.cache.TakeOfflineConfiguration;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

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
 * @since 5.2
 */
@ThreadSafe
public class OfflineStatus {

   private static Log log = LogFactory.getLog(OfflineStatus.class);

   private final TimeService timeService;
   private volatile TakeOfflineConfiguration takeOffline;
   private boolean recordingOfflineStatus = false;
   private long firstFailureTime;
   private int failureCount;
   private volatile boolean forceOffline = false;

   public OfflineStatus(TakeOfflineConfiguration takeOfflineConfiguration, TimeService timeService) {
      this.takeOffline = takeOfflineConfiguration;
      this.timeService = timeService;
   }

   public synchronized void updateOnCommunicationFailure(long sendTimeMillis) {
      if (!recordingOfflineStatus) {
         recordingOfflineStatus = true;
         firstFailureTime = sendTimeMillis;
      }
      failureCount++;
   }

   public synchronized boolean isOffline() {
      if (forceOffline)
         return true;

      if (!recordingOfflineStatus)
         return false;

      if (takeOffline.minTimeToWait() > 0) { //min time to wait is enabled
         if (!minTimeHasElapsed()) return false;
      }

      if (takeOffline.afterFailures() > 0) {
         if (takeOffline.afterFailures() <= failureCount) {
            return true;
         } else {
            return false;
         }
      } else {
         log.trace("Site is failed: minTimeToWait elapsed and we don't have a min failure number to wait for.");
         return true;
      }
   }

   public synchronized boolean minTimeHasElapsed() {
      if (takeOffline.minTimeToWait() <= 0)
         throw new IllegalStateException("Cannot invoke this method if minTimeToWait is not enabled");
      long millis = millisSinceFirstFailure();
      if (millis >= takeOffline.minTimeToWait()) {
         log.tracef("The minTimeToWait has passed: minTime=%s, timeSinceFirstFailure=%s",
                    takeOffline.minTimeToWait(), millis);
         return true;
      }
      return false;
   }

   public synchronized long millisSinceFirstFailure() {
      return timeService.timeDuration(MILLISECONDS.toNanos(firstFailureTime), MILLISECONDS);
   }

   public synchronized boolean bringOnline() {
      if (!isOffline()) return false;
      reset();
      return true;
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

   public void reset() {
      recordingOfflineStatus = false;
      failureCount = 0;
      forceOffline = false;
   }

   public TakeOfflineConfiguration getTakeOffline() {
      return takeOffline;
   }

   public boolean forceOffline() {
      if (isOffline()) return false;
      forceOffline = true;
      return true;
   }

   @Override
   public String toString() {
      return "OfflineStatus{" +
            "takeOffline=" + takeOffline +
            ", recordingOfflineStatus=" + recordingOfflineStatus +
            ", firstFailureTime=" + firstFailureTime +
            ", forceOffline=" + forceOffline +
            ", failureCount=" + failureCount +
            '}';
   }

   public void amend(Integer afterFailures, Long minTimeToWait) {
      TakeOfflineConfiguration existing = getTakeOffline();
      int newAfterFailures = afterFailures == null ? existing.afterFailures() : afterFailures;
      long newMinTieToWait = minTimeToWait == null ? existing.minTimeToWait() : minTimeToWait;
      TakeOfflineConfiguration newConfig = new TakeOfflineConfiguration(newAfterFailures, newMinTieToWait);
      amend(newConfig);
   }
}
