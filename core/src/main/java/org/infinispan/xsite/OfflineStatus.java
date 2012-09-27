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
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.TimeUnit;

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

   private final TakeOfflineConfiguration takeOffline;
   private boolean recordingOfflineStatus = false;
   private long firstFailureTime;
   private int failureCount;

   public OfflineStatus(TakeOfflineConfiguration takeOfflineConfiguration) {
      this.takeOffline = takeOfflineConfiguration;
   }

   public synchronized void updateOnCommunicationFailure(long sendTimeMillis) {
      if (!recordingOfflineStatus) {
         recordingOfflineStatus = true;
         firstFailureTime = sendTimeMillis;
      }
      failureCount++;
   }

   public synchronized boolean isOffline() {
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
         log.tracef("The minTimeToWait hasn't passed: minTime=%s, timeSinceFirstFailure=%s",
                    takeOffline.minTimeToWait(), millis);
         return true;
      }
      return false;
   }

   public synchronized long millisSinceFirstFailure() {
      return TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) - firstFailureTime;
   }

   public synchronized boolean bringOnline() {
      if (!isOffline()) return false;
      updateOnCommunicationSuccess();
      return true;
   }

   public synchronized void updateOnCommunicationSuccess() {
      recordingOfflineStatus = false;
      failureCount = 0;
   }

   public synchronized int getFailureCount() {
      return failureCount;
   }

   @Override
   public String toString() {
      return "OfflineStatus{" +
            "takeOffline=" + takeOffline +
            ", recordingOfflineStatus=" + recordingOfflineStatus +
            ", firstFailureTime=" + firstFailureTime +
            ", failureCount=" + failureCount +
            '}';
   }
}
