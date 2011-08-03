/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.remoting.transport.jgroups;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.remoting.transport.DistributedSync;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.infinispan.util.concurrent.ReclosableLatch;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static org.infinispan.util.Util.prettyPrintTime;

/**
 * A DistributedSync based on JGroups' FLUSH protocol
 *
 * @author Manik Surtani
 * @since 4.0
 */
@ThreadSafe
public class JGroupsDistSync implements DistributedSync {

   private final ReentrantReadWriteLock processingLock = new ReentrantReadWriteLock();
   private final ReclosableLatch flushBlockGate = new ReclosableLatch(true);
   private final AtomicInteger flushBlockGateCount = new AtomicInteger(0);
   private final AtomicInteger flushWaitGateCount = new AtomicInteger(0);
   private final ReclosableLatch flushWaitGate = new ReclosableLatch(false);
   private static final Log log = LogFactory.getLog(JGroupsDistSync.class);
   private static final boolean trace = log.isTraceEnabled();

   private final ConcurrentHashSet<Thread> debugReadLockHolders = new ConcurrentHashSet<Thread>();

   public SyncResponse blockUntilAcquired(long timeout, TimeUnit timeUnit) throws TimeoutException {
      int initState = flushWaitGateCount.get();
      try {
         if (!flushWaitGate.await(timeout, timeUnit))
            throw new TimeoutException("Timed out waiting for a cluster-wide sync to be acquired. (timeout = " + prettyPrintTime(timeout) + ")");
      } catch (InterruptedException ie) {
         currentThread().interrupt();
      }

      return initState == flushWaitGateCount.get() ? SyncResponse.STATE_PREEXISTED : SyncResponse.STATE_ACHIEVED;

   }

   public SyncResponse blockUntilReleased(long timeout, TimeUnit timeUnit) throws TimeoutException {
      int initState = flushBlockGateCount.get();
      try {
         if (!flushBlockGate.await(timeout, timeUnit))
            throw new TimeoutException("Timed out waiting for a cluster-wide sync to be released. (timeout = " + prettyPrintTime(timeout) + ")");
      } catch (InterruptedException ie) {
         currentThread().interrupt();
      }


      return initState == flushWaitGateCount.get() ? SyncResponse.STATE_PREEXISTED : SyncResponse.STATE_ACHIEVED;
   }

   public void acquireSync() {
      flushBlockGate.close();
      flushWaitGateCount.incrementAndGet();
      flushWaitGate.open();
   }

   public void releaseSync() {
      flushWaitGate.close();
      flushBlockGateCount.incrementAndGet();
      flushBlockGate.open();
   }

   public void acquireProcessingLock(boolean exclusive, long timeout, TimeUnit timeUnit) throws TimeoutException {
      Lock lock = exclusive ? processingLock.writeLock() : processingLock.readLock();
      try {
         if (!lock.tryLock(timeout, timeUnit)) {
            if (exclusive) log.debugf("Failed to acquire exclusive processing lock. Read lock holders are %s", debugReadLockHolders);
            throw new TimeoutException(format("%s could not obtain %s processing lock after %s.  Locks in question are %s and %s",
                  currentThread().getName(), exclusive ? "exclusive" : "shared", prettyPrintTime(timeout, timeUnit),
                  processingLock.readLock(), processingLock.writeLock()));
         }
         if (log.isDebugEnabled() && !exclusive) debugReadLockHolders.add(currentThread());
      } catch (InterruptedException ie) {
         currentThread().interrupt();
      }
   }

   public void releaseProcessingLock(boolean exclusive) {
      try {
         if (exclusive) {
            processingLock.writeLock().unlock();
         } else {
            processingLock.readLock().unlock();
            if (log.isDebugEnabled()) debugReadLockHolders.remove(currentThread());
         }
      } catch (IllegalMonitorStateException imse) {
         if (trace) log.trace("Did not own lock!");
      }
   }
}
