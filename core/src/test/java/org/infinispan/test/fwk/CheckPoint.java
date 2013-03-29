/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
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

package org.infinispan.test.fwk;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.logging.Logger;

/**
 * Behaves more or less like a map of {@link java.util.concurrent.Semaphore}s.
 *
 * One thread will wait for an event via {@code await(...)} or {@code awaitStrict(...)}, and one or more
 * other threads will trigger the event via {@code trigger(...)} or {@code triggerForever(...)}.
 *
 * @author Dan Berindei
 * @since 5.3
 */
public class CheckPoint {
   private static final Log log = LogFactory.getLog(CheckPoint.class);
   public static final int INFINITE = 999999999;
   private final Lock lock = new ReentrantLock();
   private final Condition unblockCondition = lock.newCondition();
   private final Map<String, Integer> events = new HashMap<String, Integer>();

   public void awaitStrict(String event, long timeout, TimeUnit unit)
         throws InterruptedException, TimeoutException {
      awaitStrict(event, 1, timeout, unit);
   }

   public boolean await(String event, long timeout, TimeUnit unit) throws InterruptedException {
      return await(event, 1, timeout, unit);
   }

   public void awaitStrict(String event, int count, long timeout, TimeUnit unit)
         throws InterruptedException, TimeoutException {
      if (!await(event, count, timeout, unit)) {
         throw new TimeoutException("Timed out waiting for event " + event);
      }
   }

   public boolean await(String event, int count, long timeout, TimeUnit unit) throws InterruptedException {
      log.tracef("Waiting for event %s * %d", event, count);
      lock.lock();
      try {
         long waitNanos = unit.toNanos(timeout);
         while (waitNanos > 0) {
            Integer currentCount = events.get(event);
            if (currentCount != null && currentCount >= count) {
               events.put(event, currentCount - count);
               break;
            }
            waitNanos = unblockCondition.awaitNanos(waitNanos);
         }

         if (waitNanos <= 0) {
            log.tracef("Timed out waiting for event %s * %d", event, count);
            // let the triggering thread know that we timed out
            events.put(event, -1);
            return false;
         }

         return true;
      } finally {
         lock.unlock();
      }
   }

   public void trigger(String event) {
      trigger(event, 1);
   }

   public void triggerForever(String event) {
      trigger(event, INFINITE);
   }

   public void trigger(String event, int count) {
      lock.lock();
      try {
         Integer currentCount = events.get(event);
         if (currentCount == null) {
            currentCount = 0;
         } else if (currentCount < 0) {
            throw new IllegalStateException("Thread already timed out waiting for event " + event);
         }

         // If triggerForever is called more than once, it will cause an overflow and the waiters will fail.
         int newCount = count != INFINITE ? currentCount + count : INFINITE;
         log.tracef("Triggering event %s * %d, new count is %d", event, count, newCount);
         events.put(event, newCount);
         unblockCondition.signalAll();
      } finally {
         lock.unlock();
      }
   }
}
