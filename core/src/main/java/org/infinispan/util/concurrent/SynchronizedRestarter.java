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
package org.infinispan.util.concurrent;

import org.infinispan.lifecycle.Lifecycle;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * A class that handles restarts of components via multiple threads.  Specifically, if a component needs to be restarted
 * and several threads may demand a restart but only one thread should be allowed to restart the component, then use
 * this class.
 * <p/>
 * What this class guarantees is that several threads may come in while a component is being restarted, but they will
 * block until the restart is complete.
 * <p/>
 * This is different from other techniques in that: <ul> <li>A simple compare-and-swap to check whether another thread
 * is already performing a restart will result in the requesting thread returning immediately and potentially attempting
 * to use the resource being restarted.</li> <li>A synchronized method or use of a lock would result in the thread
 * waiting for the restart to complete, but on completion will attempt to restart the component again.</li> </ul> This
 * implementation combines a compare-and-swap to detect a concurrent restart, as well as registering for notification
 * for when the restart completes and then parking the thread if the CAS variable still indicates a restart in progress,
 * and finally deregistering itself in the end.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class SynchronizedRestarter {
   private AtomicBoolean restartInProgress = new AtomicBoolean(false);
   private ConcurrentHashSet<Thread> restartWaiters = new ConcurrentHashSet<Thread>();

   public void restartComponent(Lifecycle component) throws Exception {
      // will only enter this block if no one else is restarting the socket
      // and will atomically set the flag so others won't enter
      if (restartInProgress.compareAndSet(false, true)) {
         try {
            component.stop();
            component.start();
         }
         finally {
            restartInProgress.set(false);
            for (Thread waiter : restartWaiters) {
               try {
                  LockSupport.unpark(waiter);
               }
               catch (Throwable t) {
                  // do nothing; continue notifying the rest
               }
            }
         }
      } else {
         // register interest in being notified after the restart
         restartWaiters.add(Thread.currentThread());
         // check again to ensure the restarting thread hasn't finished, then wait for that thread to finish
         if (restartInProgress.get()) LockSupport.park();
         // de-register interest in notification
         restartWaiters.remove(Thread.currentThread());
      }
   }
}
