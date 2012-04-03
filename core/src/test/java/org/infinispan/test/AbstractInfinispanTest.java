/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.test;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterTest;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import static org.testng.Assert.assertEquals;

/**
 * AbstractInfinispanTest is a superclass of all Infinispan tests.
 *
 * @author Vladimir Blagojevic
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class AbstractInfinispanTest {

   protected final Log log = LogFactory.getLog(getClass());

   private Set<Thread> spawnedThreads = new HashSet<Thread>();

   @AfterTest(alwaysRun = true)
   protected void killSpawnedThreads() {
      for (Thread t : spawnedThreads) {
         if (t.isAlive())
            t.interrupt();
      }
   }

   protected void eventually(Condition ec, long timeout) {
      int loops = 10;
      long sleepDuration = timeout / loops;
      try {
         for (int i = 0; i < loops; i++) {

            if (ec.isSatisfied()) break;
            Thread.sleep(sleepDuration);
         }
         assertEquals(true, ec.isSatisfied());
      } catch (Exception e) {
         throw new RuntimeException("Unexpected!", e);
      }
   }

   protected Thread fork(Runnable r, boolean waitForCompletion) {
      final String name = "ForkThread-" + getClass().getSimpleName() + "-" + r.hashCode();
      log.tracef("About to start thread '%s' as child of thread '%s'", name, Thread.currentThread().getName());
      final Thread t = new Thread(new RunnableWrapper(r), name);
      spawnedThreads.add(t);
      t.start();
      if (waitForCompletion) {
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return t.getState().equals(Thread.State.TERMINATED);
            }
         });
      }
      return t;
   }

   protected <T> Future<T> fork(Callable<T> c) {
      final String name = "ForkThread-" + getClass().getSimpleName() + "-" + c.hashCode();
      log.tracef("About to start thread '%s' as child of thread '%s'", name, Thread.currentThread().getName());
      FutureTask future = new FutureTask(c);
      final Thread t = new Thread(future);
      spawnedThreads.add(t);
      t.start();
      return future;
   }

   public final class RunnableWrapper implements Runnable {

      final Runnable realOne;

      public RunnableWrapper(Runnable realOne) {
         this.realOne = realOne;
      }

      @Override
      public void run() {
         try {
            log.trace("Started fork thread..");
            realOne.run();
         } catch (Throwable e) {
            log.trace("Exiting fork thread due to exception", e);
         } finally {
            log.trace("Exiting fork thread.");
         }
      }
   }


   protected void eventually(Condition ec) {
      eventually(ec, 10000);
   }

   protected interface Condition {
      public boolean isSatisfied() throws Exception;
   }
}
