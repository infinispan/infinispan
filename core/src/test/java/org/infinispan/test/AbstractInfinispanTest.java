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

import org.testng.annotations.AfterTest;

import java.util.HashSet;
import java.util.Set;

import static org.testng.Assert.assertEquals;

/**
 * AbstractInfinispanTest is a superclass of all Infinispan tests.
 *
 * @author Vladimir Blagojevic
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class AbstractInfinispanTest {

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

   protected Thread fork(Runnable r, boolean sync) {
      final Thread t = new Thread(r, "TestThread-" + r.hashCode());
      spawnedThreads.add(t);
      t.start();
      if (sync) {
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return t.getState().equals(Thread.State.TERMINATED);
            }
         });
      }
      return t;
   }


   protected void eventually(Condition ec) {
      eventually(ec, 10000);
   }

   protected interface Condition {
      public boolean isSatisfied() throws Exception;
   }
}
