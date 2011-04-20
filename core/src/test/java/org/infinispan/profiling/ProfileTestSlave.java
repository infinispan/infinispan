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
package org.infinispan.profiling;

import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "profiling", enabled = false, testName = "profiling.ProfileTestSlave")
public class ProfileTestSlave extends AbstractProfileTest {

   public static void main(String[] args) throws Exception {
      ProfileTestSlave pst = new ProfileTestSlave();
      pst.startedInCmdLine = true;

      String mode = args[0];
      try {
         pst.testWith(mode);
      } finally {
         pst.destroyAfterMethod();
         pst.destroyAfterClass();
      }
   }

   public void testReplSync() throws Exception {
      testWith(REPL_SYNC_CACHE_NAME);
   }

   public void testReplAsync() throws Exception {
      testWith(REPL_ASYNC_CACHE_NAME);
   }

   public void testDistSync() throws Exception {
      testWith(DIST_SYNC_CACHE_NAME);
   }

   public void testDistAsync() throws Exception {
      testWith(DIST_ASYNC_CACHE_NAME);
   }

   public void testDistSyncL1() throws Exception {
      testWith(DIST_SYNC_L1_CACHE_NAME);
   }

   public void testDistAsyncL1() throws Exception {
      testWith(DIST_ASYNC_L1_CACHE_NAME);
   }

   private void waitForTest() throws Exception {
      Thread t = new Thread("CompletionThread") {
         @Override
         public void run() {
            try {
                while (true) Thread.sleep(10000);
            } catch (Exception e) {
            }
         }
      };

      // attach a view change listener
      cacheManager.addListener(new ShutdownHook(t));
      t.setDaemon(true);
      t.start();
      try {
         t.join();
      } catch (InterruptedException ie) {
         // move on...
      }
   }

   private void doTest() {
      // trigger for JProfiler
   }

   protected void testWith(String cachename) throws Exception {
      log.warnf("Starting slave, cache name = %s", cachename);
      initTest();
      cache = cacheManager.getCache(cachename);
      System.out.println("Waiting for members to join.");
      TestingUtil.blockUntilViewReceived(cache, 2, 120000, true);
      System.out.println("Cluster ready, cache mode is " + cache.getConfiguration().getCacheMode());
      System.out.println("Waiting for test completion.  Hit CTRL-C when done.");
      doTest();
      waitForTest();
   }

   @Listener
   public static final class ShutdownHook {
      final Thread completionThread;

      public ShutdownHook(Thread completionThread) {
         this.completionThread = completionThread;
      }

      @ViewChanged
      public void viewChanged(ViewChangedEvent vce) {
         System.out.println("Saw view change event " + vce);
         // if the new view ONLY contains me, die!
         if (vce.getOldMembers().size() > vce.getNewMembers().size() && vce.getNewMembers().size() == 1 && vce.getNewMembers().contains(vce.getLocalAddress())) {
            completionThread.interrupt();
         }
      }
   }
}
