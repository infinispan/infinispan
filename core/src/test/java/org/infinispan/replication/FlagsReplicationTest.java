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
package org.infinispan.replication;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.AdvancedCache;
import org.infinispan.context.Flag;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Verifies the Flags affect both local and remote nodes.
 * 
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @since 4.2.1
 */
@Test(groups = "functional", testName = FlagsReplicationTest.TEST_NAME)
public class FlagsReplicationTest extends BaseDistFunctionalTest {
   
   static final String TEST_NAME = "replication.FlagsReplicationTest";
   static final String DATA_PROVIDER = TEST_NAME + ".dataprovider";
   private ExecutorService threadPool;
   
   private final Integer one = 1;
   private final String key = TEST_NAME;
   
   public FlagsReplicationTest() {
      tx = true;
      cacheName = TEST_NAME;
      cleanup = CleanupPhase.AFTER_METHOD;
      lockingMode = LockingMode.PESSIMISTIC;
      lockTimeout = 1;
   }
   
   @DataProvider(name = DATA_PROVIDER)
   public Object[][] createTestConfigurations() {
      return new Object[][] {
               { true,  true  },
               { false, false },
               { false, true  },
               { true,  false },
         };
   }

   @Test(dataProvider = DATA_PROVIDER)
   public void testScenario(boolean cache1IsOwner, boolean cache2IsOwner) throws Throwable {
      log.tracef("Start cache1IsOwner = %s, cache2IsOwner %s", cache1IsOwner, cache2IsOwner);
      AdvancedCache cache1 = (cache1IsOwner ? getFirstOwner(key) : getFirstNonOwner(key)).getAdvancedCache();
      AdvancedCache cache2 = (cache2IsOwner ? getFirstOwner(key) : getFirstNonOwner(key)).getAdvancedCache();

      assert null == cache1.put(key, one);

      haveSecondaryThreadTakeLock(cache2);

      cache1.getTransactionManager().begin();
      boolean locked = cache1.withFlags(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT, Flag.FAIL_SILENTLY).lock(key);
      assert !locked;
      Object removed = cache1.withFlags(Flag.SKIP_LOCKING).remove(key);
      assert one.equals(removed);

      haveSecondaryThreadReleaseLock(cache2);
      cache1.getTransactionManager().commit();
      assert null == cache2.get(key);
      log.tracef("End cache1IsOwner = %s, cache2IsOwner %s", cache1IsOwner, cache2IsOwner);
   }

   private void haveSecondaryThreadTakeLock(final AdvancedCache viaCache) throws InterruptedException, ExecutionException {
      AtomicBoolean noerrors = new AtomicBoolean(true);
      Future<?> submit = threadPool.submit(new LockingThread(viaCache, noerrors));
      submit.get(); //wait to be done
      assert noerrors.get();
   }
   
   private void haveSecondaryThreadReleaseLock(final AdvancedCache viaCache) throws InterruptedException, ExecutionException {
      AtomicBoolean noerrors = new AtomicBoolean(true);
      Future<?> submit = threadPool.submit(new CommitThread(viaCache, noerrors));
      submit.get(); //wait to be done
      assert noerrors.get();
   }
   
   private class LockingThread implements Runnable {
      
      private final AdvancedCache cache;
      private final AtomicBoolean allok;

      LockingThread(AdvancedCache cache, AtomicBoolean allok) {
         this.cache = cache;
         this.allok = allok;
      }

      @Override
      public void run() {
         try {
            log.trace("About to try to acquire a lock.");
            cache.getTransactionManager().begin();
            if (! cache.lock(key)) {
               allok.set(false);
               log.trace("Could not acquire lock");
            }
         } catch (Throwable e) {
            log.trace("Error", e);
            allok.set(false);
         }
      }
   }
   
   static private class CommitThread implements Runnable {
      
      private final AdvancedCache cache;
      private final AtomicBoolean allok;

      CommitThread(AdvancedCache cache, AtomicBoolean allok) {
         this.cache = cache;
         this.allok = allok;
      }

      @Override
      public void run() {
         try {
            cache.getTransactionManager().commit();
         } catch (Throwable e) {
            allok.set(false);
         }
      }
   }
   
   @BeforeClass
   protected void startThreadPool() {
      threadPool = Executors.newFixedThreadPool(1);
   }
   
   @AfterClass
   protected void stopThreadPool() {
      threadPool.shutdownNow();
   }

}
