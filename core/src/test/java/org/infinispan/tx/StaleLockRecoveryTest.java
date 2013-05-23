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
package org.infinispan.tx;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.RemoteException;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import java.util.List;

/**
 * Tests what happens when a member acquires locks and then dies.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Test(groups = "functional", testName = "tx.StaleLockRecoveryTest")
public class StaleLockRecoveryTest extends MultipleCacheManagersTest {
   private Cache<String, String> c1, c2;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      c.transaction().lockingMode(LockingMode.PESSIMISTIC)
            .locking().lockAcquisitionTimeout(500);
      List<Cache<String, String>> caches = createClusteredCaches(2, "tx", c);
      c1 = caches.get(0);
      c2 = caches.get(1);
   }

   public void testStaleLock() throws SystemException, NotSupportedException {
      c1.put("k", "v");
      assert c1.get("k").equals("v");
      assert c2.get("k").equals("v");

      TransactionManager tm = TestingUtil.getTransactionManager(c1);
      tm.begin();
      c1.getAdvancedCache().lock("k");
      tm.suspend();

      // test that both c1 and c2 have locked k
      assertLocked(c1, "k");
      assertLocked(c2, "k");

      cacheManagers.get(0).stop();
      TestingUtil.blockUntilViewReceived(c2, 1);

      EmbeddedCacheManager cacheManager = c2.getCacheManager();
      assert cacheManager.getMembers().size() == 1;

      // may take a while from when the view change is seen through to when the lock is cleared
      TestingUtil.sleepThread(1000);

      assertNotLocked(c2, "k");
   }

   private void assertLocked(Cache<String, String> c, String key) throws SystemException, NotSupportedException {
      TransactionManager tm = TestingUtil.getTransactionManager(c);
      tm.begin();
      try {
         c.put(key, "dummy"); // should time out
         assert false : "Should have been locked!";
      } catch (TimeoutException e) {
         // ignoring timeout exception
      } catch (RemoteException e) {
         assert e.getCause() instanceof TimeoutException;
         // ignoring timeout exception
      } finally {
         tm.rollback();
      }
   }

   private void assertNotLocked(Cache<String, String> c, String key) throws SystemException, NotSupportedException {
      TransactionManager tm = TestingUtil.getTransactionManager(c);
      tm.begin();
      try {
         c.put(key, "dummy"); // should time out
      } catch (TimeoutException e) {
         assert false : "Should not have been locked!";
      } finally {
         tm.rollback();
      }
   }
}
