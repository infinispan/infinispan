/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.infinispan.lucene.locking;

import java.io.IOException;

import org.infinispan.config.Configuration;
import org.infinispan.lucene.CacheTestSupport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * LockManagerFunctionalTest.
 * 
 * @author Sanne Grinovero
 * @since 4.0
 */
@Test(groups = "functional", testName = "lucene.locking.LockManagerFunctionalTest", enabled = true)
public class LockManagerFunctionalTest extends MultipleCacheManagersTest {
   
   protected void createCacheManagers() throws Throwable {
      Configuration replSync = CacheTestSupport.createTestConfiguration();
      createClusteredCaches(2, "lucene", replSync);
   }
   
   @SuppressWarnings("unchecked")
   public void testLuceneIndexLocking() throws IOException {
      final String commonIndexName = "myIndex";
      TransactionalLockFactory lockManagerA = new TransactionalLockFactory(cache(0, "lucene"), commonIndexName);
      TransactionalLockFactory lockManagerB = new TransactionalLockFactory(cache(1, "lucene"), commonIndexName);
      TransactionalLockFactory isolatedLockManager = new TransactionalLockFactory(cache(0, "lucene"), "anotherIndex");
      TransactionalSharedLuceneLock luceneLockA = lockManagerA.makeLock(TransactionalLockFactory.DEF_LOCK_NAME);
      TransactionalSharedLuceneLock luceneLockB = lockManagerB.makeLock(TransactionalLockFactory.DEF_LOCK_NAME);
      TransactionalSharedLuceneLock anotherLock = isolatedLockManager.makeLock(TransactionalLockFactory.DEF_LOCK_NAME);
      
      assert luceneLockA.obtain();
      assert luceneLockB.isLocked();
      assert ! anotherLock.isLocked();
      assert ! luceneLockA.obtain();
      assert ! luceneLockB.obtain();
      luceneLockA.release();
      assert ! luceneLockB.isLocked();
      assert luceneLockB.obtain();
      lockManagerA.clearLock(TransactionalLockFactory.DEF_LOCK_NAME);
      assert ! luceneLockB.isLocked();
   }

}
