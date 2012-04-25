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
package org.infinispan.lucene.locking;

import java.io.IOException;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lucene.CacheTestSupport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * LockManagerFunctionalTest.
 * 
 * @author Sanne Grinovero
 * @since 4.0
 */
@SuppressWarnings("unchecked")
@Test(groups = "functional", testName = "lucene.locking.LockManagerFunctionalTest", enabled = true)
public class LockManagerFunctionalTest extends MultipleCacheManagersTest {
   
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configurationBuilder = CacheTestSupport.createTestConfiguration();
      createClusteredCaches(2, "lucene", configurationBuilder);
   }
   
   public void testLuceneIndexLocking() throws IOException {
      final String commonIndexName = "myIndex";
      LockFactory lockManagerA = makeLockFactory(cache(0,"lucene"), commonIndexName);
      LockFactory lockManagerB = makeLockFactory(cache(1, "lucene"), commonIndexName);
      LockFactory isolatedLockManager = makeLockFactory(cache(0, "lucene"), "anotherIndex");
      Lock luceneLockA = lockManagerA.makeLock(IndexWriter.WRITE_LOCK_NAME);
      Lock luceneLockB = lockManagerB.makeLock(IndexWriter.WRITE_LOCK_NAME);
      Lock anotherLock = isolatedLockManager.makeLock(IndexWriter.WRITE_LOCK_NAME);
      
      assert luceneLockA.obtain();
      assert luceneLockB.isLocked();
      assert ! anotherLock.isLocked();
      assert ! luceneLockA.obtain();
      assert ! luceneLockB.obtain();
      luceneLockA.release();
      assert ! luceneLockB.isLocked();
      assert luceneLockB.obtain();
      lockManagerA.clearLock(IndexWriter.WRITE_LOCK_NAME);
      assert ! luceneLockB.isLocked();
   }

   protected LockFactory makeLockFactory(Cache cache, String commonIndexName) {
      return new BaseLockFactory(cache, commonIndexName);
   }

}
