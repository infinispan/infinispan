/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Tests verifying that the instantiation of TransactionalLockFactory in case of NON_TRANSACTIONAL cache fails.
 * The cases with started & stopped caches are checked.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "lucene.locking.TransactionalLockManagerExcFunctionalTest", enabled = true)
public class TransactionalLockManagerExcFunctionalTest extends TransactionalLockManagerFunctionalTest {

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = "Failed looking up TransactionManager: the cache is not running")
   public void testLuceneIndexLockingWithStoppedCache() throws IOException {
      final String commonIndexName = "myIndex";

      Cache cache1 = cache(0, "lucene");

      cache(0, "lucene").stop();
      cache(1, "lucene").stop();
      TestingUtil.killCacheManagers(cacheManagers);

      makeLockFactory(cache1, commonIndexName);
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = "Failed looking up TransactionManager. Check if any transaction manager is associated with Infinispan cache: 'lucene'")
   public void testLuceneIndexLockingWithCache() throws IOException {
      final String commonIndexName = "myIndex";

      Cache cache1 = cache(0, "lucene");
      makeLockFactory(cache1, commonIndexName);
   }

   @Test(dataProvider = "writeLockNameProvider", enabled = false) @Override
   public void testLuceneIndexLocking(final String writeLockProvider) throws IOException {
      //do nothing
   }

   @Override
   protected TransactionMode getTransactionsMode() {
      return TransactionMode.NON_TRANSACTIONAL;
   }
}
