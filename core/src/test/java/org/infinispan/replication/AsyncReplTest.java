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

/*
 *
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.infinispan.replication;

import org.infinispan.Cache;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "functional", testName = "replication.AsyncReplTest")
public class AsyncReplTest extends MultipleCacheManagersTest {

   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder asyncConfiguration = getDefaultClusteredCacheConfig(CacheMode.REPL_ASYNC, true);
      createClusteredCaches(2, "asyncRepl", asyncConfiguration);   
   }

   public void testWithNoTx() throws Exception {

      Cache cache1 = cache(0,"asyncRepl");
      Cache cache2 = cache(1,"asyncRepl");
      String key = "key";

      replListener(cache2).expect(PutKeyValueCommand.class);
      cache1.put(key, "value1");
      // allow for replication
      replListener(cache2).waitForRpc();
      assertEquals("value1", cache1.get(key));
      assertEquals("value1", cache2.get(key));

      replListener(cache2).expect(PutKeyValueCommand.class);
      cache1.put(key, "value2");
      assertEquals("value2", cache1.get(key));

      replListener(cache2).waitForRpc();

      assertEquals("value2", cache1.get(key));
      assertEquals("value2", cache2.get(key));
   }

   public void testWithTx() throws Exception {
      Cache cache1 = cache(0,"asyncRepl");
      Cache cache2 = cache(1,"asyncRepl");
      
      String key = "key";
      replListener(cache2).expect(PutKeyValueCommand.class);
      cache1.put(key, "value1");
      // allow for replication
      replListener(cache2).waitForRpc();
      assertNotLocked(cache1, key);

      assertEquals("value1", cache1.get(key));
      assertEquals("value1", cache2.get(key));

      TransactionManager mgr = TestingUtil.getTransactionManager(cache1);
      mgr.begin();
      replListener(cache2).expectWithTx(PutKeyValueCommand.class);
      cache1.put(key, "value2");
      assertEquals("value2", cache1.get(key));
      assertEquals("value1", cache2.get(key));
      mgr.commit();
      replListener(cache2).waitForRpc();
      assertNotLocked(cache1, key);

      assertEquals("value2", cache1.get(key));
      assertEquals("value2", cache2.get(key));

      mgr.begin();
      cache1.put(key, "value3");
      assertEquals("value3", cache1.get(key));
      assertEquals("value2", cache2.get(key));

      mgr.rollback();

      assertEquals("value2", cache1.get(key));
      assertEquals("value2", cache2.get(key));

      assertNotLocked(cache1, key);

   }

   public void simpleTest() throws Exception {
      Cache cache1 = cache(0,"asyncRepl");
      Cache cache2 = cache(1,"asyncRepl");
      
      String key = "key";
      TransactionManager mgr = TestingUtil.getTransactionManager(cache1);

      mgr.begin();
      cache1.put(key, "value3");
      mgr.rollback();

      assertNotLocked(cache1, key);

   }
}
