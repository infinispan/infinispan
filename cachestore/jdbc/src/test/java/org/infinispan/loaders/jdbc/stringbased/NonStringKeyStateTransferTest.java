/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.loaders.jdbc.stringbased;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static junit.framework.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "jdbc.stringbased.NonStringKeyStateTransferTest")
public class NonStringKeyStateTransferTest extends AbstractCacheTest {

   @Test (enabled = false, description = "Temporary disabled : https://issues.jboss.org/browse/ISPN-2249")
   public void testReplicatedStateTransfer() {
      EmbeddedCacheManager cm1 = null, cm2 = null;
      try {
         Configuration conf1 = NonStringKeyPreloadTest.createCacheStoreConfig(TwoWayPersonKey2StringMapper.class.getName(), false, true);
         conf1.setCacheMode(Configuration.CacheMode.REPL_SYNC);

         cm1 = TestCacheManagerFactory.createClusteredCacheManager(conf1);
         Cache<Person, String> c1 = cm1.getCache();
         Person mircea = new Person("markus", "mircea", 30);
         Person mircea2 = new Person("markus2", "mircea2", 30);

         c1.put(mircea, "mircea");
         c1.put(mircea2, "mircea2");

         Configuration conf2 = NonStringKeyPreloadTest.createCacheStoreConfig(TwoWayPersonKey2StringMapper.class.getName(), false, true);
         conf2.setCacheMode(Configuration.CacheMode.REPL_SYNC);

         cm2 = TestCacheManagerFactory.createClusteredCacheManager(conf2);
         Cache c2 = cm2.getCache();
         assertEquals("mircea", c2.get(mircea));
         assertEquals("mircea2", c2.get(mircea2));
         c2.get(mircea2);
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   public void testDistributedStateTransfer() {
      EmbeddedCacheManager cm1 = null, cm2 = null;
      try {
         Configuration conf = NonStringKeyPreloadTest.createCacheStoreConfig(TwoWayPersonKey2StringMapper.class.getName(), false, false);
         conf.setCacheMode(Configuration.CacheMode.DIST_SYNC);

         cm1 = TestCacheManagerFactory.createClusteredCacheManager(conf);
         Cache<Person, String> c1 = cm1.getCache();

         for (int i = 0; i < 100; i++) {
            Person mircea = new Person("markus" +i , "mircea"+i, 30);
            c1.put(mircea, "mircea"+i);
         }

         cm2 = TestCacheManagerFactory.createClusteredCacheManager(conf);
         Cache c2 = cm2.getCache();
         assert c2.size() > 0;
         for (Object key: c2.getAdvancedCache().getDataContainer().keySet()) {
            assert key instanceof Person: "expected key to be person but obtained " + key;
         }
         
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   public void testDistributedAndNoTwoWay() {
      EmbeddedCacheManager cm1;

      Configuration conf = NonStringKeyPreloadTest.createCacheStoreConfig(TwoWayPersonKey2StringMapper.class.getName(), false, false);
      conf.setCacheMode(Configuration.CacheMode.DIST_SYNC);

      cm1 = TestCacheManagerFactory.createClusteredCacheManager(conf);
      try {
         cm1.getCache();
      } finally {
         TestingUtil.killCacheManagers(cm1);
      }

   }
}
