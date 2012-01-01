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
package org.infinispan.distribution;

import org.infinispan.affinity.KeyAffinityService;
import org.infinispan.affinity.KeyAffinityServiceFactory;
import org.infinispan.affinity.RndKeyGenerator;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import java.util.concurrent.Executors;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "distribution.InvalidationFailureTest")
public class InvalidationFailureTest extends MultipleCacheManagersTest {
   private Object k0;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, true);
      config.setL1CacheEnabled(true);
      config.setNumOwners(1);
      createCluster(config, 2);
      manager(0).defineConfiguration("second", config);
      manager(1).defineConfiguration("second", config);
      manager(0).startCaches(CacheContainer.DEFAULT_CACHE_NAME, "second");
      manager(1).startCaches(CacheContainer.DEFAULT_CACHE_NAME, "second");
      waitForClusterToForm();
      waitForClusterToForm("second");
      cache(0).put("k","v");
      cache(0,"second").put("k","v");
      assert cache(1).get("k").equals("v");
      assert cache(1, "second").get("k").equals("v");

      KeyAffinityService<Object> service = KeyAffinityServiceFactory.newKeyAffinityService(cache(0),
                                                                                           Executors.newSingleThreadExecutor(),
                                                                                           new RndKeyGenerator(), 2, true);
      k0 = service.getKeyForAddress(address(0));
      service.stop();
   }

   public void testL1Invalidated() throws Exception {
      tm(1).begin();
      cache(1).put(k0,"v");
      cache(1, "second").put(k0,"v");
      assert !lockManager(1).isLocked(k0);
      assert !lockManager(1,"second").isLocked(k0);
      Transaction transaction = tm(1).suspend();

      tm(0).begin();
      log.info("Before the put");
      cache(0, "second").put(k0, "v1");
      cache(0).put(k0, "v2");
      try {
         tm(0).commit();
         log.info("After the Commit");
      } catch (Exception e) {
         e.printStackTrace();
         assert false : "this should not fail even if the invalidation does";
      } finally {
         tm(1).resume(transaction);
         tm(1).rollback();
         assert !lockManager(0).isLocked(k0);
         assert !lockManager(0, "second").isLocked(k0);
         assert !lockManager(1).isLocked(k0);
         assert !lockManager(1, "second").isLocked(k0);
      }
   }
}
