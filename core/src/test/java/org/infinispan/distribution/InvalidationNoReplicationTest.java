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
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.concurrent.Executors;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "distribution.InvalidationNoReplicationTest")
public class InvalidationNoReplicationTest extends MultipleCacheManagersTest {

   protected Object k0;
   protected boolean transactional = true;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, transactional);
      config.setL1CacheEnabled(true);
      config.setNumOwners(1);
      createCluster(config, 2);
      TestingUtil.blockUntilViewsReceived(10000, cache(0), cache(1));
      KeyAffinityService<Object> service = KeyAffinityServiceFactory.
            newKeyAffinityService(cache(0), Executors.newSingleThreadExecutor(), new RndKeyGenerator(), 2, true);
      k0 = service.getKeyForAddress(address(0));
      service.stop();
   }

   public void testInvalidation() throws Exception {
      advancedCache(1).put(k0, "k1");
      assert advancedCache(1).getDataContainer().containsKey(k0);
      assert advancedCache(0).getDataContainer().containsKey(k0);

      tm(0).begin();
      cache(0).put(k0, "v2");
      tm(0).commit();

      assert !advancedCache(1).getDataContainer().containsKey(k0);
   }

}
