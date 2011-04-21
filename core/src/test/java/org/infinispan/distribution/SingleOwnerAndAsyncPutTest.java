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
package org.infinispan.distribution;

import org.infinispan.affinity.KeyAffinityService;
import org.infinispan.affinity.KeyAffinityServiceFactory;
import org.infinispan.affinity.RndKeyGenerator;
import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.NotifyingFuture;
import org.testng.annotations.Test;

import java.util.concurrent.Executors;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "distribution.SingleOwnerAndAsyncPutTest")
public class SingleOwnerAndAsyncPutTest extends MultipleCacheManagersTest {
   private Object k0;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration c = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC);
      c.setNumOwners(1);
      c.setL1CacheEnabled(true);
      createCluster(c, 2);
      TestingUtil.blockUntilViewsReceived(10000, cache(0), cache(1));
      BaseDistFunctionalTest.RehashWaiter.waitForInitRehashToComplete(cache(0), cache(1));

      KeyAffinityService<Object> kaf =
            KeyAffinityServiceFactory.newKeyAffinityService(cache(0), Executors.newSingleThreadExecutor(),
                                                            new RndKeyGenerator(), 100, true);
      k0 = kaf.getKeyForAddress(address(0));
      kaf.stop();
   }

   public void testAsyncPut() throws Exception {
      cache(0).put(k0, "v0");
      NotifyingFuture<Object> oni = cache(0).putAsync(k0, "v1");
      assert oni != null;
      assertEquals("v0", oni.get());
   }
}
