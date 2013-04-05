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

package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

/**
 * Tests behaviour of Hot Rod clients with asymmetric clusters.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(groups = "functional", testName = "client.hotrod.ClientAsymmetricClusterTest")
public class ClientAsymmetricClusterTest extends MultiHotRodServersTest {

   private static final String CACHE_NAME = "asymmetricCache";

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false));

      createHotRodServers(2, builder);

      // Define replicated cache in only one of the nodes
      manager(0).defineConfiguration(CACHE_NAME, builder.build());
   }

   @Test(expectedExceptions = HotRodClientException.class,
         expectedExceptionsMessageRegExp = ".*CacheNotFoundException.*")
   public void testAsymmetricCluster() {
      RemoteCacheManager client0 = client(0);
      RemoteCache<Object, Object> cache0 = client0.getCache(CACHE_NAME);
      cache0.put(1, "v1");
      assertEquals("v1", cache0.get(1));
      cache0.put(2, "v1");
      assertEquals("v1", cache0.get(2));
      cache0.put(3, "v1");
      assertEquals("v1", cache0.get(3));
   }

}
