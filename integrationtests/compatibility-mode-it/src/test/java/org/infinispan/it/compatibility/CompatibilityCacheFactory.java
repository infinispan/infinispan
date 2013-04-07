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

package org.infinispan.it.compatibility;

import org.infinispan.Cache;
import org.infinispan.api.BasicCache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;

import static org.infinispan.client.hotrod.TestHelper.startHotRodServer;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createLocalCacheManager;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
public class CompatibilityCacheFactory {

   private EmbeddedCacheManager cacheManager;
   private HotRodServer hotrod;
   private RemoteCacheManager hotrodClient;

   private Cache<Object, Object> embeddedCache;
   private RemoteCache<Object,Object> hotrodCache;

   void setup() {
      cacheManager = createLocalCacheManager(false);
      embeddedCache = cacheManager.getCache();
      hotrod = startHotRodServer(cacheManager);
      hotrodClient = new RemoteCacheManager(
            "localhost:" + hotrod.getPort(), true);
      hotrodCache = hotrodClient.getCache();
   }

   void teardown() {
      killRemoteCacheManager(hotrodClient);
      killServers(hotrod);
      killCacheManagers(cacheManager);
   }

   BasicCache<Object, Object> getEmbeddedCache() {
      return embeddedCache;
   }

   BasicCache<Object, Object> getHotRodCache() {
      return hotrodCache;
   }

}
