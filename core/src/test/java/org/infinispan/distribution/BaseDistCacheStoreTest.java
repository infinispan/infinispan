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
package org.infinispan.distribution;

import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;

/**
 * DistSyncCacheStoreTest.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public abstract class BaseDistCacheStoreTest extends BaseDistFunctionalTest {
   protected boolean shared;
   static int id;

   @Override
   protected EmbeddedCacheManager addClusterEnabledCacheManager(boolean withFD) {
      Configuration cfg = new Configuration();
      CacheLoaderManagerConfig clmc = new CacheLoaderManagerConfig();
      clmc.setShared(shared);
      int idToUse = shared ? 999 : id++;
      clmc.addCacheLoaderConfig(new DummyInMemoryCacheStore.Cfg(getClass().getSimpleName() + "_" + idToUse));
      cfg.setCacheLoaderManagerConfig(clmc);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(withFD, cfg);
      cacheManagers.add(cm);
      return cm;
   }
}
