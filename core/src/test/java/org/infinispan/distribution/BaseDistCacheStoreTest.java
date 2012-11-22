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
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStoreConfigurationBuilder;

/**
 * DistSyncCacheStoreTest.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public abstract class BaseDistCacheStoreTest extends BaseDistFunctionalTest {
   protected boolean shared;
   protected boolean preload;

   @Override
   protected ConfigurationBuilder buildConfiguration() {
      ConfigurationBuilder cfg = super.buildConfiguration();
      cfg.loaders().shared(shared);
      if (shared) {
         cfg.loaders().addStore(new DummyInMemoryCacheStoreConfigurationBuilder(cfg.loaders())
               .storeName(getClass().getSimpleName()));
      } else {
         cfg.loaders().addStore(new DummyInMemoryCacheStoreConfigurationBuilder(cfg.loaders()));
      }
      cfg.loaders().preload(preload);
      return cfg;
   }
}
