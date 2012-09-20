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
package org.infinispan.cdi.test.cache.embedded.specific;

import org.infinispan.cdi.ConfigureCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class Config {

   /**
    * Associates the "large" cache with the qualifier {@link Large}.
    *
    * @param cacheManager the specific cache manager associated to this cache. This cache manager is used to get the
    *                     default cache configuration.
    */
   @Large
   @ConfigureCache("large")
   @Produces
   @SuppressWarnings("unused")
   public Configuration largeConfiguration(@Large EmbeddedCacheManager cacheManager) {
      return new ConfigurationBuilder()
            .read(cacheManager.getDefaultCacheConfiguration())
            .eviction().maxEntries(2000)
            .build();
   }

   /**
    * Associates the "small" cache with the qualifier {@link Small}.
    *
    * @param cacheManager the specific cache manager associated to this cache. This cache manager is used to get the
    *                     default cache configuration.
    */
   @Small
   @ConfigureCache("small")
   @Produces
   @SuppressWarnings("unused")
   public Configuration smallConfiguration(@Small EmbeddedCacheManager cacheManager) {
      return new ConfigurationBuilder()
            .read(cacheManager.getDefaultCacheConfiguration())
            .eviction().maxEntries(20)
            .build();
   }

   /**
    * Associates the "small" and "large" caches with this specific cache manager.
    */
   @Large
   @Small
   @Produces
   @ApplicationScoped
   @SuppressWarnings("unused")
   public EmbeddedCacheManager specificCacheManager() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.eviction().maxEntries(4000).strategy(EvictionStrategy.LIRS);
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   /**
    * Stops cache manager.
    *
    * @param cacheManager to be stopped
    */
   @SuppressWarnings("unused")
   public void killCacheManager(@Disposes @Small @Large EmbeddedCacheManager cacheManager) {
      TestingUtil.killCacheManagers(cacheManager);
   }

}
