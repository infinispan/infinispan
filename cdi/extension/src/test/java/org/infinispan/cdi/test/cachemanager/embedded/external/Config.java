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
package org.infinispan.cdi.test.cachemanager.embedded.external;

import org.infinispan.cdi.ConfigureCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

/**
 * Creates a number of caches, based on some external mechanism.
 *
 * @author Pete Muir
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class Config {
   /**
    * Associates the externally defined "large" cache with the qualifier {@link Large}.
    */
   @Large
   @ConfigureCache("large")
   @Produces
   public Configuration largeConfiguration;

   /**
    * Associates the externally defined "quick" cache with the qualifier {@link Quick}.
    */
   @Quick
   @ConfigureCache("quick")
   @Produces
   public Configuration quickConfiguration;

   /**
    * Overrides the default embedded cache manager to define the quick and large cache configurations externally.
    */
   @Produces
   @ApplicationScoped
   public EmbeddedCacheManager defaultCacheManager() {
      EmbeddedCacheManager externalCacheContainerManager = new DefaultCacheManager();

      // define large configuration
      externalCacheContainerManager.defineConfiguration("large", new ConfigurationBuilder()
            .eviction().maxEntries(100)
            .build());

      // define quick configuration
      externalCacheContainerManager.defineConfiguration("quick", new ConfigurationBuilder()
            .expiration().wakeUpInterval(1l)
            .build());

      return externalCacheContainerManager;
   }
}
