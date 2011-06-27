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
package org.infinispan.cdi.test.cachemanager.external;

import org.infinispan.cdi.CacheContainerManager;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

import javax.enterprise.inject.Specializes;

@Specializes
public class ExternalCacheContainerManager extends CacheContainerManager {

   private static final CacheContainer CACHE_CONTAINER;

   static {
      EmbeddedCacheManager cacheManager = new DefaultCacheManager();

      // define large configuration
      Configuration largeConfiguration = new Configuration();
      largeConfiguration.fluent()
            .eviction()
            .maxEntries(100);

      cacheManager.defineConfiguration("large", largeConfiguration);

      // define quick configuration
      Configuration quickConfiguration = new Configuration();
      quickConfiguration.fluent()
            .eviction()
            .wakeUpInterval(1l);

      cacheManager.defineConfiguration("quick", quickConfiguration);

      CACHE_CONTAINER = cacheManager;
   }

   public ExternalCacheContainerManager() {
      super(CACHE_CONTAINER);
   }
}
