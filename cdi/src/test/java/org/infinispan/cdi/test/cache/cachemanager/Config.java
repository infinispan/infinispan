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
package org.infinispan.cdi.test.cache.cachemanager;

import org.infinispan.cdi.Infinispan;
import org.infinispan.config.Configuration;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import static org.infinispan.eviction.EvictionStrategy.UNORDERED;

public class Config {

   @Large
   @Infinispan("large")
   @Produces
   public Configuration largeConfiguration() {
      Configuration largeConfiguration = new Configuration();
      largeConfiguration.fluent()
            .eviction()
            .strategy(UNORDERED);

      return largeConfiguration;
   }

   @Large
   @Produces
   @ApplicationScoped
   public EmbeddedCacheManager largeCacheManager() {
      Configuration defaultConfiguration = new Configuration();
      defaultConfiguration.fluent()
            .eviction()
            .maxEntries(200);

      return new DefaultCacheManager(defaultConfiguration);
   }
}
