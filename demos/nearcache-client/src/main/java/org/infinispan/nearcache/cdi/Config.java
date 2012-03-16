/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.nearcache.cdi;

import org.infinispan.cdi.ConfigureCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.nearcache.jms.RemoteEventCacheStore;

import javax.enterprise.inject.Produces;

/**
 * Configuration of the cache
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class Config {

   @AddressCache
   @ConfigureCache("address-cache")
   @Produces
   public Configuration addressCache() {
      return new ConfigurationBuilder()
         .eviction().strategy(EvictionStrategy.LRU).maxEntries(4)
         .loaders().shared(true).addCacheLoader().cacheLoader(new RemoteEventCacheStore())
         .build();
   }

}
