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
package org.infinispan.configuration.cache;

import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;

/**
 * Controls the eviction settings for the cache.
 */
public class EvictionConfiguration {
   
   private final int maxEntries;
   private final EvictionStrategy strategy;
   private final EvictionThreadPolicy threadPolicy;
   
   EvictionConfiguration(int maxEntries, EvictionStrategy strategy, EvictionThreadPolicy threadPolicy) {
      this.maxEntries = maxEntries;
      this.strategy = strategy;
      this.threadPolicy = threadPolicy;
   }
   
   /**
    * Eviction strategy. Available options are 'UNORDERED', 'LRU', 'LIRS' and 'NONE' (to disable
    * eviction).
    *
    * @param strategy
    */
   public EvictionStrategy strategy() {
      return strategy;
   }
   
   /**
    * Threading policy for eviction.
    *
    * @param threadPolicy
    */
   public EvictionThreadPolicy threadPolicy() {
      return threadPolicy;
   }
   
   /**
    * Maximum number of entries in a cache instance. Cache size is guaranteed not to exceed upper
    * limit specified by max entries. However, due to the nature of eviction it is unlikely to ever
    * be exactly maximum number of entries specified here.
    * 
    * @param maxEntries
    */
   public int maxEntries() {
      return maxEntries;
   }

   @Override
   public String toString() {
      return "EvictionConfiguration{" +
            "maxEntries=" + maxEntries +
            ", strategy=" + strategy +
            ", threadPolicy=" + threadPolicy +
            '}';
   }

}
