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
package org.infinispan.tree;

import org.infinispan.Cache;
import org.infinispan.config.ConfigurationException;

/**
 * Factory class that contains API for users to create instances of {@link org.infinispan.tree.TreeCache}
 *
 * @author Navin Surtani
 */
public class TreeCacheFactory {

   /**
    * Creates a TreeCache instance by taking in a {@link org.infinispan.Cache} as a parameter
    *
    * @param cache
    * @return instance of a {@link TreeCache}
    * @throws NullPointerException   if the cache parameter is null
    * @throws ConfigurationException if the invocation batching configuration is not enabled.
    */

   public <K, V> TreeCache<K, V> createTreeCache(Cache<K, V> cache) {

      // Validation to make sure that the cache is not null.

      if (cache == null) {
         throw new NullPointerException("The cache parameter passed in is null");
      }

      // If invocationBatching is not enabled, throw a new configuration exception.
      if (!cache.getCacheConfiguration().invocationBatching().enabled()) {
         throw new ConfigurationException("invocationBatching is not enabled for cache '" +
               cache.getName() + "'. Make sure this is enabled by" +
               " calling configurationBuilder.invocationBatching().enable()");
      }

      return new TreeCacheImpl<K, V>(cache);
   }
}
