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

package org.infinispan.jcache;

import javax.cache.Cache;
import javax.cache.CacheMXBean;
import javax.cache.Status;

/**
 * Class to help implementers
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values*
 * @author Yannis Cosmadopoulos
 * @since 1.0
 */
public class RIDelegatingCacheMXBean<K, V> implements CacheMXBean {

   private final Cache<K, V> cache;

   /**
    * Constructor
    * @param cache the cache
    */
   public RIDelegatingCacheMXBean(Cache<K, V> cache) {
      this.cache = cache;
   }

   @Override
   public String getName() {
      return cache.getName();
   }

   @Override
   public Status getStatus() {
      return cache.getStatus();
   }

   private String mbeanSafe(String string) {
      return string == null ? "" : string.replaceAll(":|=|\n", ".");
   }

}
