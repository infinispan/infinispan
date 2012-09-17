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
package org.infinispan.loaders.file;

import org.infinispan.loaders.AbstractCacheStoreConfig;

/**
 * Configures {@link FastFileCacheStore}.
 * <p/>
 * <ul>
 * <li><tt>location</tt> - a location on disk where the store can write internal files. This
 * defaults to <tt>Infinispan-FileCacheStore</tt> in the current working directory.</li>
 * <li><tt>maxEntries</tt> - maximum number of entries allowed in the cache store. If more entries
 * are added, the least recently used (LRU) entry is removed.</li>
 * </ul>
 * 
 * @author Karsten Blees
 */
public class FastFileCacheStoreConfig extends AbstractCacheStoreConfig {
   private static final long serialVersionUID = 1L;

   private String location = "Infinispan-FileCacheStore";

   private int maxEntries = -1;

   public FastFileCacheStoreConfig() {
      setCacheLoaderClassName(FastFileCacheStore.class.getName());
   }

   public String getLocation() {
      return location;
   }

   public void setLocation(String location) {
      testImmutability("location");
      this.location = location;
   }

   public FastFileCacheStoreConfig location(String location) {
      setLocation(location);
      return this;
   }

   public int getMaxEntries() {
      return maxEntries;
   }

   public void setMaxEntries(int maxEntries) {
      testImmutability("maxEntries");
      this.maxEntries = maxEntries;
   }

   public FastFileCacheStoreConfig maxEntries(int maxEntries) {
      setMaxEntries(maxEntries);
      return this;
   }
}
