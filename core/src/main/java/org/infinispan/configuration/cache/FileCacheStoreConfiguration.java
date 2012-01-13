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

import org.infinispan.configuration.cache.FileCacheStoreConfigurationBuilder.FsyncMode;
import org.infinispan.util.TypedProperties;

/**
 * File cache store configuration.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class FileCacheStoreConfiguration extends AbstractLockSupportCacheStoreConfiguration {

   private final String location;
   private final long fsyncInterval;
   private final FsyncMode fsyncMode;
   private final int streamBufferSize;

   FileCacheStoreConfiguration(String location, long fsyncInterval,
         FsyncMode fsyncMode, int streamBufferSize, long lockAcquistionTimeout,
         int lockConcurrencyLevel, boolean purgeOnStartup, boolean purgeSynchronously,
         int purgerThreads, boolean fetchPersistentState, boolean ignoreModifications,
         TypedProperties properties, AsyncLoaderConfiguration async,
         SingletonStoreConfiguration singletonStore) {
      super(lockAcquistionTimeout, lockConcurrencyLevel, purgeOnStartup,
            purgeSynchronously, purgerThreads, fetchPersistentState,
            ignoreModifications, properties, async, singletonStore);
      this.location = location;
      this.fsyncInterval = fsyncInterval;
      this.fsyncMode = fsyncMode;
      this.streamBufferSize = streamBufferSize;
   }

   public long fsyncInterval() {
      return fsyncInterval;
   }

   public FsyncMode fsyncMode() {
      return fsyncMode;
   }

   public String location() {
      return location;
   }

   public int streamBufferSize() {
      return streamBufferSize;
   }

}
