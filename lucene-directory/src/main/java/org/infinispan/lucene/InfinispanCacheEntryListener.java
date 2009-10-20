/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.infinispan.lucene;

import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.Event;

/**
 * Infinispan cache listener
 * 
 * @since 4.0
 * @author Lukasz Moren
 */
@Listener
public class InfinispanCacheEntryListener {

   private final String indexName;

   public InfinispanCacheEntryListener(String indexName) {
      this.indexName = indexName;
   }

   /**
    * Updates current Lucene file list in a cache - when cache entry is created or removed
    * 
    * @param event Infinispan event
    */
   @SuppressWarnings( { "unchecked" })
   @CacheEntryCreated
   @CacheEntryRemoved
   public void cacheEntryCreatedListener(CacheEntryEvent event) {
      Object ob = event.getKey();
      // process only local events
      if (ob instanceof FileCacheKey && event.isOriginLocal()) {
         FileCacheKey key = (FileCacheKey) ob;

         if (key.getIndexName().equals(indexName) && !key.isLockKey()) {
            FileListCacheKey fileListKey = new FileListCacheKey(key.getIndexName());
            Cache<CacheKey, Object> cache = event.getCache();
            Map<String, String> fileList = (Map<String, String>) cache.get(fileListKey);

            if (event.getType().equals(Event.Type.CACHE_ENTRY_CREATED)) {
               fileList.put(key.getFileName(), key.getFileName());
            } else if (event.getType().equals(Event.Type.CACHE_ENTRY_REMOVED)) {
               fileList.remove(key.getFileName());
            }

            cache.put(fileListKey, fileList);
         }
      }
   }

}
