/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.loaders.remote;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import net.jcip.annotations.ThreadSafe;

import org.infinispan.Cache;
import org.infinispan.api.BasicCacheContainer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.AbstractCacheStore;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderMetadata;
import org.infinispan.loaders.remote.logging.Log;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.util.logging.LogFactory;

/**
 * Cache store that delegates the call to a infinispan cluster. Communication between this cache store and the remote
 * cluster is achieved through the java HotRod client: this assures fault tolerance and smart dispatching of calls to
 * the nodes that have the highest chance of containing the given key. This cache store supports both preloading
 * and <b>fetchPersistentState</b>.
 * <p/>
 * Purging elements is not possible, as HotRod does not support the fetching of all remote keys (this would be a
 * very costly operation as well). Purging takes place at the remote end (infinispan cluster).
 * <p/>
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.loaders.remote.RemoteCacheStoreConfig
 * @see <a href="http://community.jboss.org/wiki/JavaHotRodclient">Hotrod Java Client</a>
 * @since 4.1
 */
@ThreadSafe
@CacheLoaderMetadata(configurationClass = RemoteCacheStoreConfig.class)
public class RemoteCacheStore extends AbstractCacheStore {

   private static final Log log = LogFactory.getLog(RemoteCacheStore.class, Log.class);

   private volatile RemoteCacheStoreConfig config;
   private volatile RemoteCacheManager remoteCacheManager;
   private volatile RemoteCache<Object, Object> remoteCache;
   private static final String LIFESPAN = "lifespan";
   private static final String MAXIDLE = "maxidle";

   @Override
   public InternalCacheEntry load(Object key) throws CacheLoaderException {
      return (InternalCacheEntry) remoteCache.get(key);
   }

   @Override
   protected void purgeInternal() throws CacheLoaderException {
      if (log.isTraceEnabled()) {
         log.trace("Skipping purge call, as this is performed on the remote cache.");
      }
   }

   @Override
   public boolean containsKey(Object key) throws CacheLoaderException {
      return remoteCache.containsKey(key);
   }

   @Override
   public void store(InternalCacheEntry entry) throws CacheLoaderException {
      if (log.isTraceEnabled()) {
         log.tracef("Adding entry: %s", entry);
      }
      remoteCache.put(entry.getKey(), entry, toSeconds(entry.getLifespan(), entry, LIFESPAN), TimeUnit.SECONDS, toSeconds(entry.getMaxIdle(), entry, MAXIDLE), TimeUnit.SECONDS);
   }

   @Override
   @SuppressWarnings("unchecked")
   public void fromStream(ObjectInput inputStream) throws CacheLoaderException {
      Map result;
      try {
         result = (Map<Object, InternalCacheEntry>) marshaller.objectFromObjectStream(inputStream);
         remoteCache.putAll(result);
      } catch (Exception e) {
         throw new CacheLoaderException("Exception while reading data", e);
      }
   }

   @Override
   public void toStream(ObjectOutput outputStream) throws CacheLoaderException {
      Map map = remoteCache.getBulk();
      try {
         marshaller.objectToObjectStream(map, outputStream);
      } catch (IOException e) {
         throw new CacheLoaderException("Exception while serializing remote data to stream", e);
      }
   }

   @Override
   public void clear() throws CacheLoaderException {
      remoteCache.clear();
   }

   @Override
   public boolean remove(Object key) throws CacheLoaderException {
      return remoteCache.remove(key) != null;
   }

   @Override
   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      Map<Object, Object> map = remoteCache.getBulk();
      return convertToInternalCacheEntries(map);
   }

   @Override
   public Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException {
      return convertToInternalCacheEntries(remoteCache.getBulk(numEntries));
   }

   @Override
   public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
      log.sharedModeOnlyAllowed();
      throw new CacheLoaderException("RemoteCacheStore can only run in shared mode! This method shouldn't be called in shared mode");
   }

   @Override
   public void init(CacheLoaderConfig config, Cache<?, ?> cache, StreamingMarshaller m) throws CacheLoaderException {
      super.init(config, cache, m);
      this.config = (RemoteCacheStoreConfig) config;
   }

   @Override
   public void start() throws CacheLoaderException {
      super.start();
      StreamingMarshaller marshaller = getMarshaller();

      if (marshaller == null) {throw new IllegalStateException("Null marshaller not allowed!");}
      remoteCacheManager = new RemoteCacheManager(marshaller, config.getHotRodClientProperties(), config.getAsyncExecutorFactory());
      if (config.getRemoteCacheName().equals(BasicCacheContainer.DEFAULT_CACHE_NAME))
         remoteCache = remoteCacheManager.getCache();
      else
         remoteCache = remoteCacheManager.getCache(config.getRemoteCacheName());
   }

   @Override
   public void stop() throws CacheLoaderException {
      remoteCacheManager.stop();
   }

   @Override
   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return RemoteCacheStoreConfig.class;
   }

   private long toSeconds(long millis, InternalCacheEntry entry, String desc) {
      if (millis > 0 && millis < 1000) {
         if (log.isTraceEnabled()) {
            log.tracef("Adjusting %s time for (k,v): (%s, %s) from %d millis to 1 sec, as milliseconds are not supported by HotRod",
                       desc ,entry.getKey(), entry.getValue(), millis);
         }
         return 1;
      }
      return TimeUnit.MILLISECONDS.toSeconds(millis);
   }

   private Set<InternalCacheEntry> convertToInternalCacheEntries(Map<Object, Object> map) {
      Set<InternalCacheEntry> result = new HashSet<InternalCacheEntry>(map.size());
      Set<Map.Entry<Object, Object>> set = map.entrySet();
      for (Map.Entry<Object, Object> e : set) {
         result.add((InternalCacheEntry) e.getValue());
      }
      return result;
   }
}
