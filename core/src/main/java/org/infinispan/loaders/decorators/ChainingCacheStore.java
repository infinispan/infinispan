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
package org.infinispan.loaders.decorators;

import net.jcip.annotations.GuardedBy;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.configuration.cache.LoaderConfiguration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoader;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A chaining cache loader that allows us to configure > 1 cache loader.
 * <p/>
 * READ operations are directed to each of the cache loaders (in the order which they were configured) until a non-null
 * (or non-empty in the case of retrieving collection objects) result is achieved.
 * <p/>
 * WRITE operations are propagated to ALL registered cache stores specified, except those that set ignoreModifications
 * to false.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ChainingCacheStore implements CacheStore {
   private static final Log log = LogFactory.getLog(ChainingCacheStore.class);
   private final ReadWriteLock loadersAndStoresMutex = new ReentrantReadWriteLock();
   @GuardedBy("loadersAndStoresMutex")
   private final Map<CacheLoader, LoaderConfiguration> loaders = new LinkedHashMap<CacheLoader, LoaderConfiguration>();
   @GuardedBy("loadersAndStoresMutex")
   private final Map<CacheStore, StoreConfiguration> stores = new LinkedHashMap<CacheStore, StoreConfiguration>();

   @Override
   public void store(InternalCacheEntry ed) throws CacheLoaderException {
      loadersAndStoresMutex.readLock().lock();
      try {
         for (CacheStore s : stores.keySet()) s.store(ed);
      } finally {
         loadersAndStoresMutex.readLock().unlock();
      }
   }

   @Override
   public void fromStream(ObjectInput inputStream) throws CacheLoaderException {
      loadersAndStoresMutex.readLock().lock();
      try {
         // loading and storing state via streams is *only* supported on the *first* store that has fetchPersistentState set.
         for (Map.Entry<CacheStore, StoreConfiguration> e : stores.entrySet()) {
            if (e.getValue().fetchPersistentState()) {
               e.getKey().fromStream(inputStream);
               // do NOT continue this for other stores, since the stream will not be in an appropriate state anymore
               break;
            }
         }
      } finally {
         loadersAndStoresMutex.readLock().unlock();
      }

   }

   @Override
   public void toStream(ObjectOutput outputStream) throws CacheLoaderException {
      loadersAndStoresMutex.readLock().lock();
      try {
         // loading and storing state via streams is *only* supported on the *first* store that has fetchPersistentState set.
         for (Map.Entry<CacheStore, StoreConfiguration> e : stores.entrySet()) {
            if (e.getValue().fetchPersistentState()) {
               e.getKey().toStream(outputStream);
               // do NOT continue this for other stores, since the stream will not be in an appropriate state anymore
               break;
            }
         }
      } finally {
         loadersAndStoresMutex.readLock().unlock();
      }
   }

   @Override
   public void clear() throws CacheLoaderException {
      loadersAndStoresMutex.readLock().lock();
      try {
         for (CacheStore s : stores.keySet()) s.clear();
      } finally {
         loadersAndStoresMutex.readLock().unlock();
      }
   }

   @Override
   public boolean remove(Object key) throws CacheLoaderException {
      boolean r = false;
      loadersAndStoresMutex.readLock().lock();
      try {
         for (CacheStore s : stores.keySet()) r = s.remove(key) || r;
      } finally {
         loadersAndStoresMutex.readLock().unlock();
      }
      return r;
   }

   @Override
   public void removeAll(Set<Object> keys) throws CacheLoaderException {
      loadersAndStoresMutex.readLock().lock();
      try {
         for (CacheStore s : stores.keySet()) s.removeAll(keys);
      } finally {
         loadersAndStoresMutex.readLock().unlock();
      }
   }

   @Override
   public void purgeExpired() throws CacheLoaderException {
      loadersAndStoresMutex.readLock().lock();
      try {
         for (CacheStore s : stores.keySet()) s.purgeExpired();
      } finally {
         loadersAndStoresMutex.readLock().unlock();
      }
   }

   @Override
   public void commit(GlobalTransaction tx) throws CacheLoaderException {
      loadersAndStoresMutex.readLock().lock();
      try {
         for (CacheStore s : stores.keySet()) s.commit(tx);
      } finally {
         loadersAndStoresMutex.readLock().unlock();
      }
   }

   @Override
   public void rollback(GlobalTransaction tx) {
      loadersAndStoresMutex.readLock().lock();
      try {
         for (CacheStore s : stores.keySet()) s.rollback(tx);
      } finally {
         loadersAndStoresMutex.readLock().unlock();
      }
   }

   @Override
   public void prepare(List<? extends Modification> list, GlobalTransaction tx, boolean isOnePhase) throws CacheLoaderException {
      loadersAndStoresMutex.readLock().lock();
      try {
         for (CacheStore s : stores.keySet()) s.prepare(list, tx, isOnePhase);
      } finally {
         loadersAndStoresMutex.readLock().unlock();
      }
   }

   @Override
   public void init(CacheLoaderConfig config, Cache<?, ?> cache, StreamingMarshaller m) throws CacheLoaderException {
      loadersAndStoresMutex.readLock().lock();
      try {
         for (Map.Entry<CacheLoader, LoaderConfiguration> e : loaders.entrySet()) {
            e.getKey().init(LegacyConfigurationAdaptor.adapt(e.getValue()), cache, m);
         }
      } finally {
         loadersAndStoresMutex.readLock().unlock();
      }
   }

   @Override
   public InternalCacheEntry load(Object key) throws CacheLoaderException {
      InternalCacheEntry se = null;
      loadersAndStoresMutex.readLock().lock();
      try {
         for (CacheLoader l : loaders.keySet()) {
            se = l.load(key);
            if (se != null) break;
         }
      } finally {
         loadersAndStoresMutex.readLock().unlock();
      }
      return se;
   }

   @Override
   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      Set<InternalCacheEntry> set = new HashSet<InternalCacheEntry>();
      loadersAndStoresMutex.readLock().lock();
      try {
         for (CacheStore s : stores.keySet()) set.addAll(s.loadAll());
      } finally {
         loadersAndStoresMutex.readLock().unlock();
      }
      return set;
   }

   @Override
   public Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException {
      if (numEntries < 0) return loadAll();
      Set<InternalCacheEntry> set = new HashSet<InternalCacheEntry>(numEntries);
      loadersAndStoresMutex.readLock().lock();
      try {
         for (CacheStore s : stores.keySet()) {
            Set<InternalCacheEntry> localSet = s.load(numEntries);
            Iterator<InternalCacheEntry> i = localSet.iterator();
            while (set.size() < numEntries && i.hasNext()) set.add(i.next());
            if (set.size() >= numEntries) break;
         }
      } finally {
         loadersAndStoresMutex.readLock().unlock();
      }
      return set;
   }

   @Override
   public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
      Set<Object> set = new HashSet<Object>();
      loadersAndStoresMutex.readLock().lock();
      try {
         for (CacheStore s : stores.keySet()) set.addAll(s.loadAllKeys(keysToExclude));
      } finally {
         loadersAndStoresMutex.readLock().unlock();
      }
      return set;
   }

   @Override
   public boolean containsKey(Object key) throws CacheLoaderException {
      loadersAndStoresMutex.readLock().lock();
      try {
         for (CacheLoader l : loaders.keySet()) {
            if (l.containsKey(key)) return true;
         }
      } finally {
         loadersAndStoresMutex.readLock().unlock();
      }
      return false;
   }

   @Override
   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return null;
   }

   @Override
   public void start() throws CacheLoaderException {
      loadersAndStoresMutex.readLock().lock();
      try {
         for (CacheLoader l : loaders.keySet()) l.start();
      } finally {
         loadersAndStoresMutex.readLock().unlock();
      }
   }

   @Override
   public void stop() throws CacheLoaderException {
      loadersAndStoresMutex.readLock().lock();
      try {
         for (CacheLoader l : loaders.keySet()) l.stop();
      } finally {
         loadersAndStoresMutex.readLock().unlock();
      }
   }

   public void addCacheLoader(CacheLoader loader, LoaderConfiguration config) {
      loadersAndStoresMutex.writeLock().lock();
      try {
         loaders.put(loader, config);
         if (loader instanceof CacheStore) stores.put((CacheStore) loader, (StoreConfiguration) config);
      } finally {
         loadersAndStoresMutex.writeLock().unlock();
      }
   }

   public void purgeIfNecessary() throws CacheLoaderException {
      loadersAndStoresMutex.readLock().lock();
      try {
         for (Map.Entry<CacheStore, StoreConfiguration> e : stores.entrySet()) {
            StoreConfiguration value = e.getValue();
            if (value.purgeOnStartup())
               e.getKey().clear();
         }
      } finally {
         loadersAndStoresMutex.readLock().unlock();
      }
   }

   public LinkedHashMap<CacheStore, StoreConfiguration> getStores() {
      loadersAndStoresMutex.readLock().lock();
      try {
         return new LinkedHashMap<CacheStore, StoreConfiguration>(stores);
      } finally {
         loadersAndStoresMutex.readLock().unlock();
      }
   }

   @Override
   public CacheStoreConfig getCacheStoreConfig() {
      return null;
   }

   public void removeCacheLoader(String loaderType) {
      loadersAndStoresMutex.writeLock().lock();
      try {
         Set<CacheLoader> toRemove = new HashSet<CacheLoader>();

         for (CacheStore cs : stores.keySet()) {
            if (cs.getClass().getName().equals(loaderType)) toRemove.add(cs);
         }

         for (CacheLoader cl : loaders.keySet()) {
            if (cl.getClass().getName().equals(loaderType)) toRemove.add(cl);
         }

         for (CacheLoader cl : toRemove) {
            try {
               log.debugf("Stopping and removing cache loader %s", loaderType);
               cl.stop();
            } catch (Exception e) {
               log.infof("Problems shutting down cache loader %s", loaderType, e);
            }
            stores.remove(cl);
            loaders.remove(cl);
         }
      } finally {
         loadersAndStoresMutex.writeLock().unlock();
      }
   }
}
