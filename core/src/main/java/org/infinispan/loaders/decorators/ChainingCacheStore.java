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

import org.infinispan.Cache;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.configuration.cache.LoaderConfiguration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoader;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.modifications.Modification;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A chaining cache loader that allows us to configure > 1 cache loader.
 * <p/>
 * READ operations are directed to each of the cache loaders (in the order which they were
 * configured) until a non-null (or non-empty in the case of retrieving collection objects) result
 * is achieved.
 * <p/>
 * WRITE operations are propagated to ALL registered cache stores specified, except those that set
 * ignoreModifications to false.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ChainingCacheStore implements CacheStore {

   // linked hash sets used since it provides fast (O(1)) iteration, maintains order and provides O(1) lookups to values as well.
   LinkedHashMap<CacheLoader, LoaderConfiguration> loaders = new LinkedHashMap<CacheLoader, LoaderConfiguration>(2);
   LinkedHashMap<CacheStore, StoreConfiguration> stores = new LinkedHashMap<CacheStore, StoreConfiguration>(2);

   @Override
   public void store(InternalCacheEntry ed) throws CacheLoaderException {
      for (CacheStore s : stores.keySet()) s.store(ed);
   }

   @Override
   public void fromStream(ObjectInput inputStream) throws CacheLoaderException {
      // loading and storing state via streams is *only* supported on the *first* store that has fetchPersistentState set.
      for (Map.Entry<CacheStore, StoreConfiguration> e : stores.entrySet()) {
         if (e.getValue().fetchPersistentState()) {
            e.getKey().fromStream(inputStream);
            // do NOT continue this for other stores, since the stream will not be in an appropriate state anymore
            break;
         }
      }
   }

   @Override
   public void toStream(ObjectOutput outputStream) throws CacheLoaderException {
      // loading and storing state via streams is *only* supported on the *first* store that has fetchPersistentState set.
      for (Map.Entry<CacheStore, StoreConfiguration> e : stores.entrySet()) {
         if (e.getValue().fetchPersistentState()) {
            e.getKey().toStream(outputStream);
            // do NOT continue this for other stores, since the stream will not be in an appropriate state anymore
            break;
         }
      }
   }

   @Override
   public void clear() throws CacheLoaderException {
      for (CacheStore s : stores.keySet()) s.clear();
   }

   @Override
   public boolean remove(Object key) throws CacheLoaderException {
      boolean r = false;
      for (CacheStore s : stores.keySet()) r = s.remove(key) || r;
      return r;
   }

   @Override
   public void removeAll(Set<Object> keys) throws CacheLoaderException {
      for (CacheStore s : stores.keySet()) s.removeAll(keys);
   }

   @Override
   public void purgeExpired() throws CacheLoaderException {
      for (CacheStore s : stores.keySet()) s.purgeExpired();
   }

   @Override
   public void commit(GlobalTransaction tx) throws CacheLoaderException {
      for (CacheStore s : stores.keySet()) s.commit(tx);
   }

   @Override
   public void rollback(GlobalTransaction tx) {
      for (CacheStore s : stores.keySet()) s.rollback(tx);
   }

   @Override
   public void prepare(List<? extends Modification> list, GlobalTransaction tx, boolean isOnePhase) throws CacheLoaderException {
      for (CacheStore s : stores.keySet()) s.prepare(list, tx, isOnePhase);
   }

   @Override
   public void init(CacheLoaderConfig config, Cache<?, ?> cache, StreamingMarshaller m) throws CacheLoaderException {
      for (Map.Entry<CacheLoader, LoaderConfiguration> e : loaders.entrySet()) {
         e.getKey().init(LegacyConfigurationAdaptor.adapt(e.getValue()), cache, m);
      }
   }

   @Override
   public InternalCacheEntry load(Object key) throws CacheLoaderException {
      InternalCacheEntry se = null;
      for (CacheLoader l : loaders.keySet()) {
         se = l.load(key);
         if (se != null) break;
      }
      return se;
   }

   @Override
   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      Set<InternalCacheEntry> set = new HashSet<InternalCacheEntry>();
      for (CacheStore s : stores.keySet()) set.addAll(s.loadAll());
      return set;
   }

   @Override
   public Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException {
      if (numEntries < 0) return loadAll();
      Set<InternalCacheEntry> set = new HashSet<InternalCacheEntry>(numEntries);
      for (CacheStore s: stores.keySet()) {
         Set<InternalCacheEntry> localSet = s.load(numEntries);
         Iterator<InternalCacheEntry> i = localSet.iterator();
         while (set.size() < numEntries && i.hasNext()) set.add(i.next());
         if (set.size() >= numEntries) break;
      }
      return set;
   }

   @Override
   public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
      Set<Object> set = new HashSet<Object>();
      for (CacheStore s : stores.keySet()) set.addAll(s.loadAllKeys(keysToExclude));
      return set;
   }

   @Override
   public boolean containsKey(Object key) throws CacheLoaderException {
      for (CacheLoader l : loaders.keySet()) {
         if (l.containsKey(key)) return true;
      }
      return false;
   }

   @Override
   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return null;
   }

   @Override
   public void start() throws CacheLoaderException {
      for (CacheLoader l : loaders.keySet()) l.start();
   }

   @Override
   public void stop() throws CacheLoaderException {
      for (CacheLoader l : loaders.keySet()) l.stop();
   }

   public void addCacheLoader(CacheLoader loader, LoaderConfiguration config) {
      loaders.put(loader, config);
      if (loader instanceof CacheStore) stores.put((CacheStore) loader, (StoreConfiguration) config);
   }

   public void purgeIfNecessary() throws CacheLoaderException {
      for (Map.Entry<CacheStore, StoreConfiguration> e : stores.entrySet()) {
         StoreConfiguration value = e.getValue();
         if (value.purgeOnStartup())
            e.getKey().clear();
      }
   }

   public LinkedHashMap<CacheStore, StoreConfiguration> getStores() {
      return stores;
   }

   @Override
   public CacheStoreConfig getCacheStoreConfig() {
      return null;
   }
}
