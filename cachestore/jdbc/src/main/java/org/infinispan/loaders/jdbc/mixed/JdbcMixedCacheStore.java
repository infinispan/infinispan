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
package org.infinispan.loaders.jdbc.mixed;

import org.infinispan.Cache;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.AbstractCacheStore;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderMetadata;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.jdbc.binary.JdbcBinaryCacheStore;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * Cache store that combines functionality of {@link JdbcBinaryCacheStore} and {@link JdbcStringBasedCacheStore}. It
 * aggregates an instance of JdbcBinaryCacheStore and JdbcStringBasedCacheStore, delegating work to one of them
 * (sometimes both, see below) based on the passed in key. In order to determine which store to use it will rely on the
 * configured {@link org.infinispan.loaders.keymappers.Key2StringMapper} )(see configuration).
 * <p/>
 * The advantage it brings is the possibility of efficiently storing string(able) keyed {@link
 * org.infinispan.container.entries.InternalCacheEntry}s, and at the same time being able to store any other keys, a la
 * {@link org.infinispan.loaders.jdbc.binary.JdbcBinaryCacheStore}.
 * <p/>
 * There will only be a performance cost for the aggregate operations: loadAll, fromStream, toStream and clear. For
 * these operations there will be two distinct database call, one for each JdbcStore implementation. Most of application
 * are only using these operations at lifecycles changes (e.g. fromStream and toStream at cluster join time, loadAll at
 * startup for warm caches), so performance drawback shouldn't be significant (again, most of the cases).
 * <p/>
 * Resource sharing - both aggregated cache loaders have locks and connection pools. The locking is not shared, each
 * loader keeping its own {@link org.infinispan.util.concurrent.locks.StripedLock} instance. Also the tables (even though
 * similar as definition) are different in order to avoid key collision. On the other hand, the connection pooling is a
 * shared resource.
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.loaders.jdbc.mixed.JdbcMixedCacheStoreConfig
 * @see org.infinispan.loaders.jdbc.binary.JdbcBinaryCacheStore
 * @see org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore
 */
@CacheLoaderMetadata(configurationClass = JdbcMixedCacheStoreConfig.class)
public class JdbcMixedCacheStore extends AbstractCacheStore {

   private static final Log log = LogFactory.getLog(JdbcMixedCacheStore.class);

   private JdbcMixedCacheStoreConfig config;
   private JdbcBinaryCacheStore binaryCacheStore = new JdbcBinaryCacheStore();
   private JdbcStringBasedCacheStore stringBasedCacheStore = new JdbcStringBasedCacheStore();
   private ConnectionFactory sharedConnectionFactory;

   @Override
   public void init(CacheLoaderConfig config, Cache cache, StreamingMarshaller m) throws CacheLoaderException {
      super.init(config, cache, m);
      this.config = (JdbcMixedCacheStoreConfig) config;
      binaryCacheStore.init(this.config.getBinaryCacheStoreConfig(), cache, m);
      stringBasedCacheStore.init(this.config.getStringCacheStoreConfig(), cache, m);
   }

   @Override
   public void start() throws CacheLoaderException {
      super.start();
      ConnectionFactoryConfig factoryConfig = config.getConnectionFactoryConfig();
      sharedConnectionFactory = ConnectionFactory.getConnectionFactory(factoryConfig.getConnectionFactoryClass(), config.getClassLoader());
      sharedConnectionFactory.start(factoryConfig, config.getClassLoader());
      binaryCacheStore.doConnectionFactoryInitialization(sharedConnectionFactory);
      binaryCacheStore.start();
      stringBasedCacheStore.doConnectionFactoryInitialization(sharedConnectionFactory);
      stringBasedCacheStore.start();
   }

   @Override
   public void stop() throws CacheLoaderException {
      super.stop();

      Throwable cause = null;
      try {
         binaryCacheStore.stop();
      } catch (Throwable t) {
         if (cause == null) cause = t;
         log.debug("Exception while stopping", t);
      }
      try {
         stringBasedCacheStore.stop();
      } catch (Throwable t) {
         if (cause == null) cause = t;
         log.debug("Exception while stopping", t);
      }
      try {
         sharedConnectionFactory.stop();
      } catch (Throwable t) {
         if (cause == null) cause = t;
         log.debug("Exception while stopping", t);
      }
      if (cause != null) {
         throw new CacheLoaderException("Exceptions occurred while stopping store", cause);
      }
   }

   @Override
   protected void purgeInternal() throws CacheLoaderException {
      binaryCacheStore.purgeInternal();
      stringBasedCacheStore.purgeInternal();
   }

   public InternalCacheEntry load(Object key) throws CacheLoaderException {
      return getCacheStore(key).load(key);
   }

   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      Set<InternalCacheEntry> fromBuckets = binaryCacheStore.loadAll();
      Set<InternalCacheEntry> fromStrings = stringBasedCacheStore.loadAll();
      if (log.isTraceEnabled()) {
         log.tracef("Loaded from bucket: %s", fromBuckets);
         log.tracef("Loaded from string: %s", fromStrings);
      }
      fromBuckets.addAll(fromStrings);
      return fromBuckets;
   }

   @Override
   public Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException {
      if (numEntries < 0) return loadAll();
      Set<InternalCacheEntry> set = stringBasedCacheStore.load(numEntries);

      if (set.size() < numEntries) {
         Set<InternalCacheEntry> otherSet = binaryCacheStore.load(numEntries - set.size());
         set.addAll(otherSet);
      }

      return set;
   }

   @Override
   public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
      Set<Object> fromBuckets = binaryCacheStore.loadAllKeys(keysToExclude);
      Set<Object> fromStrings = stringBasedCacheStore.loadAllKeys(keysToExclude);
      fromBuckets.addAll(fromStrings);
      return fromBuckets;
   }

   public void store(InternalCacheEntry ed) throws CacheLoaderException {
      getCacheStore(ed.getKey()).store(ed);
   }

   public void fromStream(ObjectInput inputStream) throws CacheLoaderException {
      binaryCacheStore.fromStream(inputStream);
      stringBasedCacheStore.fromStream(inputStream);
   }

   public void toStream(ObjectOutput outputStream) throws CacheLoaderException {
      binaryCacheStore.toStream(outputStream);
      stringBasedCacheStore.toStream(outputStream);
   }

   public boolean remove(Object key) throws CacheLoaderException {
      return getCacheStore(key).remove(key);
   }

   public void clear() throws CacheLoaderException {
      binaryCacheStore.clear();
      stringBasedCacheStore.clear();
   }

   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return JdbcMixedCacheStoreConfig.class;
   }

   private CacheStore getCacheStore(Object key) {
      return stringBasedCacheStore.supportsKey(key.getClass()) ? stringBasedCacheStore : binaryCacheStore;
   }

   public ConnectionFactory getConnectionFactory() {
      return sharedConnectionFactory;
   }

   public JdbcBinaryCacheStore getBinaryCacheStore() {
      return binaryCacheStore;
   }

   public JdbcStringBasedCacheStore getStringBasedCacheStore() {
      return stringBasedCacheStore;
   }
}
