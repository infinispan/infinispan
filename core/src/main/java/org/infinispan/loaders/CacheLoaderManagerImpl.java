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
package org.infinispan.loaders;

import org.infinispan.AdvancedCache;
import org.infinispan.CacheException;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationException;
import org.infinispan.container.entries.InternalCacheEntry;

import static org.infinispan.context.Flag.*;

import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.loaders.decorators.AsyncStore;
import org.infinispan.loaders.decorators.ChainingCacheStore;
import org.infinispan.loaders.decorators.ReadOnlyStore;
import org.infinispan.loaders.decorators.SingletonStore;
import org.infinispan.loaders.decorators.SingletonStoreConfig;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.util.ReflectionUtil;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collections;
import java.util.Set;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.context.Flag.SKIP_REMOTE_LOOKUP;
import static org.infinispan.factories.KnownComponentNames.CACHE_MARSHALLER;

public class CacheLoaderManagerImpl implements CacheLoaderManager {

   Configuration configuration;
   CacheLoaderManagerConfig clmConfig;
   AdvancedCache<Object, Object> cache;
   StreamingMarshaller m;
   CacheLoader loader;
   InvocationContextContainer icc;
   private static final Log log = LogFactory.getLog(CacheLoaderManagerImpl.class);

   @Inject
   public void inject(AdvancedCache<Object, Object> cache,
                      @ComponentName(CACHE_MARSHALLER) StreamingMarshaller marshaller,
                      Configuration configuration, InvocationContextContainer icc) {
      this.cache = cache;
      this.m = marshaller;
      this.configuration = configuration;
      this.icc = icc;
   }

   public CacheLoader getCacheLoader() {
      return loader;
   }

   public final CacheStore getCacheStore() {
      if (loader != null && loader instanceof CacheStore) {
         return (CacheStore) loader;
      } else {
         return null;
      }
   }

   public void purge() {
      CacheStore cs = getCacheStore();
      if (cs != null) try {
         cs.clear();
      } catch (CacheLoaderException e) {
         throw new CacheException("Unable to purge cache store", e);
      }
   }

   private void purgeLoaders(boolean force) throws Exception {
      CacheStore cs = getCacheStore();
      if (cs != null) {
         if ((cs instanceof ChainingCacheStore) && !force) {
            ((ChainingCacheStore) loader).purgeIfNecessary();
         } else {
            CacheStoreConfig first = (CacheStoreConfig) clmConfig.getFirstCacheLoaderConfig();
            if (force || (first != null && first.isPurgeOnStartup())) {
               cs.clear();
            }
         }
      }
   }

   public boolean isUsingPassivation() {
      return isEnabled() ? clmConfig.isPassivation() : false;
   }

   public boolean isShared() {
      return isEnabled() ? clmConfig.isShared() : false;
   }

   public boolean isFetchPersistentState() {
      return isEnabled() ? clmConfig.isFetchPersistentState() : false;
   }

   @Start(priority = 10)
   public void start() {
      clmConfig = configuration.getCacheLoaderManagerConfig();
      if (clmConfig != null) {
         try {
            loader = createCacheLoader();
            if (loader != null) loader.start();
            purgeLoaders(false);
         } catch (Exception e) {
            throw new CacheException("Unable to start cache loaders", e);
         }
      }
   }

   public boolean isEnabled() {
      return clmConfig != null;
   }

   /**
    * Performs a preload on the cache based on the cache loader preload configs used when configuring the cache.
    */
   @Start(priority = 56)
   public void preload() {
      if (loader != null) {
         if (clmConfig.isPreload()) {
            long start = 0;
            boolean debugTiming = log.isDebugEnabled();
            if (debugTiming) {
               start = System.currentTimeMillis();
               log.debugf("Preloading transient state from cache loader %s", loader);
            }
            Set<InternalCacheEntry> state;
            try {
               state = loadState();
            } catch (CacheLoaderException e) {
               throw new CacheException("Unable to preload!", e);
            }

            for (InternalCacheEntry e : state) {
               if (clmConfig.isShared() || !(loader instanceof ChainingCacheStore)) {
                  cache.getAdvancedCache()
                       .withFlags(SKIP_CACHE_STATUS_CHECK, CACHE_MODE_LOCAL, SKIP_CACHE_STORE, SKIP_REMOTE_LOOKUP, SKIP_INDEXING)
                       .put(e.getKey(), e.getValue(), e.getLifespan(), MILLISECONDS, e.getMaxIdle(), MILLISECONDS);
               } else {
                  cache.getAdvancedCache()
                       .withFlags(SKIP_CACHE_STATUS_CHECK, CACHE_MODE_LOCAL, SKIP_REMOTE_LOOKUP, SKIP_INDEXING)
                       .put(e.getKey(), e.getValue(), e.getLifespan(), MILLISECONDS, e.getMaxIdle(), MILLISECONDS);
               }
            }

            if (debugTiming) {
               long stop = System.currentTimeMillis();
               log.debugf("Preloaded %s keys in %s milliseconds", state.size(), stop - start);
            }
         }
      }
   }

   private Set<InternalCacheEntry> loadState() throws CacheLoaderException {
      int ne = -1;
      if (configuration.getEvictionStrategy().isEnabled()) ne = configuration.getEvictionMaxEntries();
      Set<InternalCacheEntry> state;
      switch (ne) {
         case -1:
            state = loader.loadAll();
            break;
         case 0:
            state = Collections.emptySet();
            break;
         default:
            state = loader.load(ne);
            break;
      }
      return state;
   }

   @Stop
   public void stop() {
      if (loader != null) {
         try {
            CacheStore store = getCacheStore();
            if (store != null) {
               InvocationContext ctx = icc.createNonTxInvocationContext();
               if (ctx.hasFlag(REMOVE_DATA_ON_STOP)) {
                  if (log.isTraceEnabled()) log.trace("Requested removal of data on stop, so clear cache store");
                  store.clear();
               }
            }
            loader.stop();
         } catch (CacheLoaderException e) {
            throw new CacheException(e);
         } finally {
            loader = null;
         }
      }
   }

   CacheLoader createCacheLoader() throws Exception {
      CacheLoader tmpLoader;
      // if we only have a single cache loader configured in the chaining cacheloader then
      // don't use a chaining cache loader at all.
      // also if we are using passivation then just directly use the first cache loader.
      if (clmConfig.useChainingCacheLoader()) {
         // create chaining cache loader.
         ChainingCacheStore ccl = new ChainingCacheStore();
         tmpLoader = ccl;

         // only one cache loader may have fetchPersistentState to true.
         int numLoadersWithFetchPersistentState = 0;
         for (CacheLoaderConfig cfg : clmConfig.getCacheLoaderConfigs()) {
            if (cfg instanceof CacheStoreConfig) {
               if (((CacheStoreConfig) cfg).isFetchPersistentState()) numLoadersWithFetchPersistentState++;
               if (numLoadersWithFetchPersistentState > 1)
                  throw new Exception("Invalid cache loader configuration!!  Only ONE cache loader may have fetchPersistentState set to true.  Cache will not start!");
               assertNotSingletonAndShared(((CacheStoreConfig) cfg));
            }

            CacheLoader l = createCacheLoader(cfg, cache);
            ccl.addCacheLoader(l, cfg);
         }
      } else {
         CacheLoaderConfig cfg = clmConfig.getFirstCacheLoaderConfig();
         if (cfg != null) {
            tmpLoader = createCacheLoader(cfg, cache);
            if (cfg instanceof CacheStoreConfig)
            assertNotSingletonAndShared(((CacheStoreConfig) cfg));
         } else {
            return null;
         }
      }

      // Update the config with those actually used by the loaders
      ReflectionUtil.setValue(clmConfig, "accessible", true);

      return tmpLoader;
   }

   CacheLoader createCacheLoader(CacheLoaderConfig cfg, AdvancedCache<Object, Object> cache) throws Exception {
      CacheLoader tmpLoader = (CacheLoader) Util.getInstance(cfg.getCacheLoaderClassName(), cache.getClassLoader());

      if (tmpLoader != null) {
         if (cfg instanceof CacheStoreConfig) {
            CacheStore tmpStore = (CacheStore) tmpLoader;
            // async?
            CacheStoreConfig cfg2 = (CacheStoreConfig) cfg;
            if (cfg2.getAsyncStoreConfig().isEnabled()) {
               tmpStore = new AsyncStore(tmpStore, cfg2.getAsyncStoreConfig());
               tmpLoader = tmpStore;
            }

            // read only?
            if (cfg2.isIgnoreModifications()) {
               tmpStore = new ReadOnlyStore(tmpStore);
               tmpLoader = tmpStore;
            }

            // singleton?
            SingletonStoreConfig ssc = cfg2.getSingletonStoreConfig();
            if (ssc != null && ssc.isSingletonStoreEnabled()) {
               tmpStore = new SingletonStore(tmpStore, cache, ssc);
               tmpLoader = tmpStore;
            }
         }

         // load props
         tmpLoader.init(cfg, cache, m);
      }
      return tmpLoader;
   }

   void assertNotSingletonAndShared(CacheStoreConfig cfg) {
      SingletonStoreConfig ssc = cfg.getSingletonStoreConfig();
      if (ssc != null && ssc.isSingletonStoreEnabled() && clmConfig.isShared())
         throw new ConfigurationException("Invalid cache loader configuration!!  If a cache loader is configured as a singleton, the cache loader cannot be shared in a cluster!");
   }
}
