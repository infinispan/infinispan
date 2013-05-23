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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.context.Flag.*;
import static org.infinispan.factories.KnownComponentNames.CACHE_MARSHALLER;
import static org.infinispan.loaders.decorators.AbstractDelegatingStore.undelegateCacheLoader;

import java.util.*;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.CacheException;
import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.configuration.cache.CacheLoaderConfiguration;
import org.infinispan.configuration.cache.LoadersConfiguration;
import org.infinispan.configuration.cache.CacheStoreConfiguration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.CacheLoaderInterceptor;
import org.infinispan.interceptors.CacheStoreInterceptor;
import org.infinispan.loaders.decorators.AsyncStore;
import org.infinispan.loaders.decorators.ChainingCacheStore;
import org.infinispan.loaders.decorators.ReadOnlyStore;
import org.infinispan.loaders.decorators.SingletonStore;
import org.infinispan.loaders.decorators.SingletonStoreConfig;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.TimeService;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class CacheLoaderManagerImpl implements CacheLoaderManager {

   Configuration configuration;
   LoadersConfiguration clmConfig;
   AdvancedCache<Object, Object> cache;
   StreamingMarshaller m;
   CacheLoader loader;
   InvocationContextContainer icc;
   TransactionManager transactionManager;
   private TimeService timeService;
   private static final Log log = LogFactory.getLog(CacheLoaderManagerImpl.class);

   @Inject
   public void inject(AdvancedCache<Object, Object> cache,
                      @ComponentName(CACHE_MARSHALLER) StreamingMarshaller marshaller,
                      Configuration configuration, InvocationContextContainer icc, TransactionManager transactionManager,
                      TimeService timeService) {
      this.cache = cache;
      this.m = marshaller;
      this.configuration = configuration;
      this.icc = icc;
      this.transactionManager = transactionManager;
      this.timeService = timeService;
   }

   @Override
   public CacheLoader getCacheLoader() {
      return loader;
   }

   @Override
   public final CacheStore getCacheStore() {
      if (loader != null && loader instanceof CacheStore) {
         return (CacheStore) loader;
      } else {
         return null;
      }
   }

   @Override
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
            CacheLoaderConfiguration first = clmConfig.cacheLoaders().get(0);
            if (force || (first != null && first instanceof CacheStoreConfiguration && ((CacheStoreConfiguration)first).purgeOnStartup())) {
               cs.clear();
            }
         }
      }
   }

   @Override
   public boolean isUsingPassivation() {
      return isEnabled() ? clmConfig.passivation() : false;
   }

   @Override
   public boolean isShared() {
      return isEnabled() ? clmConfig.shared() : false;
   }

   @Override
   public boolean isFetchPersistentState() {
      return isEnabled() ? clmConfig.fetchPersistentState() : false;
   }

   @Override
   @Start(priority = 10)
   public void start() {
      clmConfig = configuration.loaders();
      if (clmConfig != null) {
         try {
            loader = createCacheLoader();
            Transaction xaTx = null;
            if (transactionManager != null) {
               xaTx = transactionManager.suspend();
            }
            try {
               if (loader != null) loader.start();
            } finally {
               if (transactionManager != null && xaTx != null) {
                  transactionManager.resume(xaTx);
               }
            }
            purgeLoaders(false);
         } catch (Exception e) {
            throw new CacheException("Unable to start cache loaders", e);
         }
      }
   }

   @Override
   public boolean isEnabled() {
      return clmConfig != null;
   }

   @Override
   public void disableCacheStore(String loaderType) {
      if (isEnabled()) {
         boolean disableInterceptors = false;
         ComponentRegistry cr = cache.getComponentRegistry();
         CacheLoaderInterceptor cli = cr.getComponent(CacheLoaderInterceptor.class);
         CacheStoreInterceptor csi = cr.getComponent(CacheStoreInterceptor.class);

         if (loader instanceof ChainingCacheStore) {
            ChainingCacheStore ccs = (ChainingCacheStore) loader;
            ccs.removeCacheLoader(loaderType);
            if (ccs.getStores().isEmpty()) disableInterceptors = true;
         } else {
            String loaderClassName = undelegateCacheLoader(loader).getClass().getName();
            if (loaderClassName.equals(loaderType)) {
               try {
                  log.debugf("Stopping and removing cache loader %s", loaderType);
                  loader.stop();
               } catch (Exception e) {
                  log.infof("Problems shutting down cache loader %s", loaderType, e);
               }
               disableInterceptors = true;
            }
         }

         if (disableInterceptors) {
            cli.disableInterceptor();
            csi.disableInterceptor();
            cache.removeInterceptor(cli.getClass());
            cache.removeInterceptor(csi.getClass());
            clmConfig = null;
         }
      }
   }

   @Override
   public <T extends CacheLoader> List<T> getCacheLoaders(Class<T> loaderClass) {
      List<T> loaders;
      if (loader instanceof ChainingCacheStore) {
         ChainingCacheStore ccs = (ChainingCacheStore) loader;
         loaders = ccs.getCacheLoaders(loaderClass);
      } else {
         CacheLoader cl = undelegateCacheLoader(loader);
         if (loaderClass.isInstance(cl)) {
            loaders = Collections.singletonList((T) cl);
         } else {
            loaders = Collections.emptyList();
         }
      }
      return loaders;
   }

   /**
    * Performs a preload on the cache based on the cache loader preload configs used when configuring the cache.
    */
   @Override
   @Start(priority = 56)
   public void preload() {
      if (loader != null) {
         if (clmConfig.preload()) {
            long start = 0;
            boolean debugTiming = log.isDebugEnabled();
            if (debugTiming) {
               start = timeService.time();
               log.debugf("Preloading transient state from cache loader %s", loader);
            }
            Set<InternalCacheEntry> state;
            try {
               state = loadState();
            } catch (CacheLoaderException e) {
               throw new CacheException("Unable to preload!", e);
            }

            List<Flag> flags = new ArrayList<Flag>(Arrays.asList(
                  CACHE_MODE_LOCAL, SKIP_OWNERSHIP_CHECK, IGNORE_RETURN_VALUES, SKIP_CACHE_STORE, SKIP_LOCKING));

            if (clmConfig.shared() || !(loader instanceof ChainingCacheStore)) {
               flags.add(SKIP_CACHE_STORE);
               if (!localIndexingEnabled())
                  flags.add(SKIP_INDEXING);
            } else {
               flags.add(SKIP_INDEXING);
            }

            AdvancedCache<Object, Object> flaggedCache = cache.getAdvancedCache()
                  .withFlags(flags.toArray(new Flag[flags.size()]));

            for (InternalCacheEntry e : state)
               flaggedCache.put(e.getKey(), e.getValue(), e.getMetadata());

            if (debugTiming) {
               log.debugf("Preloaded %s keys in %s", state.size(),
                          Util.prettyPrintTime(timeService.timeDuration(start, MILLISECONDS)));
            }
         }
      }
   }

   private boolean localIndexingEnabled() {
      return configuration.indexing().enabled() && configuration.indexing().indexLocalOnly();
   }

   private Set<InternalCacheEntry> loadState() throws CacheLoaderException {
      int ne = -1;
      if (configuration.eviction().strategy().isEnabled()) ne = configuration.eviction().maxEntries();
      Set<InternalCacheEntry> state;
      switch (ne) {
         case -1:
            state = loader.loadAll();
            break;
         case 0:
            state = InfinispanCollections.emptySet();
            break;
         default:
            state = loader.load(ne);
            break;
      }
      return state;
   }

   @Override
   @Stop
   public void stop() {
      if (loader != null) {
         try {
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
      if (clmConfig.usingChainingCacheLoader()) {
         // create chaining cache loader.
         ChainingCacheStore ccl = new ChainingCacheStore();
         tmpLoader = ccl;

         // only one cache loader may have fetchPersistentState to true.
         int numLoadersWithFetchPersistentState = 0;
         for (CacheLoaderConfiguration cfg : clmConfig.cacheLoaders()) {
            if (cfg instanceof CacheStoreConfiguration) {
               CacheStoreConfiguration scfg = (CacheStoreConfiguration) cfg;
               assertNotSingletonAndShared(scfg);
               if(scfg.fetchPersistentState()) numLoadersWithFetchPersistentState++;
            }
            if (numLoadersWithFetchPersistentState > 1)
               throw new Exception("Invalid cache loader configuration!!  Only ONE cache loader may have fetchPersistentState set to true.  Cache will not start!");

            CacheLoader l = createCacheLoader(LegacyConfigurationAdaptor.adapt(cfg), cache);
            ccl.addCacheLoader(l, cfg);
         }
      } else {
         if (!clmConfig.cacheLoaders().isEmpty()) {
            CacheLoaderConfiguration cfg = clmConfig.cacheLoaders().get(0);
            if (cfg instanceof CacheStoreConfiguration)
               assertNotSingletonAndShared((CacheStoreConfiguration) cfg);
            tmpLoader = createCacheLoader(LegacyConfigurationAdaptor.adapt(cfg), cache);
         } else {
            return null;
         }
      }

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
               tmpStore = createAsyncStore(tmpStore, cfg2);
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

   protected AsyncStore createAsyncStore(CacheStore tmpStore, CacheStoreConfig cfg2) {
      return new AsyncStore(tmpStore, cfg2.getAsyncStoreConfig());
   }

   void assertNotSingletonAndShared(CacheStoreConfiguration cfg) {
      if (cfg.singletonStore().enabled() && clmConfig.shared())
         throw new ConfigurationException("Invalid cache loader configuration!!  If a cache loader is configured as a singleton, the cache loader cannot be shared in a cluster!");
   }
}
