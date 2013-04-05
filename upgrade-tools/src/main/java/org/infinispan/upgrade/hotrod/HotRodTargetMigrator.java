/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.upgrade.hotrod;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.remote.RemoteCacheStore;
import org.infinispan.loaders.remote.RemoteCacheStoreConfig;
import org.infinispan.marshall.Marshaller;
import org.infinispan.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.upgrade.TargetMigrator;
import org.infinispan.upgrade.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class HotRodTargetMigrator implements TargetMigrator {
   private static final String MIGRATION_MANAGER_HOT_ROD_KNOWN_KEYS = "___MigrationManager_HotRod_KnownKeys___";

   private static final Log log = LogFactory.getLog(HotRodTargetMigrator.class, Log.class);

   public HotRodTargetMigrator() {
   }

   @Override
   public String getName() {
      return "hotrod";
   }

   @Override
   public long synchronizeData(final Cache<Object, Object> cache) throws CacheException {
      int threads = Runtime.getRuntime().availableProcessors();
      ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
      CacheLoaderManager loaderManager = cr.getComponent(CacheLoaderManager.class);
      List<RemoteCacheStore> stores = loaderManager.getCacheLoaders(RemoteCacheStore.class);
      Marshaller marshaller = new GenericJBossMarshaller();
      byte[] knownKeys;
      try {
         knownKeys = marshaller.objectToByteBuffer(MIGRATION_MANAGER_HOT_ROD_KNOWN_KEYS);
      } catch (Exception e) {
         throw new CacheException(e);
      }

      for (RemoteCacheStore store : stores) {
         final RemoteCache<Object, Object> storeCache = store.getRemoteCache();
         if (storeCache.containsKey(knownKeys)) {
            RemoteCacheStoreConfig storeConfig = (RemoteCacheStoreConfig) store.getCacheStoreConfig();
            if (!storeConfig.isHotRodWrapping()) {
               throw log.remoteStoreNoHotRodWrapping(cache.getName());
            }


            Set<byte[]> keys;
            try {
               keys = (Set<byte[]>) marshaller.objectFromByteBuffer((byte[])storeCache.get(knownKeys));
            } catch (Exception e) {
               throw new CacheException(e);
            }

            ExecutorService es = Executors.newFixedThreadPool(threads);
            final AtomicInteger count = new AtomicInteger(0);
            for (final byte[] key : keys) {
               es.submit(new Runnable() {

                  @Override
                  public void run() {
                     try {
                        cache.get(key);
                        int i = count.getAndIncrement();
                        if (log.isDebugEnabled() && i % 100 == 0)
                           log.debugf(">>    Moved %s keys\n", i);
                     } catch (Exception e) {

                     }
                  }
               });

            }
            es.shutdown();
            try {
               while (!es.awaitTermination(500, TimeUnit.MILLISECONDS));
            } catch (InterruptedException e) {
               throw new CacheException(e);
            }
            return count.longValue();
         }
      }
      throw log.missingMigrationData(cache.getName());
   }

   @Override
   public void disconnectSource(Cache<Object, Object> cache) throws CacheException {
      ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
      CacheLoaderManager loaderManager = cr.getComponent(CacheLoaderManager.class);
      loaderManager.disableCacheStore(RemoteCacheStore.class.getName());
   }
}
