package org.infinispan.multimap.impl;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.multimap.api.embedded.EmbeddedMultimapCacheManagerFactory;
import org.infinispan.multimap.api.embedded.MultimapCache;
import org.infinispan.multimap.api.embedded.MultimapCacheManager;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.StoreTypeMultimapCacheTest")
public class StoreTypeMultimapCacheTest extends DistributedMultimapCacheTest {

   protected Map<Address, MultimapCache<String, String>> multimapCacheCluster = new HashMap<>();

   public StoreTypeMultimapCacheTest() {
      super();
      l1CacheEnabled = false;
      cacheMode = CacheMode.REPL_SYNC;
      transactional = false;
      fromOwner = true;
   }

   @Override
   public Object[] factory() {
      return new Object[]{
            new StoreTypeMultimapCacheTest().storageType(StorageType.OFF_HEAP),
            new StoreTypeMultimapCacheTest().storageType(StorageType.HEAP),
      };
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      for (EmbeddedCacheManager cacheManager : cacheManagers) {
         MultimapCacheManager multimapCacheManager = EmbeddedMultimapCacheManagerFactory.from(cacheManager);
         multimapCacheCluster.put(cacheManager.getAddress(), multimapCacheManager.get(cacheName));
      }
   }

   @Override
   protected ConfigurationBuilder buildConfiguration() {
      ConfigurationBuilder cacheCfg = super.buildConfiguration();
      cacheCfg.memory().storage(storageType);
      return cacheCfg;
   }
}
