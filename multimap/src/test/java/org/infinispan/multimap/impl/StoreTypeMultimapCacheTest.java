package org.infinispan.multimap.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

   protected StorageType storageType;

   public StoreTypeMultimapCacheTest() {
      super();
      l1CacheEnabled = false;
      cacheMode = CacheMode.REPL_SYNC;
      transactional = false;
      fromOwner = true;
   }

   public StoreTypeMultimapCacheTest storageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), "storageType");
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), storageType);
   }

   @Override
   public Object[] factory() {
      List testsToRun = new ArrayList<>();
      testsToRun.add(new StoreTypeMultimapCacheTest().storageType(StorageType.OFF_HEAP));
      testsToRun.add(new StoreTypeMultimapCacheTest().storageType(StorageType.OBJECT));
      testsToRun.add(new StoreTypeMultimapCacheTest().storageType(StorageType.BINARY));
      return testsToRun.toArray();
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
      cacheCfg.memory().storageType(storageType);
      return cacheCfg;
   }
}
