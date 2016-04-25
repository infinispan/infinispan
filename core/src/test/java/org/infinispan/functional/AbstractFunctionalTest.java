package org.infinispan.functional;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.BeforeClass;

import java.util.Random;

abstract class AbstractFunctionalTest extends MultipleCacheManagersTest {

   static final String DIST = "dist";
   static final String REPL = "repl";

   FunctionalMapImpl<Integer, String> fmapL1;
   FunctionalMapImpl<Integer, String> fmapL2;

   FunctionalMapImpl<Object, String> fmapD1;
   FunctionalMapImpl<Object, String> fmapD2;

   FunctionalMapImpl<Object, String> fmapR1;
   FunctionalMapImpl<Object, String> fmapR2;

   @Override
   protected void createCacheManagers() throws Throwable {
      // Create local caches as default in a cluster of 2
      ConfigurationBuilder localBuilder = new ConfigurationBuilder();
      localBuilder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
      createClusteredCaches(2, localBuilder);
      // Create distributed caches
      ConfigurationBuilder distBuilder = new ConfigurationBuilder();
      distBuilder.clustering().cacheMode(CacheMode.DIST_SYNC).hash().numOwners(1);
      distBuilder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
      cacheManagers.stream().forEach(cm -> cm.defineConfiguration(DIST, distBuilder.build()));
      // Create replicated caches
      ConfigurationBuilder replBuilder = new ConfigurationBuilder();
      replBuilder.clustering().cacheMode(CacheMode.REPL_SYNC);
      replBuilder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
      cacheManagers.stream().forEach(cm -> cm.defineConfiguration(REPL, replBuilder.build()));
      // Wait for cluster to form
      waitForClusterToForm(DIST, REPL);
   }

   @Override
   @BeforeClass
   public void createBeforeClass() throws Throwable {
      super.createBeforeClass();
      fmapL1 = FunctionalMapImpl.create(cacheManagers.get(0).<Integer, String>getCache().getAdvancedCache());
      fmapL2 = FunctionalMapImpl.create(cacheManagers.get(0).<Integer, String>getCache().getAdvancedCache());
      fmapD1 = FunctionalMapImpl.create(cacheManagers.get(0).<Object, String>getCache(DIST).getAdvancedCache());
      fmapD2 = FunctionalMapImpl.create(cacheManagers.get(1).<Object, String>getCache(DIST).getAdvancedCache());
      fmapR1 = FunctionalMapImpl.create(cacheManagers.get(0).<Object, String>getCache(REPL).getAdvancedCache());
      fmapR2 = FunctionalMapImpl.create(cacheManagers.get(1).<Object, String>getCache(REPL).getAdvancedCache());
   }

}
