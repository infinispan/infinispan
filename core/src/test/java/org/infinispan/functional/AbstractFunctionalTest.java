package org.infinispan.functional;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.BeforeClass;

abstract class AbstractFunctionalTest extends MultipleCacheManagersTest {

   static final String DIST = "dist";
   static final String REPL = "repl";

   // Create local caches as default in a cluster of 2
   int numNodes = 2;
   int numDistOwners = 1;
   boolean isSync = true;
   boolean persistence = true;
   boolean passivation = false;

   FunctionalMapImpl<Integer, String> fmapL1;
   FunctionalMapImpl<Integer, String> fmapL2;

   FunctionalMapImpl<Object, String> fmapD1;
   FunctionalMapImpl<Object, String> fmapD2;

   FunctionalMapImpl<Object, String> fmapR1;
   FunctionalMapImpl<Object, String> fmapR2;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder localBuilder = new ConfigurationBuilder();
      configureCache(localBuilder);
      createClusteredCaches(numNodes, localBuilder);
      // Create distributed caches
      ConfigurationBuilder distBuilder = new ConfigurationBuilder();
      distBuilder.clustering().cacheMode(isSync ? CacheMode.DIST_SYNC : CacheMode.DIST_ASYNC).hash().numOwners(numDistOwners);
      configureCache(distBuilder);
      cacheManagers.stream().forEach(cm -> cm.defineConfiguration(DIST, distBuilder.build()));
      // Create replicated caches
      ConfigurationBuilder replBuilder = new ConfigurationBuilder();
      replBuilder.clustering().cacheMode(isSync ? CacheMode.REPL_SYNC : CacheMode.REPL_ASYNC);
      configureCache(replBuilder);
      cacheManagers.stream().forEach(cm -> cm.defineConfiguration(REPL, replBuilder.build()));
      // Wait for cluster to form
      waitForClusterToForm(DIST, REPL);
   }

   private void configureCache(ConfigurationBuilder builder) {
      if (persistence) {
         builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
         builder.persistence().passivation(passivation);
      }
   }

   protected AbstractFunctionalTest persistence(boolean enabled) {
      persistence = enabled;
      return this;
   }

   protected AbstractFunctionalTest passivation(boolean enabled) {
      passivation = enabled;
      return this;
   }

   @Override
   @BeforeClass
   public void createBeforeClass() throws Throwable {
      super.createBeforeClass();
      if (cleanupAfterTest()) initMaps();
   }

   @Override
   public void createBeforeMethod() throws Throwable {
      super.createBeforeMethod();
      if (cleanupAfterMethod()) initMaps();
   }

   private void initMaps() {
      fmapL1 = FunctionalMapImpl.create(cacheManagers.get(0).<Integer, String>getCache().getAdvancedCache());
      fmapL2 = FunctionalMapImpl.create(cacheManagers.get(0).<Integer, String>getCache().getAdvancedCache());
      fmapD1 = FunctionalMapImpl.create(cacheManagers.get(0).<Object, String>getCache(DIST).getAdvancedCache());
      fmapD2 = FunctionalMapImpl.create(cacheManagers.get(1).<Object, String>getCache(DIST).getAdvancedCache());
      fmapR1 = FunctionalMapImpl.create(cacheManagers.get(0).<Object, String>getCache(REPL).getAdvancedCache());
      fmapR2 = FunctionalMapImpl.create(cacheManagers.get(1).<Object, String>getCache(REPL).getAdvancedCache());
   }

}
