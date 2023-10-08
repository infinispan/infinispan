package org.infinispan.functional;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

abstract class AbstractFunctionalTest extends MultipleCacheManagersTest {

   static final String DIST = "dist";
   static final String REPL = "repl";
   static final String SIMPLE = "simple";

   // Create local caches as default in a cluster of 2
   int numNodes = 2;
   int numDistOwners = 1;
   boolean isSync = true;
   boolean persistence = true;
   boolean passivation = false;

   FunctionalMapImpl<Integer, String> fmapS1;
   FunctionalMapImpl<Integer, String> fmapS2;

   FunctionalMapImpl<Integer, String> fmapL1;
   FunctionalMapImpl<Integer, String> fmapL2;

   FunctionalMapImpl<Object, String> fmapD1;
   FunctionalMapImpl<Object, String> fmapD2;

   // TODO: we should not create all those maps in tests where we don't use them
   FunctionalMapImpl<Object, String> fmapR1;
   FunctionalMapImpl<Object, String> fmapR2;

   private boolean skipSimpleCache = false;

   protected SerializationContextInitializer serializationContextInitializer = TestDataSCI.INSTANCE;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder localBuilder = new ConfigurationBuilder();
      configureCache(localBuilder);
      createClusteredCaches(numNodes, serializationContextInitializer, localBuilder);
      defineSimpleCache();
      // Create distributed caches
      ConfigurationBuilder distBuilder = new ConfigurationBuilder();
      distBuilder.clustering().cacheMode(isSync ? CacheMode.DIST_SYNC : CacheMode.DIST_ASYNC).hash().numOwners(numDistOwners);
      configureCache(distBuilder);
      cacheManagers.forEach(cm -> cm.defineConfiguration(DIST, distBuilder.build()));
      // Create replicated caches
      ConfigurationBuilder replBuilder = new ConfigurationBuilder();
      replBuilder.clustering().cacheMode(isSync ? CacheMode.REPL_SYNC : CacheMode.REPL_ASYNC);
      configureCache(replBuilder);
      cacheManagers.forEach(cm -> cm.defineConfiguration(REPL, replBuilder.build()));

      // Wait for cluster to form
      waitForClusterToForm(DIST, REPL);
   }

   private void defineSimpleCache() {
      // Create simple cache
      ConfigurationBuilder simpleBuilder = new ConfigurationBuilder();
      configureCache(simpleBuilder);
      simpleBuilder.persistence().stores().clear();
      simpleBuilder.simpleCache(true);

      try {
         cacheManagers.forEach(cm -> cm.defineConfiguration(SIMPLE, simpleBuilder.build()));
      } catch (CacheConfigurationException e) {
         skipSimpleCache = e.getMessage().startsWith("ISPN000381");
      }
   }

   protected void configureCache(ConfigurationBuilder builder) {
      builder.statistics().disable();
      if (transactional != null) {
         builder.transaction().transactionMode(transactionMode());
         if (lockingMode != null) {
            builder.transaction().lockingMode(lockingMode);
         }
      }
      if (isolationLevel != null) {
         builder.locking().isolationLevel(isolationLevel);
      }

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
   @BeforeClass(alwaysRun = true)
   public void createBeforeClass() throws Throwable {
      super.createBeforeClass();
      if (cleanupAfterTest()) initMaps();
   }

   @Override
   @BeforeMethod(alwaysRun = true)
   public void createBeforeMethod() throws Throwable {
      super.createBeforeMethod();
      if (cleanupAfterMethod()) initMaps();
   }

   protected void initMaps() {
      if (!isSkipSimpleCache()) {
         fmapS1 = FunctionalMapImpl.create(getAdvancedCache(cacheManagers.get(0), SIMPLE));
         fmapS2 = FunctionalMapImpl.create(getAdvancedCache(cacheManagers.get(0), SIMPLE));
      }

      fmapL1 = FunctionalMapImpl.create(getAdvancedCache(cacheManagers.get(0), null));
      fmapL2 = FunctionalMapImpl.create(getAdvancedCache(cacheManagers.get(0), null));
      fmapD1 = FunctionalMapImpl.create(getAdvancedCache(cacheManagers.get(0), DIST));
      fmapD2 = FunctionalMapImpl.create(getAdvancedCache(cacheManagers.get(1), DIST));
      fmapR1 = FunctionalMapImpl.create(getAdvancedCache(cacheManagers.get(0), REPL));
      fmapR2 = FunctionalMapImpl.create(getAdvancedCache(cacheManagers.get(1), REPL));
   }

   protected <K, V> AdvancedCache<K, V> getAdvancedCache(EmbeddedCacheManager cm, String cacheName) {
      return (AdvancedCache<K, V>) (cacheName == null ? cm.getCache() : cm.getCache(cacheName));
   }

   protected boolean isSkipSimpleCache() {
      return skipSimpleCache;
   }

   protected void checkSimpleCacheAvailable() {
      if (isSkipSimpleCache())
         throw new SkipException("Skip test because simple cache not available");
   }
}
