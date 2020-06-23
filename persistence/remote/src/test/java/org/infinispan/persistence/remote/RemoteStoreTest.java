package org.infinispan.persistence.remote;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Iterator;
import java.util.function.ToIntBiFunction;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.persistence.BaseNonBlockingStoreTest;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.core.Flowable;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "persistence.remote.RemoteStoreTest", groups = "functional")
public class RemoteStoreTest extends BaseNonBlockingStoreTest {

   private static final String REMOTE_CACHE = "remote-cache";
   private EmbeddedCacheManager localCacheManager;
   private HotRodServer hrServer;
   private boolean segmented;

   public RemoteStoreTest segmented(boolean segmented) {
      this.segmented = segmented;
      return this;
   }

   @Factory
   public Object[] factory() {
      return new Object[] {
            new RemoteStoreTest().segmented(false),
            new RemoteStoreTest().segmented(true),
      };
   }

   @Override
   protected String parameters() {
      return "[" + segmented + "]";
   }

   @Override
   protected Configuration buildConfig(ConfigurationBuilder cb) {
      cb.memory().maxCount(WRITE_DELETE_BATCH_MAX_ENTRIES)
            .expiration().wakeUpInterval(10L);

      // Unfortunately BaseNonBlockingStore stops and restarts the store, which can start a second hrServer - prevent that
      if (hrServer == null) {
         GlobalConfigurationBuilder globalConfig = new GlobalConfigurationBuilder().clusteredDefault();
         globalConfig.defaultCacheName(REMOTE_CACHE);

         localCacheManager = TestCacheManagerFactory.createClusteredCacheManager(
               globalConfig, hotRodCacheConfiguration(cb));
         TestingUtil.replaceComponent(localCacheManager, TimeService.class, timeService, true);
         localCacheManager.getCache(REMOTE_CACHE).getAdvancedCache().getComponentRegistry().rewire();

         hrServer = HotRodClientTestingUtil.startHotRodServer(localCacheManager);
         // In case if the server has to unmarshall the value, make sure to use the same marshaller
         hrServer.setMarshaller(getMarshaller());
      }

      // Set it to dist so it has segments
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);

      RemoteStoreConfigurationBuilder storeConfigurationBuilder = cb
            .persistence()
            .addStore(RemoteStoreConfigurationBuilder.class)
            .remoteCacheName(REMOTE_CACHE);
      storeConfigurationBuilder
            .addServer()
            .host(hrServer.getHost())
            .port(hrServer.getPort());

      storeConfigurationBuilder.segmented(segmented);
      storeConfigurationBuilder.shared(true);

      return cb.build();
   }

   @Override
   protected NonBlockingStore createStore() {
      return new RemoteStore();
   }

   @Override
   protected PersistenceMarshaller getMarshaller() {
      return localCacheManager.getCache(REMOTE_CACHE).getAdvancedCache().getComponentRegistry().getPersistenceMarshaller();
   }

   @Override
   @AfterMethod
   public void tearDown() {
      super.tearDown();
      HotRodClientTestingUtil.killServers(hrServer);
      hrServer = null;
      TestingUtil.killCacheManagers(localCacheManager);
   }

   @Override
   protected boolean storePurgesAllExpired() {
      return false;
   }

   @Override
   public void testReplaceExpiredEntry() {
      store.write(marshalledEntry(internalCacheEntry("k1", "v1", 100)));
      // Hot Rod does not support milliseconds, so 100ms is rounded to the nearest second,
      // and so data is stored for 1 second here. Adjust waiting time accordingly.
      timeService.advance(1101);
      assertNull(store.loadEntry("k1"));
      long start = System.currentTimeMillis();
      store.write(marshalledEntry(internalCacheEntry("k1", "v2", 100)));
      assertTrue(store.loadEntry("k1").getValue().equals("v2") || TestingUtil.moreThanDurationElapsed(start, 100));
   }

   void countWithSegments(ToIntBiFunction<NonBlockingStore<?, ?>, IntSet> countFunction) {
      Cache<byte[], byte[]> cache = localCacheManager.<byte[], byte[]>getCache(REMOTE_CACHE).getAdvancedCache().withStorageMediaType();

      store.write(marshalledEntry(internalCacheEntry("k1", "v1", 100)));

      Iterator<byte[]> iter = cache.keySet().iterator();
      assertTrue(iter.hasNext());
      byte[] key = iter.next();
      assertFalse(iter.hasNext());

      KeyPartitioner keyPartitioner = TestingUtil.extractComponent(cache, KeyPartitioner.class);

      int segment = keyPartitioner.getSegment(key);

      // Publish keys should return our key if we use a set that contains that segment
      assertEquals(1, countFunction.applyAsInt(store, IntSets.immutableSet(segment)));

      // Create int set that includes all segments but the one that maps to the key
      int maxSegments = cache.getCacheConfiguration().clustering().hash().numSegments();
      IntSet intSet = IntSets.mutableEmptySet(maxSegments);
      for (int i = 0; i < maxSegments; ++i) {
         if (i != segment) {
            intSet.set(i);
         }
      }

      // Publish keys shouldn't return our key since the IntSet doesn't contain our segment
      assertEquals(0, countFunction.applyAsInt(store, intSet));
   }

   public void testPublishKeysWithSegments() {
      countWithSegments((salws, intSet) ->
         Flowable.fromPublisher(salws.publishKeys(intSet, null))
               .count()
               .blockingGet().intValue()
      );
   }

   public void testPublishEntriesWithSegments() {
      countWithSegments((salws, intSet) ->
            Flowable.fromPublisher(salws.publishEntries(intSet, null, true))
                  .count()
                  .blockingGet().intValue()
      );
   }
}
