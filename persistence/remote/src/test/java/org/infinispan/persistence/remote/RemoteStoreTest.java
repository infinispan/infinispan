package org.infinispan.persistence.remote;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.ToIntBiFunction;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.eviction.EvictionType;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.SegmentedAdvancedLoadWriteStore;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.TimeService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import io.reactivex.Flowable;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "persistence.remote.RemoteStoreTest", groups = "functional")
public class RemoteStoreTest extends BaseStoreTest {

   private static final String REMOTE_CACHE = "remote-cache";
   private EmbeddedCacheManager localCacheManager;
   private HotRodServer hrServer;

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {
      ConfigurationBuilder localBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      localBuilder.memory().evictionType(EvictionType.COUNT).size(WRITE_DELETE_BATCH_MAX_ENTRIES).expiration().wakeUpInterval(10L);
      // Set it to dist so it has segments
      localBuilder.clustering().cacheMode(CacheMode.DIST_SYNC);

      GlobalConfigurationBuilder globalConfig = new GlobalConfigurationBuilder().clusteredDefault();
      globalConfig.globalJmxStatistics().defaultCacheName(REMOTE_CACHE);

      localCacheManager = TestCacheManagerFactory.createClusteredCacheManager(
            globalConfig, hotRodCacheConfiguration(localBuilder));
      localCacheManager.getCache(REMOTE_CACHE);
      GlobalComponentRegistry gcr = localCacheManager.getGlobalComponentRegistry();
      gcr.registerComponent(timeService, TimeService.class);
      gcr.rewire();
      localCacheManager.getCache(REMOTE_CACHE).getAdvancedCache().getComponentRegistry().rewire();
      hrServer = HotRodClientTestingUtil.startHotRodServer(localCacheManager);
      // In case if the server has to unmarshall the value, make sure to use the same marshaller
      hrServer.setMarshaller(getMarshaller());

      ConfigurationBuilder builder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false);
      RemoteStoreConfigurationBuilder storeConfigurationBuilder = builder
            .persistence()
               .addStore(RemoteStoreConfigurationBuilder.class)
               .remoteCacheName(REMOTE_CACHE);
      storeConfigurationBuilder
               .addServer()
                  .host(hrServer.getHost())
                  .port(hrServer.getPort());

      RemoteStore remoteStore = new RemoteStore();
      remoteStore.init(createContext(builder.build()));
      return remoteStore;
   }

   @Override
   protected StreamingMarshaller getMarshaller() {
      return localCacheManager.getCache(REMOTE_CACHE).getAdvancedCache().getComponentRegistry().getCacheMarshaller();
   }

   @Override
   @AfterMethod
   public void tearDown() {
      HotRodClientTestingUtil.killServers(hrServer);
      TestingUtil.killCacheManagers(localCacheManager);
   }

   @Override
   protected boolean storePurgesAllExpired() {
      return false;
   }

   @Override
   public void testReplaceExpiredEntry() throws Exception {
      cl.write(marshalledEntry(internalCacheEntry("k1", "v1", 100)));
      // Hot Rod does not support milliseconds, so 100ms is rounded to the nearest second,
      // and so data is stored for 1 second here. Adjust waiting time accordingly.
      timeService.advance(1101);
      assertNull(cl.load("k1"));
      long start = System.currentTimeMillis();
      cl.write(marshalledEntry(internalCacheEntry("k1", "v2", 100)));
      assertTrue(cl.load("k1").getValue().equals("v2") || TestingUtil.moreThanDurationElapsed(start, 100));
   }

   void countWithSegments(ToIntBiFunction<SegmentedAdvancedLoadWriteStore<?, ?>, IntSet> countFunction) {
      Cache<byte[], byte[]> cache = localCacheManager.getCache(REMOTE_CACHE);
      RemoteStore rs = (RemoteStore) cl;

      rs.write(marshalledEntry(internalCacheEntry("k1", "v1", 100)));

      Iterator<byte[]> iter = cache.keySet().iterator();
      assertTrue(iter.hasNext());
      byte[] key = iter.next();
      assertFalse(iter.hasNext());

      KeyPartitioner keyPartitioner = TestingUtil.extractComponent(cache, KeyPartitioner.class);

      int segment = keyPartitioner.getSegment(key);

      // Publish keys should return our key if we use a set that contains that segment
      assertEquals(1, countFunction.applyAsInt(rs, IntSets.immutableSet(segment)));

      // Create int set that includes all segments but the one that maps to the key
      int maxSegments = cache.getCacheConfiguration().clustering().hash().numSegments();
      IntSet intSet = IntSets.mutableEmptySet(maxSegments);
      for (int i = 0; i < maxSegments; ++i) {
         if (i != segment) {
            intSet.set(i);
         }
      }

      // Publish keys shouldn't return our key since the IntSet doesn't contain our segment
      assertEquals(0, countFunction.applyAsInt(rs, intSet));
   }

   public void testPublishKeysWithSegments() throws IOException, InterruptedException {
      countWithSegments((salws, intSet) ->
         Flowable.fromPublisher(salws.publishKeys(intSet, null))
               .count()
               .blockingGet().intValue()
      );
   }

   public void testPublishEntriesWithSegments() throws IOException, InterruptedException {
      countWithSegments((salws, intSet) ->
            Flowable.fromPublisher(salws.publishEntries(intSet, null, true, true))
                  .count()
                  .blockingGet().intValue()
      );
   }
}
