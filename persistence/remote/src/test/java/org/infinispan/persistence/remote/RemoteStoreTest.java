package org.infinispan.persistence.remote;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.util.function.Predicate;
import java.util.function.ToIntBiFunction;

import org.infinispan.AdvancedCache;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
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
import org.infinispan.marshall.protostream.impl.MarshallableUserObject;
import org.infinispan.persistence.BaseNonBlockingStoreTest;
import org.infinispan.persistence.internal.PersistenceUtil;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.protostream.ProtobufUtil;
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
   private AdvancedCache<Object, Object> localCache;
   private HotRodServer hrServer;
   private boolean segmented;
   private MediaType cacheMediaType;
   private boolean isRawValues;

   private ProtoStreamMarshaller marshaller = new ProtoStreamMarshaller(ProtobufUtil.newSerializationContext());

   public RemoteStoreTest segmented(boolean segmented) {
      this.segmented = segmented;
      return this;
   }

   public RemoteStoreTest cacheMediaType(MediaType cacheMediaType) {
      this.cacheMediaType = cacheMediaType;
      return this;
   }

   public RemoteStoreTest rawValues(boolean isRawValues) {
      this.isRawValues = isRawValues;
      return this;
   }

   @Factory
   public Object[] factory() {
      return new Object[] {
            new RemoteStoreTest().segmented(false).cacheMediaType(MediaType.APPLICATION_OBJECT).rawValues(true),
            new RemoteStoreTest().segmented(false).cacheMediaType(MediaType.APPLICATION_OBJECT).rawValues(false),
            new RemoteStoreTest().segmented(false).cacheMediaType(MediaType.APPLICATION_PROTOSTREAM).rawValues(true),
            new RemoteStoreTest().segmented(false).cacheMediaType(MediaType.APPLICATION_PROTOSTREAM).rawValues(false),
            new RemoteStoreTest().segmented(true).cacheMediaType(MediaType.APPLICATION_OBJECT).rawValues(true),
            new RemoteStoreTest().segmented(true).cacheMediaType(MediaType.APPLICATION_OBJECT).rawValues(false),
            new RemoteStoreTest().segmented(true).cacheMediaType(MediaType.APPLICATION_PROTOSTREAM).rawValues(true),
            new RemoteStoreTest().segmented(true).cacheMediaType(MediaType.APPLICATION_PROTOSTREAM).rawValues(false),
      };
   }

   @Override
   protected String parameters() {
      return "[" + segmented + ", " + cacheMediaType + ", " + isRawValues + "]";
   }

   @Override
   protected Configuration buildConfig(ConfigurationBuilder cb) {
      cb.memory().maxCount(WRITE_DELETE_BATCH_MAX_ENTRIES)
            .expiration().wakeUpInterval(10L);

      // Unfortunately BaseNonBlockingStore stops and restarts the store, which can start a second hrServer - prevent that
      if (hrServer == null) {
         GlobalConfigurationBuilder globalConfig = new GlobalConfigurationBuilder().clusteredDefault();
         globalConfig.defaultCacheName(REMOTE_CACHE);

         ConfigurationBuilder configurationBuilder = hotRodCacheConfiguration(cb);
         configurationBuilder.encoding().mediaType(cacheMediaType.toString());
         configurationBuilder.clustering().cacheMode(CacheMode.DIST_SYNC);
         localCacheManager = TestCacheManagerFactory.createClusteredCacheManager(
               globalConfig, configurationBuilder);
         TestingUtil.replaceComponent(localCacheManager, TimeService.class, timeService, true);

         localCache = localCacheManager.getCache(REMOTE_CACHE).getAdvancedCache()
               .withMediaType(MediaType.APPLICATION_OBJECT, MediaType.APPLICATION_OBJECT);
         keyPartitioner = localCache.getAdvancedCache().getComponentRegistry().getComponent(KeyPartitioner.class);

         hrServer = HotRodClientTestingUtil.startHotRodServer(localCacheManager);
      }

      // Set it to dist so it has segments
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      cb.encoding().mediaType(cacheMediaType.toString());

      RemoteStoreConfigurationBuilder storeConfigurationBuilder = cb
            .persistence()
            .addStore(RemoteStoreConfigurationBuilder.class)
            .remoteCacheName(REMOTE_CACHE)
            .rawValues(isRawValues);
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
   protected Object keyToStorage(Object key) {
      if (cacheMediaType.equals(MediaType.APPLICATION_PROTOSTREAM)) {
         try {
            return new WrappedByteArray(marshaller.objectToByteBuffer(key));
         } catch (IOException | InterruptedException e) {
            throw new AssertionError(e);
         }
      }
      return super.keyToStorage(key);
   }

   @Override
   protected Object valueToStorage(Object value) {
      return keyToStorage(value);
   }

   @Override
   public void testReplaceExpiredEntry() {
      store.write(marshalledEntry(internalCacheEntry("k1", "v1", 100)));
      // Hot Rod does not support milliseconds, so 100ms is rounded to the nearest second,
      // and so data is stored for 1 second here. Adjust waiting time accordingly.
      timeService.advance(1101);
      Object storedKey = keyToStorage("k1");
      assertNull(store.loadEntry(storedKey));
      long start = System.currentTimeMillis();
      store.write(marshalledEntry(internalCacheEntry("k1", "v2", 100)));
      assertTrue(store.loadEntry(storedKey).getValue().equals(valueToStorage("v2")) ||
            TestingUtil.moreThanDurationElapsed(start, 100));
   }

   void countWithSegments(ToIntBiFunction<NonBlockingStore<Object, Object>, IntSet> countFunction) {
      store.write(marshalledEntry(internalCacheEntry("k1", "v1", 100)));

      int segment = getKeySegment("k1");

      // Publish keys should return our key if we use a set that contains that segment
      assertEquals(1, countFunction.applyAsInt(store, IntSets.immutableSet(segment)));

      // Create int set that includes all segments but the one that maps to the key
      int maxSegments = localCache.getCacheConfiguration().clustering().hash().numSegments();
      IntSet intSet = IntSets.mutableEmptySet(maxSegments);
      for (int i = 0; i < maxSegments; ++i) {
         if (i != segment) {
            intSet.set(i);
         }
      }

      // Publish keys shouldn't return our key since the IntSet doesn't contain our segment
      assertEquals(0, countFunction.applyAsInt(store, intSet));
   }

   int getKeySegment(Object obj) {
      Object key = keyToStorage(obj);
      if (segmented && !isRawValues && cacheMediaType.equals(MediaType.APPLICATION_OBJECT))
         key = new MarshallableUserObject<>(key);
      return keyPartitioner.getSegment(key);
   }

   public void testPublishKeysWithSegments() {
      countWithSegments((salws, intSet) -> {
         IntSet segments;
         Predicate<Object> predicate;
         if (segmented) {
            segments = intSet;
            predicate = null;
         } else {
            segments = null;
            predicate = PersistenceUtil.<Object>combinePredicate(intSet, keyPartitioner, null);
         }
         return Flowable.fromPublisher(salws.publishKeys(segments, predicate))
               .count()
               .blockingGet().intValue();
      });

   }

   public void testPublishEntriesWithSegments() {
      countWithSegments((salws, intSet) -> {
         IntSet segments;
         Predicate<Object> predicate;
         if (segmented) {
            segments = intSet;
            predicate = null;
         } else {
            segments = null;
            predicate = PersistenceUtil.<Object>combinePredicate(intSet, keyPartitioner, null);
         }
         return Flowable.fromPublisher(salws.publishEntries(segments, predicate, false))
               .count()
               .blockingGet().intValue();
      });
   }

   @Override
   @Test(enabled = false)
   public void testLoadAndStoreBytesValues() throws PersistenceException, IOException, InterruptedException {
      // This test messes with the actual types provided which can fail due to different media types
   }
}
