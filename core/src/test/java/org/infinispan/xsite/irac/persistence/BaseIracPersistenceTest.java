package org.infinispan.xsite.irac.persistence;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.container.versioning.irac.TopologyIracVersion;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.persistence.KeyValueWrapper;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.support.WaitNonBlockingStore;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.XSiteNamedCache;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import net.jcip.annotations.GuardedBy;

/**
 * An abstract IRAC test to make sure the {@link IracMetadata} is properly stored and retrieved from persistence.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Test(groups = "functional")
public abstract class BaseIracPersistenceTest<V> extends SingleCacheManagerTest {

   private static final AtomicLong V_GENERATOR = new AtomicLong();
   private static final String SITE = "LON";
   private final KeyValueWrapper<String, String, V> keyValueWrapper;
   protected String tmpDirectory;
   protected WaitNonBlockingStore<String, V> cacheStore;
   protected int segmentCount;
   protected MarshallableEntryFactory<String, V> entryFactory;

   protected BaseIracPersistenceTest(
         KeyValueWrapper<String, String, V> keyValueWrapper) {
      this.keyValueWrapper = keyValueWrapper;
   }

   public void testWriteAndPublisher(Method method) {
      String key = TestingUtil.k(method);
      String value = TestingUtil.v(method);
      IracMetadata metadata = createMetadata();

      cacheStore.write(createEntry(key, value, metadata));

      MarshallableEntrySubscriber<V> subscriber = new MarshallableEntrySubscriber<>();
      cacheStore.publishEntries(IntSets.immutableRangeSet(segmentCount), key::equals, true).subscribe(subscriber);

      List<MarshallableEntry<String, V>> entries = subscriber.cf.join();
      assertEquals(1, entries.size());
      assertCorrectEntry(entries.get(0), key, value, metadata);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder gBuilder = createGlobalConfigurationBuilder();
      ConfigurationBuilder cBuilder = new ConfigurationBuilder();
      configure(cBuilder);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(gBuilder, cBuilder);
      cacheStore = TestingUtil.getFirstStoreWait(cm.getCache());
      segmentCount = cm.getCache().getCacheConfiguration().clustering().hash().numSegments();
      //noinspection unchecked
      entryFactory = TestingUtil.extractComponent(cm.getCache(), MarshallableEntryFactory.class);
      return cm;
   }

   @Override
   protected void teardown() {
      super.teardown();
      cacheStore = null;
      entryFactory = null;
   }

   protected abstract void configure(ConfigurationBuilder builder);

   public void testWriteAndLoad(Method method) {
      String key = TestingUtil.k(method);
      String value = TestingUtil.v(method);
      IracMetadata metadata = createMetadata();

      cacheStore.write(createEntry(key, value, metadata));
      MarshallableEntry<String, V> loadedMEntry = cacheStore.loadEntry(key);
      assertCorrectEntry(loadedMEntry, key, value, metadata);
   }

   private GlobalConfigurationBuilder createGlobalConfigurationBuilder() {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder().nonClusteredDefault();
      builder.globalState().enable().persistentLocation(tmpDirectory);
      builder.serialization().addContextInitializer(getSerializationContextInitializer());
      return builder;
   }

   @BeforeClass(alwaysRun = true)
   @Override
   protected void createBeforeClass() throws Exception {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(getClass());
      Util.recursiveFileRemove(tmpDirectory);
      boolean created = new File(tmpDirectory).mkdirs();
      log.debugf("Created temporary directory %s (exists? %s)", tmpDirectory, !created);
      super.createBeforeClass();
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroyAfterClass() {
      super.destroyAfterClass();
      Util.recursiveFileRemove(tmpDirectory);
   }

   protected SerializationContextInitializer getSerializationContextInitializer() {
      return TestDataSCI.INSTANCE;
   }

   private void assertCorrectEntry(MarshallableEntry<String, V> entry, String key, String value,
         IracMetadata metadata) {
      assertNotNull(entry);
      assertEquals(key, entry.getKey());
      assertEquals(value, keyValueWrapper.unwrap(entry.getValue()));
      PrivateMetadata internalMetadata = entry.getInternalMetadata();
      assertNotNull(internalMetadata);
      IracMetadata storedMetadata = entry.getInternalMetadata().iracMetadata();
      assertEquals(metadata, storedMetadata);
   }

   private MarshallableEntry<String, V> createEntry(String key, String value, IracMetadata metadata) {
      return entryFactory.create(key, keyValueWrapper.wrap(key, value), null, wrapInternalMetadata(metadata), -1, -1);
   }

   private static IracMetadata createMetadata() {
      TopologyIracVersion version = TopologyIracVersion.create(1, V_GENERATOR.incrementAndGet());
      ByteString site = XSiteNamedCache.cachedByteString(SITE);
      return new IracMetadata(site, IracEntryVersion.newVersion(site, version));
   }

   private static PrivateMetadata wrapInternalMetadata(IracMetadata metadata) {
      return new PrivateMetadata.Builder()
            .iracMetadata(metadata)
            .build();
   }

   private static class MarshallableEntrySubscriber<V> implements Subscriber<MarshallableEntry<String, V>> {

      @GuardedBy("this")
      private final List<MarshallableEntry<String, V>> entries = new ArrayList<>(1);
      private final CompletableFuture<List<MarshallableEntry<String, V>>> cf = new CompletableFuture<>();

      @Override
      public void onSubscribe(Subscription subscription) {
         subscription.request(Long.MAX_VALUE);
      }

      @Override
      public synchronized void onNext(MarshallableEntry<String, V> entry) {
         entries.add(entry);
      }

      @Override
      public void onError(Throwable throwable) {
         cf.completeExceptionally(throwable);
      }

      @Override
      public synchronized void onComplete() {
         cf.complete(entries);
      }
   }

}
