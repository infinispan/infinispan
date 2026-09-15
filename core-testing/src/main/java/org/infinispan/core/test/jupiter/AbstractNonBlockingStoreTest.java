package org.infinispan.core.test.jupiter;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.infinispan.commons.io.ByteBufferFactoryImpl;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.marshall.persistence.impl.MarshalledEntryFactoryImpl;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.InitializationContextImpl;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.spi.NonBlockingStore.Characteristic;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.NonBlockingManager;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Abstract JUnit 5 test suite for {@link NonBlockingStore} implementations.
 * <p>
 * Subclasses provide their store via {@link #createStore()} and configure it
 * via {@link #buildConfig(ConfigurationBuilder)}. The test suite validates
 * the core store contract: CRUD, expiration, bulk operations, and segmentation.
 * <p>
 * Example:
 * <pre>{@code
 * class MyStoreTest extends AbstractNonBlockingStoreTest {
 *
 *    @Override
 *    protected NonBlockingStore<Object, Object> createStore() {
 *       return new MyStore();
 *    }
 *
 *    @Override
 *    protected Configuration buildConfig(ConfigurationBuilder builder) {
 *       builder.persistence()
 *             .addStore(CustomStoreConfigurationBuilder.class)
 *             .customStoreClass(MyStore.class);
 *       return builder.build();
 *    }
 * }
 * }</pre>
 *
 * @since 16.2
 */
public abstract class AbstractNonBlockingStoreTest {

   private EmbeddedCacheManager cacheManager;
   protected NonBlockingStore<Object, Object> store;
   protected ControlledTimeService timeService;
   @SuppressWarnings("rawtypes")
   protected MarshallableEntryFactory entryFactory;
   protected PersistenceMarshaller marshaller;
   protected KeyPartitioner keyPartitioner;
   protected int segmentCount;
   protected IntSet segments;
   protected Set<Characteristic> characteristics;
   protected Configuration configuration;

   /**
    * Creates the store instance to test. Called once per test method.
    */
   protected abstract NonBlockingStore<Object, Object> createStore();

   /**
    * Configures the store within the given builder. Must add at least one store
    * configuration to the persistence section.
    *
    * @param builder the configuration builder with clustering.hash.numSegments pre-set
    * @return the built configuration
    */
   protected abstract Configuration buildConfig(ConfigurationBuilder builder);

   @BeforeEach
   void setUp() throws Exception {
      timeService = new ControlledTimeService();
      segmentCount = 256;
      keyPartitioner = k -> Math.abs(k.hashCode() % segmentCount);
      segments = IntSets.immutableRangeSet(segmentCount);

      // Build store configuration
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().hash().numSegments(segmentCount);
      configuration = buildConfig(builder);

      // Create a cache manager for component extraction
      cacheManager = new DefaultCacheManager(new ConfigurationBuilderHolder(), true);
      replaceTimeService(cacheManager, timeService);

      // Define a cache with the correct segment count (no persistence)
      ConfigurationBuilder cacheBuilder = new ConfigurationBuilder();
      cacheBuilder.clustering().hash().numSegments(segmentCount);
      cacheManager.defineConfiguration("store-test", cacheBuilder.build());
      var cache = cacheManager.getCache("store-test");

      // Extract marshaller and managers
      GlobalComponentRegistry gcr = GlobalComponentRegistry.of(cacheManager);
      BasicComponentRegistry bcr = gcr.getComponent(BasicComponentRegistry.class);
      marshaller = bcr.getComponent(KnownComponentNames.PERSISTENCE_MARSHALLER,
            PersistenceMarshaller.class).running();
      entryFactory = new MarshalledEntryFactoryImpl(marshaller);

      BlockingManager blockingManager = gcr.getComponent(BlockingManager.class);
      NonBlockingManager nonBlockingManager = gcr.getComponent(NonBlockingManager.class);

      // Create and start the store
      store = createStore();

      var storeConfig = configuration.persistence().stores().get(0);
      InitializationContext ctx = new InitializationContextImpl(
            storeConfig, cache, keyPartitioner, marshaller, timeService,
            new ByteBufferFactoryImpl(), entryFactory,
            new WithinThreadExecutor(),
            cacheManager.getCacheManagerConfiguration(),
            blockingManager, nonBlockingManager);

      join(store.start(ctx));
      characteristics = store.characteristics();
   }

   @AfterEach
   void tearDown() {
      if (store != null) {
         try {
            join(store.clear());
            join(store.stop());
         } catch (Exception ignored) {
         }
      }
      if (cacheManager != null) {
         cacheManager.stop();
      }
   }

   // ---- Test Methods ----

   @Test
   void testLoadAndStoreImmortal() {
      assertStoreIsEmpty();

      write("k1", "v1");
      MarshallableEntry<Object, Object> entry = load("k1");
      assertThat(entry).isNotNull();
      assertThat(entry.getValue()).isEqualTo("v1");
      assertThat(entry.getMetadata() == null || entry.expiryTime() == -1)
            .as("Expected an immortal entry").isTrue();

      assertContains("k1", true);
      assertContains("k2", false);
   }

   @Test
   void testStoreAndRemove() {
      assertStoreIsEmpty();

      write("k1", "v1");
      write("k2", "v2");
      write("k3", "v3");

      assertThat(allEntries()).hasSize(3);

      delete("k1");
      delete("k2");

      assertThat(allEntries()).hasSize(1);
      assertThat(load("k3")).isNotNull();
      assertThat(load("k3").getValue()).isEqualTo("v3");
      assertContains("k1", false);
      assertContains("k2", false);
   }

   @Test
   void testReplaceEntry() {
      assertStoreIsEmpty();

      write("k1", "v1");
      assertThat(load("k1").getValue()).isEqualTo("v1");

      write("k1", "v2");
      assertThat(load("k1").getValue()).isEqualTo("v2");
   }

   @Test
   void testClear() {
      write("k1", "v1");
      write("k2", "v2");
      assertThat(allEntries()).hasSize(2);

      join(store.clear());
      assertStoreIsEmpty();
   }

   @Test
   void testLoadAll() {
      if (!characteristics.contains(Characteristic.BULK_READ)) return;

      assertStoreIsEmpty();
      write("k1", "v1");
      write("k2", "v2");
      write("k3", "v3");
      write("k4", "v4");
      write("k5", "v5");

      assertThat(allEntries()).hasSize(5);

      // Filter out k3
      List<MarshallableEntry<Object, Object>> filtered = publishEntries(k -> !"k3".equals(k));
      assertThat(filtered).hasSize(4);
      assertThat(filtered).noneMatch(e -> "k3".equals(e.getKey()));
   }

   @Test
   void testApproximateSize() {
      assertStoreIsEmpty();

      write("k1", "v1");
      write("k2", "v2");
      write("k3", "v3");

      long size = join(store.approximateSize(segments));
      assertThat(size).isEqualTo(3);
   }

   @Test
   void testSize() {
      if (!characteristics.contains(Characteristic.BULK_READ)) return;

      assertStoreIsEmpty();

      write("k1", "v1");
      write("k2", "v2");

      long size = join(store.size(segments));
      assertThat(size).isEqualTo(2);
   }

   @Test
   void testIsAvailable() {
      assertThat(join(store.isAvailable())).isTrue();
   }

   @Test
   void testLoadAndStoreWithLifespan() {
      if (!characteristics.contains(Characteristic.EXPIRATION)) return;

      assertStoreIsEmpty();

      long lifespan = 120_000;
      write("k1", "v1", lifespan);

      assertContains("k1", true);
      MarshallableEntry<Object, Object> entry = load("k1");
      assertThat(entry).isNotNull();
      assertThat(entry.getValue()).isEqualTo("v1");
      assertThat(entry.getMetadata().lifespan()).isEqualTo(lifespan);

      // Advance past lifespan
      timeService.advance(lifespan + 1);
      assertThat(load("k1")).isNull();
      assertContains("k1", false);
   }

   @Test
   void testLoadAndStoreWithMaxIdle() {
      if (!characteristics.contains(Characteristic.EXPIRATION)) return;

      assertStoreIsEmpty();

      long maxIdle = 120_000;
      write("k1", "v1", -1, maxIdle);

      assertContains("k1", true);
      MarshallableEntry<Object, Object> entry = load("k1");
      assertThat(entry).isNotNull();
      assertThat(entry.getValue()).isEqualTo("v1");
      assertThat(entry.getMetadata().maxIdle()).isEqualTo(maxIdle);

      // Advance past max idle
      timeService.advance(maxIdle + 1);
      assertThat(load("k1")).isNull();
      assertContains("k1", false);
   }

   @Test
   void testPurgeExpired() {
      if (!characteristics.contains(Characteristic.EXPIRATION)) return;

      assertStoreIsEmpty();

      long lifespan = 5000;
      write("k1", "v1", lifespan);
      write("k2", "v2", -1); // immortal
      write("k3", "v3", lifespan);

      assertContains("k1", true);
      assertContains("k2", true);
      assertContains("k3", true);

      timeService.advance(lifespan + 1);

      // Expired entries should not be visible
      assertContains("k1", false);
      assertContains("k2", true);
      assertContains("k3", false);

      // Purge and verify
      List<MarshallableEntry<Object, Object>> purged = join(Flowable.fromPublisher(store.purgeExpired())
            .toList().toCompletionStage());
      assertThat(purged).hasSize(2);
      Set<Object> purgedKeys = purged.stream()
            .map(MarshallableEntry::getKey).collect(Collectors.toSet());
      assertThat(purgedKeys).containsExactlyInAnyOrder("k1", "k3");

      // Immortal entry remains
      assertThat(load("k2")).isNotNull();
   }

   @Test
   void testStopStartDoesNotNukeValues() {
      assertStoreIsEmpty();

      write("k1", "v1");
      write("k2", "v2");

      // Stop and restart
      join(store.stop());
      join(store.start(lastInitializationContext()));

      MarshallableEntry<Object, Object> entry = load("k1");
      assertThat(entry).isNotNull();
      assertThat(entry.getValue()).isEqualTo("v1");
      assertThat(load("k2")).isNotNull();
   }

   @Test
   void testReplaceExpiredEntry() {
      if (!characteristics.contains(Characteristic.EXPIRATION)) return;

      assertStoreIsEmpty();

      long lifespan = 3000;
      write("k1", "v1", lifespan);
      assertThat(load("k1").getValue()).isEqualTo("v1");

      timeService.advance(lifespan + 1);
      assertThat(load("k1")).isNull();

      // Write new value
      write("k1", "v2", lifespan);
      assertThat(load("k1").getValue()).isEqualTo("v2");
   }

   @Test
   void testRemoveSegments() {
      if (!characteristics.contains(Characteristic.SEGMENTABLE)) return;
      if (segmentCount <= 1) return;

      // Write to segment 0
      join(store.write(0, marshalledEntry("k1", "v1")));

      assertThat(join(store.size(segments))).isEqualTo(1);

      // Remove all segments except 0
      join(store.removeSegments(IntSets.immutableOffsetIntSet(1, segmentCount)));

      // Entry in segment 0 should still be present
      assertThat(join(store.size(IntSets.immutableSet(0)))).isEqualTo(1);
   }

   // ---- Helper Methods ----

   /**
    * Returns the last {@link InitializationContext} used to start the store.
    * Useful for stop/start tests. Subclasses can override to customize.
    */
   protected InitializationContext lastInitializationContext() {
      var cache = cacheManager.getCache("store-test");
      var storeConfig = configuration.persistence().stores().get(0);
      GlobalComponentRegistry gcr = GlobalComponentRegistry.of(cacheManager);
      return new InitializationContextImpl(
            storeConfig, cache, keyPartitioner, marshaller, timeService,
            new ByteBufferFactoryImpl(), entryFactory,
            new WithinThreadExecutor(),
            cacheManager.getCacheManagerConfiguration(),
            gcr.getComponent(BlockingManager.class),
            gcr.getComponent(NonBlockingManager.class));
   }

   @SuppressWarnings("unchecked")
   protected MarshallableEntry<Object, Object> marshalledEntry(Object key, Object value) {
      return entryFactory.create(key, value);
   }

   @SuppressWarnings("unchecked")
   protected MarshallableEntry<Object, Object> marshalledEntry(Object key, Object value, long lifespan) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, TimeUnit.MILLISECONDS)
            .build();
      long now = timeService.wallClockTime();
      return entryFactory.create(key, value, metadata, null, now, now);
   }

   @SuppressWarnings("unchecked")
   protected MarshallableEntry<Object, Object> marshalledEntry(Object key, Object value,
                                                               long lifespan, long maxIdle) {
      EmbeddedMetadata.Builder builder = new EmbeddedMetadata.Builder();
      if (lifespan > 0) builder.lifespan(lifespan, TimeUnit.MILLISECONDS);
      if (maxIdle > 0) builder.maxIdle(maxIdle, TimeUnit.MILLISECONDS);
      Metadata metadata = builder.build();
      long now = timeService.wallClockTime();
      return entryFactory.create(key, value, metadata, null, now, now);
   }

   protected void write(Object key, Object value) {
      int segment = keyPartitioner.getSegment(key);
      join(store.write(segment, marshalledEntry(key, value)));
   }

   protected void write(Object key, Object value, long lifespan) {
      int segment = keyPartitioner.getSegment(key);
      join(store.write(segment, marshalledEntry(key, value, lifespan)));
   }

   protected void write(Object key, Object value, long lifespan, long maxIdle) {
      int segment = keyPartitioner.getSegment(key);
      join(store.write(segment, marshalledEntry(key, value, lifespan, maxIdle)));
   }

   protected MarshallableEntry<Object, Object> load(Object key) {
      int segment = keyPartitioner.getSegment(key);
      return join(store.load(segment, key));
   }

   protected boolean delete(Object key) {
      int segment = keyPartitioner.getSegment(key);
      return join(store.delete(segment, key));
   }

   protected void assertContains(Object key, boolean expected) {
      int segment = keyPartitioner.getSegment(key);
      assertThat(join(store.containsKey(segment, key)))
            .as("containsKey(%s)", key)
            .isEqualTo(expected);
   }

   @SuppressWarnings("unchecked")
   protected List<MarshallableEntry<Object, Object>> allEntries() {
      return (List) join(Flowable.fromPublisher(store.publishEntries(segments, null, true))
            .toList().toCompletionStage());
   }

   @SuppressWarnings("unchecked")
   protected List<MarshallableEntry<Object, Object>> publishEntries(java.util.function.Predicate<Object> filter) {
      return (List) join(Flowable.fromPublisher(store.publishEntries(segments, filter, true))
            .toList().toCompletionStage());
   }

   protected void assertStoreIsEmpty() {
      assertThat(allEntries()).isEmpty();
   }

   protected static <T> T join(CompletionStage<T> stage) {
      try {
         return stage.toCompletableFuture().get(30, TimeUnit.SECONDS);
      } catch (ExecutionException e) {
         Throwable cause = e.getCause();
         if (cause instanceof RuntimeException re) throw re;
         throw new RuntimeException(cause);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new RuntimeException(e);
      } catch (TimeoutException e) {
         throw new RuntimeException("Operation timed out", e);
      }
   }

   private static void replaceTimeService(EmbeddedCacheManager manager, TimeService timeService) {
      GlobalComponentRegistry gcr = GlobalComponentRegistry.of(manager);
      BasicComponentRegistry bcr = gcr.getComponent(BasicComponentRegistry.class);
      bcr.replaceComponent(TimeService.class.getName(), timeService, true);
      gcr.rewire();
   }
}
