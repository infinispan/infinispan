package org.infinispan.persistence;

import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.manager.PersistenceManagerImpl;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.AdvancedCacheWriter;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * @author Mircea Markus
 * @since 6.0
 */
@Test (groups = "functional", testName = "persistence.ParallelIterationTest")
public abstract class ParallelIterationTest extends SingleCacheManagerTest {

   private static final int NUM_THREADS = 10;
   public static final int NUM_ENTRIES = 200;

   protected AdvancedCacheLoader loader;
   protected AdvancedCacheWriter writer;
   protected Executor executor;
   protected StreamingMarshaller sm;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cb = getDefaultStandaloneCacheConfig(false);
      configurePersistence(cb);
      EmbeddedCacheManager manager = TestCacheManagerFactory.createCacheManager(cb);
      ComponentRegistry componentRegistry = manager.getCache().getAdvancedCache().getComponentRegistry();
      PersistenceManagerImpl pm = (PersistenceManagerImpl) componentRegistry.getComponent(PersistenceManager.class);
      sm = pm.getMarshaller();
      loader = TestingUtil.getFirstLoader(manager.getCache());
      writer = TestingUtil.getFirstWriter(manager.getCache());
      executor = Executors.newFixedThreadPool(NUM_THREADS, getTestThreadFactory("iteration"));
      return manager;
   }

   protected abstract void configurePersistence(ConfigurationBuilder cb);

   public void testParallelIterationWithValueAndMetadata() {
      runIterationTest(executor, true, true);
   }

   public void testParallelIterationWithValueWithoutMetadata() {
      runIterationTest(executor, true, false);
   }

   public void testSequentialIterationWithValueAndMetadata() {
      runIterationTest(new WithinThreadExecutor(), true, true);
   }

   public void testSequentialIterationWithValueWithoutMetadata() {
      runIterationTest(new WithinThreadExecutor(), true, false);
   }

   public void testParallelIterationWithoutValueWithMetadata() {
      runIterationTest(executor, false, true);
   }

   public void testParallelIterationWithoutValueOrMetadata() {
      runIterationTest(executor, false, false);
   }

   public void testSequentialIterationWithoutValueWithMetadata() {
      runIterationTest(new WithinThreadExecutor(), false, true);
   }

   public void testSequentialIterationWithoutValueOrMetadata() {
      runIterationTest(new WithinThreadExecutor(), false, false);
   }

   public void testCancelingTaskMultipleProcessors() {
      insertData();
      final ConcurrentMap<Object, Object> entries = new ConcurrentHashMap<>();
      final AtomicBoolean stopped = new AtomicBoolean(false);

      loader.process(null, new AdvancedCacheLoader.CacheLoaderTask() {
         @Override
         public void processEntry(MarshalledEntry marshalledEntry, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
            synchronized (entries) {
               boolean shouldStop = entries.size() == 100 && !stopped.get();
               log.trace("shouldStop = " + shouldStop + ",entries size = " + entries.size());
               if (shouldStop) {
                  stopped.set(true);
                  taskContext.stop();
                  return;
               }
               entries.put(unwrapKey(marshalledEntry.getKey()), unwrapValue(marshalledEntry.getValue()));
            }
         }
      }, executor, true, true);

      assertTrue(stopped.get());

      assertTrue(entries.size() <= 100 + NUM_THREADS,
            "got " + entries.size() + " elements, expected less than " + (100 + NUM_THREADS));
      assertTrue(entries.size() >= 100);
   }

   private void runIterationTest(Executor persistenceExecutor1, final boolean fetchValues,
         boolean fetchMetadata) {
      final ConcurrentMap<Integer, Integer> entries = new ConcurrentHashMap<>();
      final ConcurrentMap<Integer, InternalMetadata> metadata = new ConcurrentHashMap<>();
      final AtomicBoolean sameKeyMultipleTimes = new AtomicBoolean();
      final AtomicInteger processed = new AtomicInteger();
      final AtomicBoolean brokenBarrier = new AtomicBoolean(false);

      assertEquals(loader.size(), 0);
      insertData();

      loader.process(null, new AdvancedCacheLoader.CacheLoaderTask() {
         @Override
         public void processEntry(MarshalledEntry marshalledEntry, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
            int key = unwrapKey(marshalledEntry.getKey());
            if (fetchValues) {
               // Note: MarshalledEntryImpl.getValue() fails with NPE when it's got null valueBytes,
               // that's why we must not call this when values are not retrieved
               Integer existing = entries.put(key, unwrapValue(marshalledEntry.getValue()));
               if (existing != null) {
                  log.warnf("Already a value present for key %s: %s", key, existing);
                  sameKeyMultipleTimes.set(true);
               }
            }
            if (marshalledEntry.getMetadata() != null) {
               log.tracef("For key %d found metadata %s", key, marshalledEntry.getMetadata());
               InternalMetadata prevMetadata = metadata.put(key, marshalledEntry.getMetadata());
               if (prevMetadata != null) {
                  log.warnf("Already a metadata present for key %s: %s", key, prevMetadata);
                  sameKeyMultipleTimes.set(true);
               }
            } else {
               log.tracef("No metadata found for key %d", key);
            }
            processed.incrementAndGet();
         }
      }, persistenceExecutor1, fetchValues, fetchMetadata);

      assertFalse(sameKeyMultipleTimes.get());
      assertFalse(brokenBarrier.get());
      assertEquals(processed.get(), NUM_ENTRIES);
      for (int i = 0; i < NUM_ENTRIES; i++) {
         if (fetchValues) {
            assertEquals(entries.get(i), (Integer) i, "For key " + i);
         } else {
            assertNull(entries.get(i), "For key " + i);
         }
         if (fetchMetadata && hasMetadata(i)) {
            assertNotNull(metadata.get(i), "For key " + i);
            assertEquals(metadata.get(i).lifespan(), lifespan(i), "For key " + i);
            assertEquals(metadata.get(i).maxIdle(), maxIdle(i), "For key " + i);
         } else {
            assertMetadataEmpty(metadata.get(i));
         }
      }
   }

   private void insertData() {
      for (int i = 0; i < NUM_ENTRIES; i++) {
         MarshalledEntryImpl me = new MarshalledEntryImpl(wrapKey(i), wrapValue(i, i),
               insertMetadata(i) ? TestingUtil.internalMetadata(lifespan(i), maxIdle(i)) : null, sm);
         writer.write(me);
      }
   }

   protected void assertMetadataEmpty(InternalMetadata metadata) {
      assertNull(metadata);
   }

   protected boolean insertMetadata(int i) {
      return i % 2 == 0;
   }

   protected boolean hasMetadata(int i) {
      return insertMetadata(i);
   }

   protected long lifespan(int i) {
      return 1000l * (i + 1000);
   }

   protected long maxIdle(int i) {
      return 10000l * (i + 1000);
   }

   protected Object wrapKey(int key) {
      return key;
   }

   protected Integer unwrapKey(Object key) {
      return (Integer) key;
   }

   protected Object wrapValue(int key, int value) {
      return value;
   }

   protected Integer unwrapValue(Object value) {
      return (Integer) value;
   }
}
