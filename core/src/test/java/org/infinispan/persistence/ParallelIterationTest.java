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
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
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

   protected AdvancedLoadWriteStore store;
   protected Executor persistenceExecutor;
   protected StreamingMarshaller sm;
   protected boolean multipleThreads = true;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cb = getDefaultStandaloneCacheConfig(false);
      configurePersistence(cb);
      EmbeddedCacheManager manager = TestCacheManagerFactory.createCacheManager(cb);
      ComponentRegistry componentRegistry = manager.getCache().getAdvancedCache().getComponentRegistry();
      PersistenceManagerImpl pm = (PersistenceManagerImpl) componentRegistry.getComponent(PersistenceManager.class);
      persistenceExecutor = pm.getPersistenceExecutor();
      sm = pm.getMarshaller();
      store = TestingUtil.getFirstWriter(manager.getCache());
      return manager;
   }

   protected abstract void configurePersistence(ConfigurationBuilder cb);

   protected abstract int numThreads();

   public void testParallelIterationWithValueAndMetadata() {
      runIterationTest(numThreads(), persistenceExecutor, true, true);
   }

   public void testParallelIterationWithValueWithoutMetadata() {
      runIterationTest(numThreads(), persistenceExecutor, true, false);
   }

   public void testSequentialIterationWithValueAndMetadata() {
      runIterationTest(1, new WithinThreadExecutor(), true, true);
   }

   public void testSequentialIterationWithValueWithoutMetadata() {
      runIterationTest(1, new WithinThreadExecutor(), true, false);
   }

   public void testParallelIterationWithoutValueWithMetadata() {
      runIterationTest(numThreads(), persistenceExecutor, false, true);
   }

   public void testParallelIterationWithoutValueOrMetadata() {
      runIterationTest(numThreads(), persistenceExecutor, false, false);
   }

   public void testSequentialIterationWithoutValueWithMetadata() {
      runIterationTest(1, new WithinThreadExecutor(), false, true);
   }

   public void testSequentialIterationWithoutValueOrMetadata() {
      runIterationTest(1, new WithinThreadExecutor(), false, false);
   }

   public void testCancelingTaskMultipleProcessors() {
      insertData();
      final ConcurrentMap<Object, Object> entries = new ConcurrentHashMap<>();
      final AtomicBoolean stopped = new AtomicBoolean(false);

      store.process(null, new AdvancedCacheLoader.CacheLoaderTask() {
         @Override
         public void processEntry(MarshalledEntry marshalledEntry, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
            synchronized (entries) {
               boolean shouldStop = entries.size() == 100 && !stopped.get();
               log.info("shouldStop = " + shouldStop + ",entries size = " + entries.size());
               if (shouldStop) {
                  stopped.set(true);
                  taskContext.stop();
                  return;
               }
               entries.put(unwrapKey(marshalledEntry.getKey()), unwrapValue(marshalledEntry.getValue()));
            }
         }
      }, persistenceExecutor, true, true);

      assertTrue(stopped.get());

      assertTrue(entries.size() <= 100 + numThreads(), "got " + entries.size() + " elements, expected less than " +
            (100 + numThreads()));
      assertTrue(entries.size() >= 100);
   }

   private void runIterationTest(int numThreads, Executor persistenceExecutor1, final boolean fetchValues, boolean fetchMetadata) {
      assertEquals(store.size(), 0);
      int numEntries = insertData();
      final ConcurrentMap<Integer, Integer> entries = new ConcurrentHashMap<>();
      final ConcurrentMap<Integer, InternalMetadata> metadata = new ConcurrentHashMap<>();
      final AtomicBoolean sameKeyMultipleTimes = new AtomicBoolean();
      final AtomicInteger processed = new AtomicInteger();
      final CyclicBarrier barrier = new CyclicBarrier(numThreads);
      final AtomicBoolean brokenBarrier = new AtomicBoolean(false);

      store.process(null, new AdvancedCacheLoader.CacheLoaderTask() {
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
               log.tracef("For key %d found metdata %s", key, marshalledEntry.getMetadata());
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
      assertEquals(processed.get(), numEntries);
      for (int i = 0; i < numEntries; i++) {
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

   private int insertData() {
      int numEntries = 200;
      for (int i = 0; i < numEntries; i++) {
         MarshalledEntryImpl me = new MarshalledEntryImpl(wrapKey(i), wrapValue(i, i),
               insertMetadata(i) ? TestingUtil.internalMetadata(lifespan(i), maxIdle(i)) : null, sm);
         store.write(me);
      }
      return numEntries;
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
