package org.infinispan.persistence;

import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.manager.PersistenceManagerImpl;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.MarshalledEntry;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.*;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@Test (groups = "functional", testName = "persistence.ParallelIterationTest")
public class ParallelIterationTest extends SingleCacheManagerTest {

   protected AdvancedLoadWriteStore store;
   protected ExecutorService persistenceExecutor;
   protected StreamingMarshaller sm;
   protected boolean multipleThreads = true;
   private int numEntries;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cb = getDefaultStandaloneCacheConfig(false);
      configurePersistence(cb);
      EmbeddedCacheManager manager = TestCacheManagerFactory.createCacheManager(cb);
      ComponentRegistry componentRegistry = manager.getCache().getAdvancedCache().getComponentRegistry();
      PersistenceManagerImpl pm = (PersistenceManagerImpl) componentRegistry.getComponent(PersistenceManager.class);
      persistenceExecutor = pm.getPersistenceExecutor();
      sm = pm.getMarshaller();
      store = (AdvancedLoadWriteStore) TestingUtil.getFirstWriter(manager.getCache());
      return manager;
   }

   protected void configurePersistence(ConfigurationBuilder cb) {
      cb.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
   }

   public void testParallelIteration() {
      runIterationTest(numThreads(), persistenceExecutor);
   }

   public void testSequentialIteration() {
      runIterationTest(1, new WithinThreadExecutor());
   }

   public void testCancelingTaskMultipleProcessors() {
      insertData();
      final ConcurrentMap entries = new ConcurrentHashMap();
      final AtomicBoolean stopped = new AtomicBoolean(false);

      store.process(null, new AdvancedCacheLoader.CacheLoaderTask() {
         @Override
         public void processEntry(MarshalledEntry marshalledEntry, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
            synchronized (entries) {
               boolean shouldStop = entries.size() == 100 && !stopped.get();
               log.info("shouldStop = " + shouldStop);
               if (shouldStop) {
                  stopped.set(true);
                  taskContext.stop();
                  return;
               }
               entries.put(marshalledEntry.getKey(), marshalledEntry.getValue());
            }
         }
      }, persistenceExecutor, true, true);

      assertTrue(stopped.get());

      assertTrue(entries.size() <= 100 + numThreads(), "got " + entries.size() + " elements, expected less than " +
            (100 + numThreads()));
      assertTrue(entries.size() >= 100);
   }

   private void runIterationTest(int numThreads, ExecutorService persistenceExecutor1) {
      int numEntries = insertData();
      final ConcurrentMap entries = new ConcurrentHashMap();
      final ConcurrentMap threads = new ConcurrentHashMap();
      final AtomicBoolean sameKeyMultipleTimes = new AtomicBoolean();

      store.process(null, new AdvancedCacheLoader.CacheLoaderTask() {
         @Override
         public void processEntry(MarshalledEntry marshalledEntry, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
            Object existing = entries.put(marshalledEntry.getKey(), marshalledEntry.getValue());
            threads.put(Thread.currentThread(), Thread.currentThread());
            if (existing != null) {
               log.warnf("Already a value present for key %s: %s", marshalledEntry.getKey(), existing);
               sameKeyMultipleTimes.set(true);
            }
         }
      }, persistenceExecutor1, true, true);

      assertFalse(sameKeyMultipleTimes.get());
      for (int i = 0; i < numEntries; i++) {
         assertEquals(entries.get(i), i, "For key" + i);
      }

      if (multipleThreads) {
         assertEquals(threads.size(), numThreads);
      }
   }

   private int insertData() {
      numEntries = 12000;
      for (int i = 0; i < numEntries; i++) {
         MarshalledEntryImpl me = new MarshalledEntryImpl(i, i, null, sm);
         store.write(me);
      }
      return numEntries;
   }

   protected int numThreads() {
      return 1;
   }
}
