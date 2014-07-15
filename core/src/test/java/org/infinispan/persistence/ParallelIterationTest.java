package org.infinispan.persistence;

import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.manager.PersistenceManagerImpl;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.*;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@Test (groups = "functional", testName = "persistence.ParallelIterationTest")
public abstract class ParallelIterationTest extends SingleCacheManagerTest {

   protected AdvancedLoadWriteStore store;
   protected Executor persistenceExecutor;
   protected StreamingMarshaller sm;

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

   public void testParallelIteration() {
      runIterationTest(numThreads(), persistenceExecutor);
   }

   public void testSequentialIteration() {
      runIterationTest(1, new WithinThreadExecutor());
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

   private void runIterationTest(int numThreads, Executor persistenceExecutor1) {
      int numEntries = insertData();
      final ConcurrentMap<Object, Object> entries = new ConcurrentHashMap<>();
      final ConcurrentHashSet<Thread> threads = new ConcurrentHashSet<>();
      final AtomicBoolean sameKeyMultipleTimes = new AtomicBoolean();
      final CyclicBarrier barrier = new CyclicBarrier(numThreads);
      final AtomicBoolean brokenBarrier = new AtomicBoolean(false);

      store.process(null, new AdvancedCacheLoader.CacheLoaderTask() {
         @Override
         public void processEntry(MarshalledEntry marshalledEntry, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
            Object existing = entries.put(marshalledEntry.getKey(), marshalledEntry.getValue());
            if (threads.add(Thread.currentThread())) {
               try {
                  //in some cases, if the task is fast enough, it may not use all the threads expected
                  //this barrier will ensure that when the thread is used for the first time, it will wait
                  //for the expected number of threads.
                  //this should remove the random failures.
                  barrier.await(1, TimeUnit.MINUTES);
               } catch (BrokenBarrierException | TimeoutException e) {
                  log.warn("Exception occurred while waiting for barrier", e);
                  brokenBarrier.set(true);
               }
            }
            if (existing != null) {
               log.warnf("Already a value present for key %s: %s", marshalledEntry.getKey(), existing);
               sameKeyMultipleTimes.set(true);
            }
         }
      }, persistenceExecutor1, true, true);

      assertFalse(sameKeyMultipleTimes.get());
      assertFalse(brokenBarrier.get());
      for (int i = 0; i < numEntries; i++) {
         assertEquals(entries.get(i), i, "For key" + i);
      }

      assertEquals(threads.size(), numThreads);
   }

   private int insertData() {
      int numEntries = 12000;
      for (int i = 0; i < numEntries; i++) {
         MarshalledEntryImpl me = new MarshalledEntryImpl(i, i, null, sm);
         store.write(me);
      }
      return numEntries;
   }
}
