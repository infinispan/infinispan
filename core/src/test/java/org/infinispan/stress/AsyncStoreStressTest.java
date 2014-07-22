package org.infinispan.stress;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.io.ByteBufferFactoryImpl;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.InternalEntryFactoryImpl;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.core.MarshalledEntryFactoryImpl;
import org.infinispan.persistence.DummyInitializationContext;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.async.AdvancedAsyncCacheLoader;
import org.infinispan.persistence.async.AdvancedAsyncCacheWriter;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfiguration;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.AdvancedCacheWriter;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.support.DelegatingCacheLoader;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.concurrent.locks.containers.LockContainer;
import org.infinispan.util.concurrent.locks.containers.ReentrantPerEntryLockContainer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static java.lang.Math.sqrt;
import static org.infinispan.test.TestingUtil.marshalledEntry;
import static org.junit.Assert.assertTrue;

/**
 * Async store stress test.
 *
 * // TODO: Add a test to verify clear() too!
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(testName = "stress.AsyncStoreStressTest", groups = "stress")
public class AsyncStoreStressTest {

   static final Log log = LogFactory.getLog(AsyncStoreStressTest.class);
   static final boolean trace = log.isTraceEnabled();

   static final int CAPACITY = Integer.getInteger("size", 100000);
   static final int LOOP_FACTOR = 10;
   static final long RUNNING_TIME = Integer.getInteger("time", 1) * 60 * 1000;
   static final Random RANDOM = new Random(12345);

   private volatile CountDownLatch latch;
   private List<String> keys = new ArrayList<String>();
   private InternalEntryFactory entryFactory = new InternalEntryFactoryImpl();
   private Map<Object, InternalCacheEntry> expectedState = new ConcurrentHashMap<Object, InternalCacheEntry>();
   private TestObjectStreamMarshaller marshaller;

   @BeforeTest
   void startMarshaller() {
      marshaller = new TestObjectStreamMarshaller();
   }

   @AfterTest
   void stopMarshaller() {
      marshaller.stop();
   }

   // Lock container that mimics per-key locking produced by the cache.
   // This per-key lock holder provides guarantees that the final expected
   // state has not been affected by ordering issues such as this:
   //
   // (Thread-200:) Enqueuing modification Store{storedEntry=
   // ImmortalCacheEntry{key=key165168, value=ImmortalCacheValue {value=60483}}}
   // (Thread-194:) Enqueuing modification Store{storedEntry=
   // ImmortalCacheEntry{key=key165168, value=ImmortalCacheValue {value=61456}}}
   // (Thread-194:) Expected state updated with key=key165168, value=61456
   // (Thread-200:) Expected state updated with key=key165168, value=60483
   private LockContainer locks = new ReentrantPerEntryLockContainer(32, AnyEquivalence.getInstance());

   private Map<String, KeyValuePair<AdvancedAsyncCacheLoader, AdvancedAsyncCacheWriter>> createAsyncStores() throws PersistenceException {
      Map<String, KeyValuePair<AdvancedAsyncCacheLoader, AdvancedAsyncCacheWriter>> stores = new TreeMap<String, KeyValuePair<AdvancedAsyncCacheLoader, AdvancedAsyncCacheWriter>>();
      AdvancedAsyncCacheWriter writer = createAsyncStore();
      AdvancedAsyncCacheLoader loader = new AdvancedAsyncCacheLoader((CacheLoader) writer.undelegate(), writer.getState());
      KeyValuePair<AdvancedAsyncCacheLoader, AdvancedAsyncCacheWriter> pair = new
            KeyValuePair<AdvancedAsyncCacheLoader, AdvancedAsyncCacheWriter>(loader, writer);
      stores.put("ASYNC", pair);
      return stores;
   }

   private AdvancedAsyncCacheWriter createAsyncStore() throws PersistenceException {
      DummyInMemoryStore backendStore = createBackendStore("async2");
      AdvancedAsyncCacheWriter store = new AdvancedAsyncCacheWriter(backendStore);
      store.init(new DummyInitializationContext() {
         @Override
         public StreamingMarshaller getMarshaller() {
            return marshaller;
         }
      });
      store.start();
      return store;
   }

   private DummyInMemoryStore createBackendStore(String storeName) throws PersistenceException {
      DummyInMemoryStore store = new DummyInMemoryStore();
      DummyInMemoryStoreConfiguration dummyConfiguration = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .persistence()
               .addStore(DummyInMemoryStoreConfigurationBuilder.class)
                  .storeName(storeName)
                  .create();
      store.init(new DummyInitializationContext(dummyConfiguration, null, marshaller, new ByteBufferFactoryImpl(),
                                                new MarshalledEntryFactoryImpl(marshaller)));
      store.start();
      return store;
   }

   @DataProvider(name = "readWriteRemove")
   public Object[][] independentReadWriteRemoveParams() {
      return new Object[][]{
            new Object[]{CAPACITY, 3 * CAPACITY, 90, 9, 1},
            new Object[]{CAPACITY, 3 * CAPACITY, 9, 1, 0},
      };
   }

   @Test(dataProvider = "readWriteRemove")
   public void testReadWriteRemove(int capacity, int numKeys,
         int readerThreads, int writerThreads, int removerThreads) throws Exception {
      System.out.printf("Testing independent read/write/remove performance " +
            "with capacity %d, keys %d, readers %d, writers %d, removers %d\n",
            capacity, numKeys, readerThreads, writerThreads, removerThreads);

      generateKeyList(numKeys);

      Map<String, KeyValuePair<AdvancedAsyncCacheLoader, AdvancedAsyncCacheWriter>> stores = createAsyncStores();
      try {
         for (Map.Entry<String, KeyValuePair<AdvancedAsyncCacheLoader, AdvancedAsyncCacheWriter>> e : stores.entrySet()) {
            mapTestReadWriteRemove(e.getKey(), e.getValue().getKey(), e.getValue().getValue(), numKeys,
                  readerThreads, writerThreads, removerThreads);
            e.setValue(null);
         }
      } finally {
         for (Iterator<KeyValuePair<AdvancedAsyncCacheLoader, AdvancedAsyncCacheWriter>> it = stores.values().iterator(); it.hasNext(); ) {
            KeyValuePair<AdvancedAsyncCacheLoader, AdvancedAsyncCacheWriter> store = it.next();
            try {
               store.getKey().stop();
               store.getValue().stop();
               it.remove();
            } catch (Exception ex) {
               log.error("Failed to stop cache store", ex);
            }
         }
      }
      assertTrue("Not all stores were properly shut down", stores.isEmpty());
   }

   private void mapTestReadWriteRemove(String name, AdvancedCacheLoader loader, AdvancedCacheWriter writer,
         int numKeys, int readerThreads, int writerThreads, int removerThreads) throws Exception {
      DummyInMemoryStore delegate = (DummyInMemoryStore) ((DelegatingCacheLoader) loader).undelegate();
      try {
         // warm up for 1 second
         System.out.printf("[store=%s] Warming up\n", name);
         runMapTestReadWriteRemove(name, loader, writer, readerThreads, writerThreads, removerThreads, 1000);

         // real test
         System.out.printf("[store=%s] Testing...\n", name);
         TotalStats perf = runMapTestReadWriteRemove(name, loader, writer, readerThreads, writerThreads, removerThreads, RUNNING_TIME);

         // Wait until the cache store contains the expected state
         System.out.printf("[store=%s] Verify contents\n", name);
         delegate.blockUntilCacheStoreContains(expectedState.keySet(), 60000);

         System.out.printf("Container %-12s  ", name);
         System.out.printf("Ops/s %10.2f  ", perf.getTotalOpsPerSec());
         System.out.printf("Gets/s %10.2f  ", perf.getOpsPerSec("GET"));
         System.out.printf("Puts/s %10.2f  ", perf.getOpsPerSec("PUT"));
         System.out.printf("Removes/s %10.2f  ", perf.getOpsPerSec("REMOVE"));
         System.out.printf("HitRatio %10.2f  ", perf.getTotalHitRatio() * 100);
         System.out.printf("Size %10d  ", PersistenceUtil.count(loader, null));
         double stdDev = computeStdDev(loader, numKeys);
         System.out.printf("StdDev %10.2f\n", stdDev);
      } finally {
         // Clean up state, expected state and keys
         expectedState.clear();
         delegate.clear();
      }
   }

   private TotalStats runMapTestReadWriteRemove(String name, final AdvancedCacheLoader loader, final AdvancedCacheWriter cWriter, int numReaders, int numWriters,
         int numRemovers, final long runningTimeout) throws Exception {
      latch = new CountDownLatch(1);
      final TotalStats perf = new TotalStats();
      List<Thread> threads = new LinkedList<Thread>();

      for (int i = 0; i < numReaders; i++) {
         Thread reader = new WorkerThread("worker-" + name + "-get-" + i, runningTimeout, perf, readOperation(loader));
         threads.add(reader);
      }

      for (int i = 0; i < numWriters; i++) {
         Thread writer = new WorkerThread("worker-" + name + "-put-" + i, runningTimeout, perf, writeOperation(cWriter));
         threads.add(writer);
      }

      for (int i = 0; i < numRemovers; i++) {
         Thread remover = new WorkerThread("worker-" + name + "-remove-" + i, runningTimeout, perf, removeOperation(cWriter));
         threads.add(remover);
      }

      for (Thread t : threads)
         t.start();
      latch.countDown();

      for (Thread t : threads)
         t.join();

      return perf;
   }

   private void generateKeyList(int numKeys) {
      // without this we keep getting OutOfMemoryErrors
      keys = null;
      keys = new ArrayList<String>(numKeys * LOOP_FACTOR);
      for (int i = 0; i < numKeys * LOOP_FACTOR; i++) {
         keys.add("key" + nextIntGaussian(numKeys));
      }
   }

   private int nextIntGaussian(int numKeys) {
      double gaussian = RANDOM.nextGaussian();
      if (gaussian < -3 || gaussian > 3)
         return nextIntGaussian(numKeys);

      return (int) Math.abs((gaussian + 3) * numKeys / 6);
   }

   private void waitForStart() {
      try {
         latch.await();
      } catch (InterruptedException e) {
         throw new RuntimeException(e);
      }
   }

   private Operation<String, Integer> readOperation(final AdvancedCacheLoader store) {
      return new Operation<String, Integer>("GET") {
         @Override
         public boolean call(String key, long run) {
            MarshalledEntry me = store.load(key);
            if (trace)
               log.tracef("Loaded key=%s, value=%s", key, me != null ? me.getValue() : "null");
            return me != null;
         }
      };
   }

   private Operation<String, Integer> writeOperation(final AdvancedCacheWriter store) {
      return new Operation<String, Integer>("PUT") {
         @Override
         public boolean call(final String key, long run) {
            final int value = (int) run;
            final InternalCacheEntry entry =
                  entryFactory.create(key, value, new EmbeddedMetadata.Builder().build());
            // Store acquiring locks and catching exceptions
            boolean result = withStore(key, new Callable<Boolean>() {
               @Override
               public Boolean call() throws Exception {
                  store.write(marshalledEntry(entry, marshaller));
                  expectedState.put(key, entry);
                  if (trace)
                     log.tracef("Expected state updated with key=%s, value=%s", key, value);
                  return true;
               }
            });
            return result;
         }
      };
   }

   private Operation<String, Integer> removeOperation(final AdvancedCacheWriter store) {
      return new Operation<String, Integer>("REMOVE") {
         @Override
         public boolean call(final String key, long run) {
            // Remove acquiring locks and catching exceptions
            boolean result = withStore(key, new Callable<Boolean>() {
               @Override
               public Boolean call() throws Exception {
                  boolean removed = store.delete(key);
                  if (removed) {
                     expectedState.remove(key);
                     if (trace)
                        log.tracef("Expected state removed key=%s", key);
                  }
                  return true;
               }
            });
            return result;
         }
      };
   }
   
   private boolean withStore(String key, Callable<Boolean> call) {
      Lock lock = null;
      boolean result = false;
      try {
         lock = locks.acquireLock(Thread.currentThread(), key, 30, TimeUnit.SECONDS);
         if (lock != null) {
            result = call.call().booleanValue();
         }
      } finally {
         if (lock != null) {
            lock.unlock();
         }
         return result;
      }
   }

   private double computeStdDev(AdvancedCacheLoader store, int numKeys) throws PersistenceException {
      // The keys closest to the mean are suposed to be accessed more often
      // So we score each map by the standard deviation of the keys in the map
      // at the end of the test
      double variance = 0;
      Set<Object> keys = PersistenceUtil.toKeySet(store, null);
      for (Object key : keys) {
         double value = Integer.parseInt(((String )key).substring(3));
         variance += (value - numKeys / 2) * (value - numKeys / 2);
      }
      return sqrt(variance / keys.size());
   }

   private class WorkerThread extends Thread {
      private final long runningTimeout;
      private final TotalStats perf;
      private Operation<String, Integer> op;

      public WorkerThread(String name, long runningTimeout, TotalStats perf, Operation<String, Integer> op) {
         super(name);
         this.runningTimeout = runningTimeout;
         this.perf = perf;
         this.op = op;
      }

      public void run() {
         waitForStart();
         long startMilis = System.currentTimeMillis();
         long endMillis = startMilis + runningTimeout;
         int keyIndex = RANDOM.nextInt(keys.size());
         long runs = 0;
         long missCount = 0;
         while ((runs & 0x3FFF) != 0 || System.currentTimeMillis() < endMillis) {
            boolean hit = op.call(keys.get(keyIndex), runs);
            if (!hit) missCount++;
            keyIndex++;
            runs++;
            if (keyIndex >= keys.size()) {
               keyIndex = 0;
            }
         }
         perf.addStats(op.getName(), runs, System.currentTimeMillis() - startMilis, missCount);
      }
   }

   private static abstract class Operation<K, V> {
      protected final String name;

      public Operation(String name) {
         this.name = name;
      }

      /**
       * @return Return true for a hit, false for a miss.
       */
      public abstract boolean call(K key, long run);

      public String getName() {
         return name;
      }
   }

   private static class TotalStats {
      private ConcurrentHashMap<String, OpStats> statsMap = new ConcurrentHashMap<String, OpStats>();

      public void addStats(String opName, long opCount, long runningTime, long missCount) {
         OpStats s = new OpStats(opName, opCount, runningTime, missCount);
         OpStats old = statsMap.putIfAbsent(opName, s);
         boolean replaced = old == null;
         while (!replaced) {
            old = statsMap.get(opName);
            s = new OpStats(old, opCount, runningTime, missCount);
            replaced = statsMap.replace(opName, old, s);
         }
      }

      public double getOpsPerSec(String opName) {
         OpStats s = statsMap.get(opName);
         if (s == null) return 0;
         return s.opCount * 1000. / s.runningTime * s.threadCount;
      }

      public double getTotalOpsPerSec() {
         long totalOpCount = 0;
         long totalRunningTime = 0;
         long totalThreadCount = 0;
         for (Map.Entry<String, OpStats> e : statsMap.entrySet()) {
            OpStats s = e.getValue();
            totalOpCount += s.opCount;
            totalRunningTime += s.runningTime;
            totalThreadCount += s.threadCount;
         }
         return totalOpCount * 1000. / totalRunningTime * totalThreadCount;
      }

      public double getHitRatio(String opName) {
         OpStats s = statsMap.get(opName);
         if (s == null) return 0;
         return 1 - 1. * s.missCount / s.opCount;
      }

      public double getTotalHitRatio() {
         long totalOpCount = 0;
         long totalMissCount = 0;
         for (Map.Entry<String, OpStats> e : statsMap.entrySet()) {
            OpStats s = e.getValue();
            totalOpCount += s.opCount;
            totalMissCount += s.missCount;
         }
         return 1 - 1. * totalMissCount / totalOpCount;
      }
   }

   private static class OpStats {
      public final String opName;
      public final int threadCount;
      public final long opCount;
      public final long runningTime;
      public final long missCount;

      private OpStats(String opName, long opCount, long runningTime, long missCount) {
         this.opName = opName;
         this.threadCount = 1;
         this.opCount = opCount;
         this.runningTime = runningTime;
         this.missCount = missCount;
      }

      private OpStats(OpStats base, long opCount, long runningTime, long missCount) {
         this.opName = base.opName;
         this.threadCount = base.threadCount + 1;
         this.opCount = base.opCount + opCount;
         this.runningTime = base.runningTime + runningTime;
         this.missCount = base.missCount + missCount;
      }
   }

   @Test(enabled = false) // Disable explicitly to avoid TestNG thinking this is a test!!
   public static void main(String[] args) throws Exception {
      AsyncStoreStressTest test = new AsyncStoreStressTest();
      test.testReadWriteRemove(100000, 300000, 90, 9, 1);
      test.testReadWriteRemove(10000, 30000, 9, 1, 0);
      System.exit(0);
   }

}
