package org.infinispan.stress;

import static java.lang.Math.sqrt;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.io.ByteBufferFactoryImpl;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.time.DefaultTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.ProcessorInfo;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.BlockingRejectedExecutionHandler;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.commons.util.concurrent.NonBlockingRejectedExecutionHandler;
import org.infinispan.configuration.cache.SingleFileStoreConfiguration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.container.impl.InternalEntryFactoryImpl;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.ch.impl.SingleSegmentKeyPartitioner;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.impl.TestComponentAccessors;
import org.infinispan.factories.threads.DefaultThreadFactory;
import org.infinispan.factories.threads.NonBlockingThreadFactory;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.marshall.persistence.impl.MarshalledEntryFactoryImpl;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.persistence.DummyInitializationContext;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.async.AsyncNonBlockingStore;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfiguration;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.file.SingleFileStore;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.BlockingManagerImpl;
import org.infinispan.util.concurrent.locks.impl.LockContainer;
import org.infinispan.util.concurrent.locks.impl.PerKeyLockContainer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Async store stress test.
 *
 * // TODO: Add a test to verify clear() too!
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(testName = "stress.AsyncStoreStressTest", groups = "stress")
public class AsyncStoreStressTest extends AbstractInfinispanTest {
   static final Log log = LogFactory.getLog(AsyncStoreStressTest.class);

   static final int CAPACITY = Integer.getInteger("size", 100000);
   static final int LOOP_FACTOR = 10;
   static final long RUNNING_TIME = Integer.getInteger("time", 1) * 60 * 1000;
   static final Random RANDOM = new Random(12345);

   private volatile CountDownLatch latch;
   private List<String> keys = new ArrayList<>();
   private final InternalEntryFactory entryFactory = new InternalEntryFactoryImpl();
   private final Map<Object, InternalCacheEntry> expectedState = new ConcurrentHashMap<Object, InternalCacheEntry>();
   private TestObjectStreamMarshaller marshaller;
   private ExecutorService nonBlockingExecutor;
   private ExecutorService blockingExecutor;
   private TimeService timeService;

   protected String location;

   @BeforeClass(alwaysRun = true)
   void startMarshaller() {
      marshaller = new TestObjectStreamMarshaller();
      location = CommonsTestingUtil.tmpDirectory(this.getClass());

      nonBlockingExecutor = new ThreadPoolExecutor(0, ProcessorInfo.availableProcessors() * 2,
            60L, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(KnownComponentNames.getDefaultQueueSize(KnownComponentNames.NON_BLOCKING_EXECUTOR)),
            new NonBlockingThreadFactory(Thread.NORM_PRIORITY, DefaultThreadFactory.DEFAULT_PATTERN, "Test", "non-blocking"),
            NonBlockingRejectedExecutionHandler.getInstance());
      blockingExecutor = new ThreadPoolExecutor(0, 150,
            60L, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(KnownComponentNames.getDefaultQueueSize(KnownComponentNames.BLOCKING_EXECUTOR)),
            getTestThreadFactory("Blocking"),
            BlockingRejectedExecutionHandler.getInstance());

      PerKeyLockContainer lockContainer = new PerKeyLockContainer();
      TestingUtil.inject(lockContainer, new DefaultTimeService());
      locks = lockContainer;

      timeService = new DefaultTimeService();
      TestingUtil.inject(locks, timeService, nonBlockingExecutor);
   }

   @AfterClass(alwaysRun = true)
   void stopMarshaller() throws InterruptedException {
      marshaller.stop();
      Util.recursiveFileRemove(location);

      if (nonBlockingExecutor != null) {
         nonBlockingExecutor.shutdown();
         nonBlockingExecutor.awaitTermination(10, TimeUnit.SECONDS);
      }

      if (blockingExecutor != null) {
         blockingExecutor.shutdown();
         blockingExecutor.awaitTermination(10, TimeUnit.SECONDS);
      }
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
   private LockContainer locks;

   private AsyncNonBlockingStore<Object, Object> createDummyAsyncStore() {
      DummyInMemoryStoreConfiguration dummyConfiguration = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .segmented(false)
            .storeName("async2")
            .create();

      return createAsyncStore(new DummyInMemoryStore(), dummyConfiguration);
   }

   private AsyncNonBlockingStore<Object, Object> createFileAsyncStore() {
      SingleFileStoreConfiguration singleFileStoreConfiguration = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .persistence()
            .addSingleFileStore()
            .location(location)
            .segmented(false)
            .create();

      return createAsyncStore(new SingleFileStore<>(), singleFileStoreConfiguration);
   }

   private AsyncNonBlockingStore<Object, Object> createAsyncStore(NonBlockingStore backendStore, StoreConfiguration storeConfiguration) throws PersistenceException {

      AsyncNonBlockingStore<Object, Object> store = new AsyncNonBlockingStore<>(backendStore);
      BlockingManager blockingManager = new BlockingManagerImpl();
      TestingUtil.inject(blockingManager,
            new TestComponentAccessors.NamedComponent(KnownComponentNames.NON_BLOCKING_EXECUTOR, nonBlockingExecutor),
            new TestComponentAccessors.NamedComponent(KnownComponentNames.BLOCKING_EXECUTOR, blockingExecutor));
      TestingUtil.startComponent(blockingManager);
      Cache cacheMock = Mockito.mock(Cache.class, Mockito.RETURNS_DEEP_STUBS);
      Mockito.when(ComponentRegistry.componentOf(cacheMock, KeyPartitioner.class))
            .thenReturn(SingleSegmentKeyPartitioner.getInstance());
      CompletionStages.join(store.start(new DummyInitializationContext(storeConfiguration, cacheMock, marshaller, new ByteBufferFactoryImpl(),
            new MarshalledEntryFactoryImpl(marshaller), nonBlockingExecutor,
            new GlobalConfigurationBuilder().globalState().persistentLocation(location).build(), blockingManager, null, new DefaultTimeService())));
      return store;
   }

   private Map<String, AsyncNonBlockingStore<Object, Object>> createAsyncStores() throws PersistenceException {
      Map<String, AsyncNonBlockingStore<Object, Object>> stores = new TreeMap<>();
      stores.put("Dummy-ASYNC", createDummyAsyncStore());
      stores.put("File-ASYNC", createFileAsyncStore());
      return stores;
   }

   @DataProvider(name = "readWriteRemove")
   public Object[][] independentReadWriteRemoveParams() {
      return new Object[][]{
            new Object[]{CAPACITY, 3 * CAPACITY, 9, 20, 1},
            new Object[]{CAPACITY, 3 * CAPACITY, 90, 1, 0},
      };
   }

   @Test(dataProvider = "readWriteRemove")
   public void testReadWriteRemove(int capacity, int numKeys,
         int readerThreads, int writerThreads, int removerThreads) throws Exception {
      System.out.printf("Testing independent read/write/remove performance " +
            "with capacity %d, keys %d, readers %d, writers %d, removers %d\n",
            capacity, numKeys, readerThreads, writerThreads, removerThreads);

      generateKeyList(numKeys);

      Map<String, AsyncNonBlockingStore<Object, Object>> stores = createAsyncStores();
      try {
         for (Map.Entry<String, AsyncNonBlockingStore<Object, Object>> e : stores.entrySet()) {
            mapTestReadWriteRemove(e.getKey(), e.getValue(),  numKeys, readerThreads, writerThreads, removerThreads);
         }
      } finally {
         for (Iterator<AsyncNonBlockingStore<Object, Object>> it = stores.values().iterator(); it.hasNext(); ) {
            AsyncNonBlockingStore<Object, Object> store = it.next();
            try {
               CompletionStages.join(store.stop());
               it.remove();
            } catch (Exception ex) {
               log.error("Failed to stop cache store", ex);
            }
         }
      }
      assertTrue("Not all stores were properly shut down", stores.isEmpty());
   }

   private void mapTestReadWriteRemove(String name, AsyncNonBlockingStore<Object, Object> store,
         int numKeys, int readerThreads, int writerThreads, int removerThreads) throws Exception {
      NonBlockingStore<Object, Object> delegate = store.delegate();
      try {
         // warm up for 1 second
         System.out.printf("[store=%s] Warming up\n", name);
         runMapTestReadWriteRemove(name, store, readerThreads, writerThreads, removerThreads, 1000);

         // real test
         System.out.printf("[store=%s] Testing...\n", name);
         TotalStats perf = runMapTestReadWriteRemove(name, store, readerThreads, writerThreads, removerThreads, RUNNING_TIME);

         // Wait until the cache store contains the expected state
         System.out.printf("[store=%s] Verify contents\n", name);
         eventually(() ->
            "Store contains: " + PersistenceUtil.toKeySet(delegate, IntSets.immutableSet(0), null) + " but expected: " + expectedState.keySet()
         ,() -> PersistenceUtil.toKeySet(delegate, IntSets.immutableSet(0), null).equals(expectedState.keySet()));

         System.out.printf("Container %-12s  ", name);
         System.out.printf("Ops/s %10.2f  ", perf.getTotalOpsPerSec());
         System.out.printf("Gets/s %10.2f  ", perf.getOpsPerSec("GET"));
         System.out.printf("Puts/s %10.2f  ", perf.getOpsPerSec("PUT"));
         System.out.printf("Removes/s %10.2f  ", perf.getOpsPerSec("REMOVE"));
         System.out.printf("HitRatio %10.2f  ", perf.getTotalHitRatio() * 100);
         System.out.printf("Size %10d  ", CompletionStages.join(store.size(IntSets.immutableSet(0))), null);
         double stdDev = computeStdDev(store, numKeys);
         System.out.printf("StdDev %10.2f\n", stdDev);
      } finally {
         // Clean up state, expected state and keys
         expectedState.clear();
         CompletionStages.join(delegate.clear());
      }
   }

   private TotalStats runMapTestReadWriteRemove(String name, final NonBlockingStore<Object, Object> store, int numReaders, int numWriters,
         int numRemovers, final long runningTimeout) throws Exception {
      latch = new CountDownLatch(1);
      final TotalStats perf = new TotalStats();
      List<Thread> threads = new LinkedList<Thread>();

      for (int i = 0; i < numReaders; i++) {
         Thread reader = new WorkerThread("worker-" + name + "-get-" + i, runningTimeout, perf, readOperation(store));
         threads.add(reader);
      }

      for (int i = 0; i < numWriters; i++) {
         Thread writer = new WorkerThread("worker-" + name + "-put-" + i, runningTimeout, perf, writeOperation(store));
         threads.add(writer);
      }

      for (int i = 0; i < numRemovers; i++) {
         Thread remover = new WorkerThread("worker-" + name + "-remove-" + i, runningTimeout, perf, removeOperation(store));
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

   private Operation<String, Integer> readOperation(final NonBlockingStore<Object, Object> store) {
      return new Operation<String, Integer>("GET") {
         @Override
         public boolean call(String key, long run) {
            MarshallableEntry me = CompletionStages.join(store.load(0, key));
            if (log.isTraceEnabled())
               log.tracef("Loaded key=%s, value=%s", key, me != null ? me.getValue() : "null");
            return me != null;
         }
      };
   }

   private Operation<String, Integer> writeOperation(final NonBlockingStore<Object, Object> store) {
      return new Operation<String, Integer>("PUT") {
         @Override
         public boolean call(final String key, long run) {
            final int value = (int) run;
            final InternalCacheEntry entry =
                  entryFactory.create(key, value, new EmbeddedMetadata.Builder().build());
            // Store acquiring locks and catching exceptions
            return withKeyLock(key, () -> {
               CompletionStages.join(store.write(0, MarshalledEntryUtil.create(entry, marshaller)));
               expectedState.put(key, entry);
               if (log.isTraceEnabled())
                  log.tracef("Expected state updated with key=%s, value=%s", key, value);
               return true;
            });
         }
      };
   }

   private Operation<String, Integer> removeOperation(final NonBlockingStore<Object, Object> store) {
      return new Operation<String, Integer>("REMOVE") {
         @Override
         public boolean call(final String key, long run) {
            // Remove acquiring locks and catching exceptions
            return withKeyLock(key, () -> {
               Boolean removed = CompletionStages.join(store.delete(0, key));
               assertNull(removed);
               expectedState.remove(key);
               if (log.isTraceEnabled())
                  log.tracef("Expected state removed key=%s", key);
               return true;
            });
         }
      };
   }

   private boolean withKeyLock(String key, Callable<Boolean> call) {
      boolean result = false;
      try {
         locks.acquire(key, Thread.currentThread(), 5, TimeUnit.SECONDS).lock();
         result = call.call();
      } catch (Exception e) {
         //ignored
         e.printStackTrace();
      } finally {
         locks.release(key, Thread.currentThread());
      }
      return result;
   }

   private double computeStdDev(NonBlockingStore store, int numKeys) throws PersistenceException {
      // The keys closest to the mean are suposed to be accessed more often
      // So we score each map by the standard deviation of the keys in the map
      // at the end of the test
      double variance = 0;
      Set<Object> keys = PersistenceUtil.toKeySet(store, IntSets.immutableSet(0), null);
      for (Object key : keys) {
         double value = Integer.parseInt(((String )key).substring(3));
         variance += (value - numKeys / 2) * (value - numKeys / 2);
      }
      return sqrt(variance / keys.size());
   }

   private class WorkerThread extends Thread {
      private final long runningTimeout;
      private final TotalStats perf;
      private final Operation<String, Integer> op;

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

   private abstract static class Operation<K, V> {
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
      private final ConcurrentHashMap<String, OpStats> statsMap = new ConcurrentHashMap<String, OpStats>();

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
