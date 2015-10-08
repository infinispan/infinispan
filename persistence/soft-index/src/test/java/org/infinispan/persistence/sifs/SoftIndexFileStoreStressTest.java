package org.infinispan.persistence.sifs;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.InternalEntryFactoryImpl;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.util.DefaultTimeService;
import org.infinispan.util.PersistenceMockUtil;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileFilter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.testng.Assert.fail;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "stress", testName = "persistence.SoftIndexFileStoreStressTest")
public class SoftIndexFileStoreStressTest extends AbstractInfinispanTest {
   protected static final int THREADS = 10;
   protected static final long TEST_DURATION = TimeUnit.MINUTES.toMillis(10);
   protected static final int KEY_RANGE = 1000;

   private TestObjectStreamMarshaller marshaller;
   private InternalEntryFactory factory;
   private SoftIndexFileStore store;
   private String tmpDirectory;
   private ExecutorService executorService;
   private volatile boolean terminate;
   private DefaultTimeService timeService;

   @BeforeMethod(alwaysRun = true)
   public void setUp() throws Exception {
      tmpDirectory = TestingUtil.tmpDirectory(this.getClass());
      if (new File(tmpDirectory).exists()) {
         Stream.of(new File(tmpDirectory).listFiles()).forEach(this::deleteRecursive);
      }
      marshaller = new TestObjectStreamMarshaller();
      factory = new InternalEntryFactoryImpl();
      store = new SoftIndexFileStore();
      ConfigurationBuilder builder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false);
      log.info("Using directory " + tmpDirectory);
      builder.persistence()
            .addStore(SoftIndexFileStoreConfigurationBuilder.class)
            .indexLocation(tmpDirectory).dataLocation(tmpDirectory + "/data")
            .purgeOnStartup(false)
            .maxFileSize(1000);

      timeService = new DefaultTimeService();
      store.init(PersistenceMockUtil.createContext(getClass().getSimpleName(), builder.build(), marshaller, timeService));
      ((InternalEntryFactoryImpl) factory).injectTimeService(timeService);
      store.start();
      executorService = Executors.newFixedThreadPool(THREADS + 1);
   }

   private void deleteRecursive(File f) {
      if (f.isDirectory()) {
         Stream.of(f.listFiles()).forEach(this::deleteRecursive);
      }
      f.delete();
   }

   @AfterMethod
   public void shutdown() {
      store.clear();
      store.stop();
      marshaller.stop();
      executorService.shutdown();
   }

   public void test() throws ExecutionException, InterruptedException {
      terminate = false;
      ArrayList<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < THREADS; ++i) {
         Future<?> future = executorService.submit(new TestThread());
         futures.add(future);
      }
      executorService.submit(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            Thread.sleep(TEST_DURATION);
            terminate = true;
            return null;
         }
      });
      for (Future<?> future : futures) {
         future.get();
      }
      ConcurrentHashMap<Object, MarshalledEntry> entries = new ConcurrentHashMap<>();
      store.process(KeyFilter.ACCEPT_ALL_FILTER, (marshalledEntry, taskContext) -> {
         MarshalledEntry prev = entries.putIfAbsent(marshalledEntry.getKey(), marshalledEntry);
         if (prev != null) {
            fail("Returned entry twice: " + marshalledEntry.getKey() + " -> " + prev.getValue() + ", " + marshalledEntry.getValue());
         }
      }, new WithinThreadExecutor(), true, true);
      store.stop();
      // remove index files
      Stream.of(new File(tmpDirectory).listFiles(file -> !file.isDirectory())).map(f -> f.delete());
      store.start();
      store.process(KeyFilter.ACCEPT_ALL_FILTER, (marshalledEntry, taskContext) -> {
         MarshalledEntry stored = entries.get(marshalledEntry.getKey());
         if (stored == null) {
            fail("Loaded " + marshalledEntry.getKey() + " -> " + marshalledEntry.getValue() + " but it's not in the map");
         } else if (!Objects.equals(stored.getValue(), marshalledEntry.getValue())) {
            fail("Loaded " + marshalledEntry.getKey() + " -> " + marshalledEntry.getValue() + " but it's was " + stored.getValue());
         }
      }, new WithinThreadExecutor(), true, true);
      for (MarshalledEntry stored : entries.values()) {
         MarshalledEntry loaded = store.load(stored.getKey());
         if (loaded == null) {
            if (stored.getMetadata().isExpired(timeService.wallClockTime())) {
               continue;
            }
            fail("Did not load " + stored.getKey() + " -> " + stored.getValue());
         } else if (!Objects.equals(stored.getValue(), loaded.getValue())) {
            fail("Loaded " + stored.getKey() + " -> " + loaded.getValue() + " but it should be " + stored.getValue());
         }
      }
   }

   private class TestThread implements Runnable {
      @Override
      public void run() {
         ThreadLocalRandom random = ThreadLocalRandom.current();
         InternalCacheEntry ice;
         while (!terminate) {
            long lifespan;
            switch (random.nextInt(3)) {
               case 0:
                  lifespan = random.nextInt(3) == 0 ? random.nextInt(10) : - 1;
                  ice = TestInternalCacheEntryFactory.<Object, Object>create(factory,
                     key(random), String.valueOf(random.nextInt()), lifespan);
                  store.write(TestingUtil.marshalledEntry(ice, marshaller));
                  break;
               case 1:
                  store.delete(key(random));
                  break;
               case 2:
                  store.load(key(random));
            }
         }
      }
   }

   protected String key(ThreadLocalRandom random) {
      return "key" + random.nextInt(KEY_RANGE);
   }
}
