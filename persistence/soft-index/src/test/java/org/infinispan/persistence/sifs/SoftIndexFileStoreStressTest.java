package org.infinispan.persistence.sifs;

import static org.testng.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.container.impl.InternalEntryFactoryImpl;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.util.EmbeddedTimeService;
import org.infinispan.util.PersistenceMockUtil;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "stress", testName = "persistence.SoftIndexFileStoreStressTest", timeOut = 15*60*1000)
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
   private EmbeddedTimeService timeService;

   @BeforeMethod(alwaysRun = true)
   public void setUp() throws Exception {
      tmpDirectory = TestingUtil.tmpDirectory(this.getClass());
      Util.recursiveFileRemove(tmpDirectory);
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

      timeService = new EmbeddedTimeService();
      store.init(PersistenceMockUtil.createContext(getClass().getSimpleName(), builder.build(), marshaller, timeService));
      TestingUtil.inject(factory, timeService);
      store.start();
      executorService = Executors.newFixedThreadPool(THREADS + 1);
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
      // let's wait so that we don't store expired values in the map
      Thread.sleep(100);

      Map<Object, Object> entries = new HashMap<>();
      store.process(KeyFilter.ACCEPT_ALL_FILTER, (marshalledEntry, taskContext) -> {
         Object prev = entries.put(marshalledEntry.getKey(), marshalledEntry.getValue());
         if (prev != null) {
            fail("Returned entry twice: " + marshalledEntry.getKey() + " -> " + prev + ", " + marshalledEntry.getValue());
         }
      }, new WithinThreadExecutor(), true, false);
      store.stop();
      // remove index files
      Stream.of(new File(tmpDirectory).listFiles(file -> !file.isDirectory())).map(f -> f.delete());
      store.start();
      store.process(KeyFilter.ACCEPT_ALL_FILTER, (marshalledEntry, taskContext) -> {
         Object stored = entries.get(marshalledEntry.getKey());
         if (stored == null) {
            fail("Loaded " + marshalledEntry.getKey() + " -> " + marshalledEntry.getValue() + " but it's not in the map");
         } else if (!Objects.equals(stored, marshalledEntry.getValue())) {
            fail("Loaded " + marshalledEntry.getKey() + " -> " + marshalledEntry.getValue() + " but it's was " + stored);
         }
      }, new WithinThreadExecutor(), true, false);
      for (Map.Entry<Object, Object> entry : entries.entrySet()) {
         MarshalledEntry loaded = store.load(entry.getKey());
         if (loaded == null) {
            fail("Did not load " + entry.getKey() + " -> " + entry.getValue());
         } else if (!Objects.equals(entry.getValue(), loaded.getValue())) {
            fail("Loaded " + entry.getKey() + " -> " + loaded.getValue() + " but it should be " + entry.getValue());
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
             String key = key(random);
            switch (random.nextInt(3)) {
               case 0:
                  lifespan = random.nextInt(3) == 0 ? random.nextInt(10) : -1;
                  ice = TestInternalCacheEntryFactory.<Object, Object>create(factory,
                     key(random), String.valueOf(random.nextInt()), lifespan);
                  store.write(TestingUtil.marshalledEntry(ice, marshaller));
                  break;
               case 1:
                  store.delete(key);
                  break;
               case 2:
                  store.load(key);
            }
         }
      }
   }

   protected String key(ThreadLocalRandom random) {
      return "key" + random.nextInt(KEY_RANGE);
   }
}
