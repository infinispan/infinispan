package org.infinispan.persistence.sifs;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.InternalEntryFactoryImpl;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.util.DefaultTimeService;
import org.infinispan.util.PersistenceMockUtil;
import org.infinispan.util.TimeService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "stress", testName = "persistence.SoftIndexFileStoreStressTest")
public class SoftIndexFileStoreStressTest extends AbstractInfinispanTest {
   protected static final int THREADS = 10;
   protected static final long TEST_DURATION = TimeUnit.MINUTES.toMillis(10);

   private TestObjectStreamMarshaller marshaller;
   private InternalEntryFactory factory;
   private SoftIndexFileStore store;
   private String tmpDirectory;
   private ExecutorService executorService;
   private volatile boolean terminate;

   @BeforeMethod(alwaysRun = true)
   public void setUp() throws Exception {
      tmpDirectory = TestingUtil.tmpDirectory(this.getClass());
      marshaller = new TestObjectStreamMarshaller();
      factory = new InternalEntryFactoryImpl();
      store = new SoftIndexFileStore();
      ConfigurationBuilder builder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false);
      builder.persistence()
            .addStore(SoftIndexFileStoreConfigurationBuilder.class)
            .indexLocation(tmpDirectory).dataLocation(tmpDirectory + "/data")
            .maxFileSize(1000);

      TimeService timeService = new DefaultTimeService();
      store.init(PersistenceMockUtil.createContext(getClass().getSimpleName(), builder.build(), marshaller, timeService));
      ((InternalEntryFactoryImpl) factory).injectTimeService(timeService);
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
                  ice = TestInternalCacheEntryFactory.<Object, Object>create(factory, key(random), "value", lifespan);
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
      return "key" + random.nextInt(1000);
   }
}
