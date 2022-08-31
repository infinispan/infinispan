package org.infinispan.persistence.manager;

import static org.infinispan.commons.test.Exceptions.expectException;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import io.reactivex.rxjava3.core.Flowable;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.MemoryConfiguration;
import org.infinispan.configuration.cache.TransactionConfiguration;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.RunningComponentRef;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.PreloadManagerTest")
public class PreloadManagerTest extends AbstractInfinispanTest {

   private final ExecutorService preloadExecutor =
         Executors.newSingleThreadExecutor(getTestThreadFactory("PreloadManager"));

   public void testNoConcurrentLoad() throws Exception {
      PreloadManager manager = new PreloadManager();
      PersistenceManager persistenceManager = mock(PersistenceManager.class);
      CheckPoint checkPoint = new CheckPoint();

      waitForPublisherCall(checkPoint, persistenceManager);

      manager.nonBlockingExecutor = preloadExecutor;
      manager.persistenceManager = persistenceManager;
      manager.timeService = new ControlledTimeService();
      manager.configuration = mockConfiguration();
      manager.cache = mockedCache();

      Future<Void> initial = fork(manager::blockingPreload);
      checkPoint.awaitStrict("preload_starting", 10, TimeUnit.SECONDS);

      assertFalse(manager.isFullyPreloaded());
      assertEquals(PreloadStatus.RUNNING, manager.currentStatus());

      CompletionStage<Void> second = manager.preload();
      expectException(CompletionException.class, IllegalStateException.class,
            "Preloader already running",
            () -> await(second.toCompletableFuture()));

      checkPoint.trigger("preload_starting_proceed");
      initial.get(10, TimeUnit.SECONDS);

      assertTrue(manager.isFullyPreloaded());
      assertEquals(PreloadStatus.COMPLETE_LOAD, manager.currentStatus());
   }

   public void testFailedLoading() {
      PreloadManager manager = new PreloadManager();
      PersistenceManager persistenceManager = mock(PersistenceManager.class);

      manager.nonBlockingExecutor = preloadExecutor;
      manager.persistenceManager = persistenceManager;
      manager.timeService = new ControlledTimeService();
      manager.configuration = mockConfiguration();
      manager.cache = mockedCache();

      when(persistenceManager.preloadPublisher())
            .thenAnswer(ivk -> Flowable.error(new RuntimeException("Failed loading data")));

      expectException(CompletionException.class, RuntimeException.class,
            "Failed loading data", manager::blockingPreload);

      assertFalse(manager.isFullyPreloaded());
      assertEquals(PreloadStatus.FAILED_LOAD, manager.currentStatus());
   }

   private void waitForPublisherCall(CheckPoint checkPoint, PersistenceManager manager) {
      when(manager.preloadPublisher()).thenAnswer(ivk -> {
         checkPoint.trigger("preload_starting");
         checkPoint.awaitStrict("preload_starting_proceed", 10, TimeUnit.SECONDS);
         return Flowable.empty();
      });
   }

   private ComponentRef<AdvancedCache<?, ?>> mockedCache() {
      AdvancedCache<Object, Object> cache = mock(AdvancedCache.class);
      when(cache.getName()).thenReturn("PreloadManagerTest");
      when(cache.withStorageMediaType()).thenReturn(cache);
      when(cache.getKeyDataConversion()).thenReturn(null);
      when(cache.getValueDataConversion()).thenReturn(null);

      return new RunningComponentRef<>("SomeMockName", AdvancedCache.class, cache);
   }

   private Configuration mockConfiguration() {
      Configuration configuration = mock(Configuration.class);
      MemoryConfiguration memConf = mock(MemoryConfiguration.class);
      TransactionConfiguration transactionConf = mock(TransactionConfiguration.class);

      when(memConf.isEvictionEnabled()).thenReturn(false);
      when(transactionConf.transactionMode()).thenReturn(TransactionMode.NON_TRANSACTIONAL);
      when(configuration.transaction()).thenReturn(transactionConf);
      when(configuration.memory()).thenReturn(memConf);
      return configuration;
   }
}
