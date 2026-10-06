package org.infinispan.eviction.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.AbstractInternalDataContainer;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.impl.PassivationWriterInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfiguration;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.DataOperationOrderer;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "eviction.impl.PassivationRemoveStoreOrderingTest")
public class PassivationRemoveStoreOrderingTest extends SingleCacheManagerTest {

   private static final String KEY = "k";
   private static final String VALUE = "v";
   private static final int MAX_ENTRIES = 100;

   public PassivationRemoveStoreOrderingTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.persistence().passivation(true)
            .addStore(GatingStore.ConfigurationBuilder.class).purgeOnStartup(true);
      cfg.memory().maxCount(MAX_ENTRIES);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   public void testEvictionDoesNotResurrectEntryWhileRemoveDeleteInFlight() throws Exception {
      AdvancedCache<String, String> testCache = cacheManager.<String, String>getCache().getAdvancedCache();

      testCache.put(KEY, VALUE);

      InternalDataContainer<String, String> dc =
            (InternalDataContainer<String, String>) testCache.getDataContainer();
      Object storageKey = testCache.getKeyDataConversion().toStorage(KEY);
      InternalCacheEntry<String, String> entry = dc.peek(storageKey);
      assertThat(entry).as("entry must be resident before the remove").isNotNull();

      DataOperationOrderer orderer = TestingUtil.extractComponent(testCache, DataOperationOrderer.class);
      PassivationManager passivator = TestingUtil.extractComponent(testCache, PassivationManager.class);
      ExecutorService nonBlockingExecutor = TestingUtil.extractGlobalComponent(cacheManager, ExecutorService.class,
            KnownComponentNames.NON_BLOCKING_EXECUTOR);
      GatingStore store = TestingUtil.getFirstStore(testCache);

      Future<String> remove = fork(() -> testCache.remove(KEY));

      store.deleteEntered.get(10, TimeUnit.SECONDS);

      CompletionStage<Void> eviction = AbstractInternalDataContainer.handleEviction(entry, orderer, passivator,
            null, dc, nonBlockingExecutor, null);

      boolean passivationWriteIssued;
      try {
         store.writeIssued.get(2, TimeUnit.SECONDS);
         passivationWriteIssued = true;
      } catch (TimeoutException e) {
         passivationWriteIssued = false;
      }

      store.releaseDelete();
      remove.get(10, TimeUnit.SECONDS);
      eviction.toCompletableFuture().get(10, TimeUnit.SECONDS);

      assertThat(passivationWriteIssued)
            .as("eviction passivated a key whose remove store delete was still in flight")
            .isFalse();
      assertThat(testCache.get(KEY)).as("removed entry resurrected from the store").isNull();
   }

   public void testEvictionDoesNotResurrectEntryInOrdererGapBetweenStoreDeleteAndCommit() throws Exception {
      AdvancedCache<String, String> testCache = cacheManager.<String, String>getCache().getAdvancedCache();

      GatingStore store = TestingUtil.getFirstStore(testCache);
      store.gateDelete = false;

      testCache.put(KEY, VALUE);

      InternalDataContainer<String, String> dc =
            (InternalDataContainer<String, String>) testCache.getDataContainer();
      Object storageKey = testCache.getKeyDataConversion().toStorage(KEY);
      InternalCacheEntry<String, String> entry = dc.peek(storageKey);
      assertThat(entry).as("entry must be resident before the remove").isNotNull();

      GapInterceptor gap = new GapInterceptor();
      boolean added = TestingUtil.extractInterceptorChain(testCache)
            .addInterceptorBefore(gap, PassivationWriterInterceptor.class);
      assertThat(added).as("GapInterceptor must be spliced into the chain").isTrue();

      DataOperationOrderer orderer = TestingUtil.extractComponent(testCache, DataOperationOrderer.class);
      PassivationManager passivator = TestingUtil.extractComponent(testCache, PassivationManager.class);
      ExecutorService nonBlockingExecutor = TestingUtil.extractGlobalComponent(cacheManager, ExecutorService.class,
            KnownComponentNames.NON_BLOCKING_EXECUTOR);

      Future<String> remove = fork(() -> testCache.remove(KEY));

      gap.gapEntered.get(10, TimeUnit.SECONDS);

      CompletionStage<Void> eviction = AbstractInternalDataContainer.handleEviction(entry, orderer, passivator,
            null, dc, nonBlockingExecutor, null);

      boolean passivationWriteIssued;
      try {
         store.writeIssued.get(2, TimeUnit.SECONDS);
         passivationWriteIssued = true;
      } catch (TimeoutException e) {
         passivationWriteIssued = false;
      }

      gap.gapGate.complete(null);
      remove.get(10, TimeUnit.SECONDS);
      eviction.toCompletableFuture().get(10, TimeUnit.SECONDS);

      assertThat(passivationWriteIssued)
            .as("eviction passivated a key in the gap between the remove's store delete and container commit slots")
            .isFalse();
      assertThat(testCache.get(KEY)).as("removed entry resurrected from the store").isNull();
   }

   /**
    * Interceptor spliced directly above {@link PassivationWriterInterceptor} (the store writer in a passivation chain).
    * Its way-up handler runs after the store delete has completed but before {@code EntryWrappingInterceptor} commits the
    * removal to the container. Blocking there parks the remove in the window that, before the fix, had no REMOVE orderer
    * slot held while the key was still resident, so an eviction landing in it would passivate (and resurrect) the entry.
    */
   static class GapInterceptor extends DDAsyncInterceptor {

      final CompletableFuture<Void> gapEntered = new CompletableFuture<>();
      final CompletableFuture<Void> gapGate = new CompletableFuture<>();

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
            gapEntered.complete(null);
            try {
               gapGate.get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         });
      }
   }

   /**
    * In-memory store that holds a {@code delete} in flight (unless {@link #gateDelete} is cleared) and signals when a
    * {@code write} is issued, so the test can force the overlap between a remove's store delete and an eviction's
    * passivation write.
    */
   public static class GatingStore extends DummyInMemoryStore {

      final CompletableFuture<Void> deleteEntered = new CompletableFuture<>();
      final CompletableFuture<Void> writeIssued = new CompletableFuture<>();
      private final CompletableFuture<Void> deleteGate = new CompletableFuture<>();
      volatile boolean gateDelete = true;

      void releaseDelete() {
         deleteGate.complete(null);
      }

      @Override
      public CompletionStage<Void> write(int segment, MarshallableEntry entry) {
         writeIssued.complete(null);
         return super.write(segment, entry);
      }

      @Override
      public CompletionStage<Boolean> delete(int segment, Object key) {
         deleteEntered.complete(null);
         if (!gateDelete) {
            return super.delete(segment, key);
         }
         return deleteGate.thenCompose(ignore -> super.delete(segment, key));
      }

      @BuiltBy(ConfigurationBuilder.class)
      @ConfigurationFor(GatingStore.class)
      public static class Configuration extends DummyInMemoryStoreConfiguration {
         public Configuration(AttributeSet attributes, AsyncStoreConfiguration async) {
            super(attributes, async);
         }
      }

      public static class ConfigurationBuilder extends DummyInMemoryStoreConfigurationBuilder {
         public ConfigurationBuilder(PersistenceConfigurationBuilder builder) {
            super(builder);
         }

         @Override
         public Configuration create() {
            return new Configuration(attributes.protect(), async.create());
         }
      }
   }
}
