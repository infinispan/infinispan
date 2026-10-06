package org.infinispan.eviction.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.AbstractInternalDataContainer;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.impl.PassivationWriterInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.MarshallableFunctions;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfiguration;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.PersistenceException;
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
      GatingStore store = TestingUtil.getFirstStore(testCache);

      InternalCacheEntry<String, String> entry = putResident(testCache);

      Future<String> remove = fork(() -> testCache.remove(KEY));

      store.deleteEntered.get(10, TimeUnit.SECONDS);

      CompletionStage<Void> eviction = evict(testCache, entry);

      store.releaseDelete();
      remove.get(10, TimeUnit.SECONDS);
      eviction.toCompletableFuture().get(10, TimeUnit.SECONDS);

      assertThat(store.writeIssued.isDone())
            .as("eviction passivated a key whose remove store delete was still in flight")
            .isFalse();
      assertThat(testCache.get(KEY)).as("removed entry resurrected from the store").isNull();
   }

   public void testEvictionPassivatesEntryWhenRemoveStoreDeleteFails() throws Exception {
      AdvancedCache<String, String> testCache = cacheManager.<String, String>getCache().getAdvancedCache();
      GatingStore store = TestingUtil.getFirstStore(testCache);

      // The entry is only resident: with passivation, the store has no copy of it until it is evicted.
      InternalCacheEntry<String, String> entry = putResident(testCache);

      Future<String> remove = fork(() -> testCache.remove(KEY));

      store.deleteEntered.get(10, TimeUnit.SECONDS);

      // The eviction orders on the key while the remove holds it, so it waits for the remove's outcome.
      CompletionStage<Void> eviction = evict(testCache, entry);

      store.failDelete(new PersistenceException("Injected store delete failure"));

      assertThatThrownBy(() -> remove.get(10, TimeUnit.SECONDS))
            .as("the remove must surface the store failure")
            .hasCauseInstanceOf(PersistenceException.class);
      eviction.toCompletableFuture().get(10, TimeUnit.SECONDS);

      // The remove failed, so the entry still exists: the eviction must keep it by passivating it to the store.
      assertThat(store.keySet())
            .as("eviction skipped passivation after a failed remove, losing the entry")
            .contains(entry.getKey());
   }

   public void testEvictionDoesNotResurrectEntryInOrdererGapBetweenStoreDeleteAndCommit() throws Exception {
      assertEvictionInGapDoesNotResurrect(c -> c.remove(KEY));
   }

   public void testEvictionDoesNotResurrectComputedRemovalInOrdererGapBetweenStoreDeleteAndCommit() throws Exception {
      assertEvictionInGapDoesNotResurrect(c -> c.compute(KEY, (k, v) -> null));
   }

   public void testEvictionDoesNotResurrectMergedRemovalInOrdererGapBetweenStoreDeleteAndCommit() throws Exception {
      assertEvictionInGapDoesNotResurrect(c -> c.merge(KEY, VALUE, (oldValue, value) -> null));
   }

   public void testEvictionDoesNotResurrectFunctionalRemovalInOrdererGapBetweenStoreDeleteAndCommit() throws Exception {
      assertEvictionInGapDoesNotResurrect(c -> FunctionalMap.create(c).toReadWriteMap()
            .eval(KEY, MarshallableFunctions.removeReturnPrevOrNull())
            .join());
   }

   public void testEvictionDoesNotResurrectWriteOnlyManyRemovalInOrdererGapBetweenStoreDeleteAndCommit() throws Exception {
      assertEvictionInGapDoesNotResurrect(c -> FunctionalMap.create(c).toWriteOnlyMap()
            .evalMany(Set.of(KEY), MarshallableFunctions.removeConsumer())
            .join());
   }

   public void testEvictionDoesNotResurrectReadWriteManyRemovalInOrdererGapBetweenStoreDeleteAndCommit() throws Exception {
      // Traversing the result waits for the removal to complete.
      assertEvictionInGapDoesNotResurrect(c -> FunctionalMap.create(c).toReadWriteMap()
            .evalMany(Set.of(KEY), MarshallableFunctions.removeReturnPrevOrNull())
            .count());
   }

   /**
    * Writes {@link #VALUE} under {@link #KEY} and returns the entry resident in memory for it.
    *
    * @param testCache the cache to write to.
    * @return the resident entry for {@link #KEY}, keyed by its storage key.
    */
   private static InternalCacheEntry<String, String> putResident(AdvancedCache<String, String> testCache) {
      testCache.put(KEY, VALUE);
      Object storageKey = testCache.getKeyDataConversion().toStorage(KEY);
      InternalCacheEntry<String, String> entry = testCache.getDataContainer().peek(storageKey);
      assertThat(entry).as("entry must be resident before the remove").isNotNull();
      return entry;
   }

   /**
    * Evicts {@code entry} as size-based eviction does once the container has selected it, with the cache's own orderer,
    * passivator and executor.
    *
    * @param testCache the cache {@code entry} belongs to.
    * @param entry     the entry selected for eviction.
    * @return a stage that completes once the eviction has passivated {@code entry} or skipped passivating it.
    */
   private CompletionStage<Void> evict(AdvancedCache<String, String> testCache, InternalCacheEntry<String, String> entry) {
      DataOperationOrderer orderer = TestingUtil.extractComponent(testCache, DataOperationOrderer.class);
      PassivationManager passivator = TestingUtil.extractComponent(testCache, PassivationManager.class);
      ExecutorService nonBlockingExecutor = TestingUtil.extractGlobalComponent(cacheManager, ExecutorService.class,
            KnownComponentNames.NON_BLOCKING_EXECUTOR);
      InternalDataContainer<String, String> dc = (InternalDataContainer<String, String>) testCache.getDataContainer();
      return AbstractInternalDataContainer.handleEviction(entry, orderer, passivator, null, dc, nonBlockingExecutor, null);
   }

   /**
    * Parks {@code removal} between its store delete and its container commit, evicts whatever is resident for
    * {@link #KEY} at that point, and asserts the eviction neither passivated the key nor left it readable afterwards.
    *
    * @param removal the operation that removes {@link #KEY}, already resident in memory, from the cache.
    * @throws Exception if the removal or the eviction fails or does not complete in time.
    */
   private void assertEvictionInGapDoesNotResurrect(Function<AdvancedCache<String, String>, ?> removal) throws Exception {
      AdvancedCache<String, String> testCache = cacheManager.<String, String>getCache().getAdvancedCache();

      GatingStore store = TestingUtil.getFirstStore(testCache);
      store.gateDelete = false;

      InternalCacheEntry<String, String> entry = putResident(testCache);

      GapInterceptor gap = new GapInterceptor();
      boolean added = TestingUtil.extractInterceptorChain(testCache)
            .addInterceptorBefore(gap, PassivationWriterInterceptor.class);
      assertThat(added).as("GapInterceptor must be spliced into the chain").isTrue();

      Future<?> remove = fork(() -> removal.apply(testCache));

      gap.gapEntered.get(10, TimeUnit.SECONDS);

      // Size-based eviction only selects entries resident in the container, so evict whatever is resident now.
      InternalCacheEntry<String, String> resident = testCache.getDataContainer().peek(entry.getKey());
      CompletionStage<Void> eviction = resident == null
            ? CompletableFutures.completedNull()
            : evict(testCache, resident);

      gap.gapGate.complete(null);
      remove.get(10, TimeUnit.SECONDS);
      eviction.toCompletableFuture().get(10, TimeUnit.SECONDS);

      assertThat(store.writeIssued.isDone())
            .as("eviction passivated a key in the gap between the remove's store delete and container commit slots")
            .isFalse();
      assertThat(testCache.get(KEY)).as("removed entry resurrected from the store").isNull();
   }

   /**
    * Interceptor spliced directly above {@link PassivationWriterInterceptor} (the store writer in a passivation chain).
    * Its way-up handler runs after the store delete has completed but before {@code EntryWrappingInterceptor} commits the
    * removal to the container. Blocking there parks the removal in the window that, before the fix, had no REMOVE orderer
    * slot held while the key was still resident, so an eviction landing in it would passivate (and resurrect) the entry.
    * <p>
    * Each command that can remove the entry is parked the same way. The interceptor is spliced in after the entry is
    * written, so only the removal under test reaches it.
    * </p>
    */
   static class GapInterceptor extends DDAsyncInterceptor {

      final CompletableFuture<Void> gapEntered = new CompletableFuture<>();
      final CompletableFuture<Void> gapGate = new CompletableFuture<>();

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) {
         return parkAfterStoreWrite(ctx, command);
      }

      @Override
      public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) {
         return parkAfterStoreWrite(ctx, command);
      }

      @Override
      public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) {
         return parkAfterStoreWrite(ctx, command);
      }

      @Override
      public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) {
         return parkAfterStoreWrite(ctx, command);
      }

      @Override
      public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) {
         return parkAfterStoreWrite(ctx, command);
      }

      @Override
      public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) {
         return parkAfterStoreWrite(ctx, command);
      }

      /**
       * Lets the command run through the store writer, then blocks its way up until the test opens {@link #gapGate}.
       *
       * @param ctx     the invocation context of the command.
       * @param command the removal under test.
       * @return the command's result once the gate is opened.
       */
      private Object parkAfterStoreWrite(InvocationContext ctx, WriteCommand command) {
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

      /**
       * Fails the {@code delete} held in flight with {@code cause}, without deleting anything from the store.
       *
       * @param cause the failure the held {@code delete} completes with.
       */
      void failDelete(Throwable cause) {
         deleteGate.completeExceptionally(cause);
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
