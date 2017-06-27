package org.infinispan.distribution.groups;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.Cache;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.filter.KeyFilter;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.util.logging.Log;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * It tests the grouping advanced interface.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "distribution.groups.GetGroupKeysTest")
public class GetGroupKeysTest extends BaseUtilGroupTest {

   protected static final String PERSISTENCE_CACHE = "persistence-cache";
   protected static final String PERSISTENCE_PASSIVATION_CACHE = "persistence-passivation-cache";
   protected final boolean transactional;

   @Override
   public Object[] factory() {
      return new Object[] {
         new GetGroupKeysTest(false, TestCacheFactory.PRIMARY_OWNER),
         new GetGroupKeysTest(false, TestCacheFactory.BACKUP_OWNER),
         new GetGroupKeysTest(false, TestCacheFactory.NON_OWNER),
         new GetGroupKeysTest(false, TestCacheFactory.PRIMARY_OWNER).cacheMode(CacheMode.SCATTERED_SYNC),
         new GetGroupKeysTest(false, TestCacheFactory.NON_OWNER).cacheMode(CacheMode.SCATTERED_SYNC),
      };
   }

   public GetGroupKeysTest() {
      this(false, null);
   }

   protected GetGroupKeysTest(boolean transactional, TestCacheFactory factory) {
      super(factory);
      this.transactional = transactional;
   }

   public void testGetKeysInGroup() {
      final TestCache testCache = createTestCacheAndReset(GROUP, this.caches());
      initCache(testCache.primaryOwner);
      Map<GroupKey, String> groupKeySet = testCache.testCache.getGroup(GROUP);
      Map<GroupKey, String> expectedGroupSet = createMap(0, 10);
      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);
   }

   public void testGetKeysInGroupWithPersistence() {
      final TestCache testCache = createTestCacheAndReset(GROUP, this.caches(PERSISTENCE_CACHE));
      initCache(testCache.primaryOwner);
      Map<GroupKey, String> groupKeySet = testCache.testCache.getGroup(GROUP);
      Map<GroupKey, String> expectedGroupSet = createMap(0, 10);
      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);
   }

   public void testGetKeysInGroupWithPersistenceAndPassivation() {
      final TestCache testCache = createTestCacheAndReset(GROUP, this.caches(PERSISTENCE_PASSIVATION_CACHE));
      initCache(testCache.primaryOwner);
      Map<GroupKey, String> groupKeySet = testCache.testCache.getGroup(GROUP);
      Map<GroupKey, String> expectedGroupSet = createMap(0, 10);
      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);
   }

   public void testGetKeysInGroupWithPersistenceAndSkipCacheLoader() {
      final TestCache testCache = createTestCacheAndReset(GROUP, this.caches(PERSISTENCE_CACHE));
      initCache(testCache.primaryOwner);
      Map<GroupKey, String> groupKeySet = testCache.testCache.withFlags(Flag.SKIP_CACHE_LOAD).getGroup(GROUP);
      //noinspection unchecked
      Map<GroupKey, String> expectedGroupSet = new HashMap<>();
      //noinspection unchecked
      for (InternalCacheEntry<GroupKey, String> entry :
            (DataContainer<GroupKey, String>) TestingUtil.extractComponent(extractTargetCache(testCache), DataContainer.class)) {
         if (entry.getKey().getGroup().equals(GROUP)) {
            expectedGroupSet.put(entry.getKey(), entry.getValue());
         }
      }
      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);
   }

   public void testGetKeyInGroupWithConcurrentActivation() throws TimeoutException, InterruptedException, ExecutionException {
      final TestCache testCache = createTestCacheAndReset(GROUP, this.caches(PERSISTENCE_PASSIVATION_CACHE));
      initCache(testCache.primaryOwner);
      final BlockCommandInterceptor interceptor = injectIfAbsent(extractTargetCache(testCache));

      interceptor.open = false;
      Future<Map<GroupKey, String>> future = fork(new Callable<Map<GroupKey, String>>() {
         @Override
         public Map<GroupKey, String> call() throws Exception {
            return testCache.testCache.getGroup(GROUP);
         }
      });
      interceptor.awaitCommandBlock();

      final AtomicReference<GroupKey> keyToActivate = new AtomicReference<>(null);
      PersistenceManager persistenceManager = TestingUtil.extractComponent(extractTargetCache(testCache), PersistenceManager.class);
      persistenceManager.processOnAllStores(KeyFilter.ACCEPT_ALL_FILTER, new AdvancedCacheLoader.CacheLoaderTask() {
         @Override
         public void processEntry(MarshalledEntry marshalledEntry, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
            keyToActivate.compareAndSet(null, (GroupKey) marshalledEntry.getKey());
            taskContext.stop();
         }
      }, false, false);
      AssertJUnit.assertNotNull(extractTargetCache(testCache).get(keyToActivate.get())); //activates the key


      interceptor.unblockCommand();

      //it should able to pick the remove key
      Map<GroupKey, String> groupKeySet = future.get();
      Map<GroupKey, String> expectedGroupSet = createMap(0, 10);
      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);
   }

   public void testRemoveGroupKeys() {
      final TestCache testCache = createTestCacheAndReset(GROUP, this.caches());
      initCache(testCache.primaryOwner);
      Map<GroupKey, String> groupKeySet = testCache.testCache.getGroup(GROUP);
      Map<GroupKey, String> expectedGroupSet = createMap(0, 10);
      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);

      testCache.testCache.removeGroup(GROUP);
      AssertJUnit.assertTrue(testCache.testCache.getGroup(GROUP).isEmpty());
   }

   public void testRemoveGroupKeysWithPersistence() {
      final TestCache testCache = createTestCacheAndReset(GROUP, this.caches(PERSISTENCE_CACHE));
      initCache(testCache.primaryOwner);
      Map<GroupKey, String> groupKeySet = testCache.testCache.getGroup(GROUP);
      Map<GroupKey, String> expectedGroupSet = createMap(0, 10);
      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);

      testCache.testCache.removeGroup(GROUP);
      AssertJUnit.assertTrue(testCache.testCache.getGroup(GROUP).isEmpty());
   }

   public void testRemoveGroupKeysWithPersistenceAndPassivation() {
      final TestCache testCache = createTestCacheAndReset(GROUP, this.caches(PERSISTENCE_PASSIVATION_CACHE));
      initCache(testCache.primaryOwner);
      Map<GroupKey, String> groupKeySet = testCache.testCache.getGroup(GROUP);
      Map<GroupKey, String> expectedGroupSet = createMap(0, 10);
      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);

      testCache.testCache.removeGroup(GROUP);
      AssertJUnit.assertTrue(testCache.testCache.getGroup(GROUP).isEmpty());
   }

   public void testRemoveGroupKeysWithPersistenceAndSkipCacheWriter() {
      final TestCache testCache = createTestCacheAndReset(GROUP, this.caches(PERSISTENCE_CACHE));
      initCache(testCache.primaryOwner);
      Map<GroupKey, String> groupKeySet = testCache.testCache.getGroup(GROUP);
      Map<GroupKey, String> expectedGroupSet = createMap(0, 10);
      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);

      testCache.testCache.withFlags(Flag.SKIP_CACHE_STORE).removeGroup(GROUP);
      final Map<GroupKey, String> expectedGroupSet2 = new ConcurrentHashMap<>();
      TestingUtil.extractComponent(extractTargetCache(testCache), PersistenceManager.class).processOnAllStores(
            new KeyFilter() {
               @Override
               public boolean accept(Object key) {
                  return ((GroupKey) key).getGroup().equals(GROUP);
               }
            }, new AdvancedCacheLoader.CacheLoaderTask() {
               @Override
               public void processEntry(MarshalledEntry marshalledEntry, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
                  expectedGroupSet2.put((GroupKey) marshalledEntry.getKey(), (String) marshalledEntry.getValue());
               }
            }, true, true);

      groupKeySet = testCache.testCache.getGroup(GROUP);
      expectedGroupSet = new HashMap<>(expectedGroupSet2);
      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);
   }


   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(3, amendConfiguration(createConfigurationBuilder(transactional)));
      defineConfigurationOnAllManagers(PERSISTENCE_CACHE,
                                       amendConfiguration(createConfigurationBuilderWithPersistence(transactional, false)));
      waitForClusterToForm(PERSISTENCE_CACHE);
      defineConfigurationOnAllManagers(PERSISTENCE_PASSIVATION_CACHE,
                                       amendConfiguration(createConfigurationBuilderWithPersistence(transactional, true)));
      waitForClusterToForm(PERSISTENCE_PASSIVATION_CACHE);
   }

   protected ConfigurationBuilder amendConfiguration(ConfigurationBuilder builder) {
      return builder;
   }

   @Override
   protected final void resetCaches(List<Cache<BaseUtilGroupTest.GroupKey, String>> cacheList) {
      for (Cache cache : cacheList) {
         AsyncInterceptorChain chain = cache.getAdvancedCache().getAsyncInterceptorChain();
         BlockCommandInterceptor interceptor = chain.findInterceptorExtending(BlockCommandInterceptor.class);
         if (interceptor != null) {
            interceptor.reset();
         }
      }
   }

   private ConfigurationBuilder createConfigurationBuilder(boolean transactional) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(cacheMode, transactional);
      builder.clustering().stateTransfer().fetchInMemoryState(false);
      builder.clustering().hash().groups().enabled(true);
      return builder;
   }

   private ConfigurationBuilder createConfigurationBuilderWithPersistence(boolean transactional, boolean passivation) {
      ConfigurationBuilder builder = createConfigurationBuilder(transactional);
      if (passivation) {
         builder.eviction().maxEntries(2);
      }
      builder.persistence().passivation(passivation)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .fetchPersistentState(false);
      return builder;
   }

   private BlockCommandInterceptor injectIfAbsent(Cache<?, ?> cache) {
      log.debugf("Injecting BlockCommandInterceptor in %s", cache);
      AsyncInterceptorChain chain = cache.getAdvancedCache().getAsyncInterceptorChain();
      BlockCommandInterceptor interceptor = chain.findInterceptorExtending(BlockCommandInterceptor.class);
      if (interceptor == null) {
         interceptor = new BlockCommandInterceptor(log);
         EntryWrappingInterceptor ewi = chain.findInterceptorExtending(EntryWrappingInterceptor.class);
         AssertJUnit.assertTrue(chain.addInterceptorAfter(interceptor, ewi.getClass()));
      }
      interceptor.reset();
      log.debugf("Injected BlockCommandInterceptor in %s. Interceptor=%s", cache, interceptor);
      return interceptor;
   }

   private static class BlockCommandInterceptor extends CommandInterceptor {

      private volatile CheckPoint checkPoint;
      private volatile boolean open;
      private final Log log;

      private BlockCommandInterceptor(Log log) {
         this.log = log;
         checkPoint = new CheckPoint();
      }

      @Override
      public Object visitGetKeysInGroupCommand(InvocationContext ctx, GetKeysInGroupCommand command) throws Throwable {
         log.debugf("Visit Get Keys in Group. Open? %s. CheckPoint=%s", open, checkPoint);
         if (!open) {
            checkPoint.trigger("before");
            checkPoint.awaitStrict("after", 30, TimeUnit.SECONDS);
         }
         return invokeNextInterceptor(ctx, command);
      }

      public final void awaitCommandBlock() throws TimeoutException, InterruptedException {
         checkPoint.awaitStrict("before", 30, TimeUnit.SECONDS);
      }

      public final void unblockCommand() {
         checkPoint.trigger("after");
      }

      public final void reset() {
         open = true;
         checkPoint = new CheckPoint();
      }
   }
}
