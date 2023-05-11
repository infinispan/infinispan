package org.infinispan.distribution.groups;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.Flag;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.core.Flowable;

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
      return new Object[]{
            new GetGroupKeysTest(false, TestCacheFactory.PRIMARY_OWNER),
            new GetGroupKeysTest(false, TestCacheFactory.BACKUP_OWNER),
            new GetGroupKeysTest(false, TestCacheFactory.NON_OWNER),
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
      TestCache testCache = createTestCacheAndReset(GROUP, caches());
      initCache(testCache.primaryOwner);
      Map<GroupKey, String> groupKeySet = testCache.testCache.getGroup(GROUP);
      Map<GroupKey, String> expectedGroupSet = createMap(0, 10);
      assertEquals(expectedGroupSet, groupKeySet);
   }

   public void testGetKeysInGroupWithPersistence() {
      TestCache testCache = createTestCacheAndReset(GROUP, caches(PERSISTENCE_CACHE));
      initCache(testCache.primaryOwner);
      Map<GroupKey, String> groupKeySet = testCache.testCache.getGroup(GROUP);
      Map<GroupKey, String> expectedGroupSet = createMap(0, 10);
      assertEquals(expectedGroupSet, groupKeySet);
   }

   public void testGetKeysInGroupWithPersistenceAndPassivation() {
      TestCache testCache = createTestCacheAndReset(GROUP, caches(PERSISTENCE_PASSIVATION_CACHE));
      initCache(testCache.primaryOwner);
      Map<GroupKey, String> groupKeySet = testCache.testCache.getGroup(GROUP);
      Map<GroupKey, String> expectedGroupSet = createMap(0, 10);
      assertEquals(expectedGroupSet, groupKeySet);
   }

   public void testGetKeysInGroupWithPersistenceAndSkipCacheLoader() {
      TestCache testCache = createTestCacheAndReset(GROUP, caches(PERSISTENCE_CACHE));
      initCache(testCache.primaryOwner);
      Map<GroupKey, String> groupKeySet = testCache.testCache.withFlags(Flag.SKIP_CACHE_LOAD).getGroup(GROUP);
      Map<GroupKey, String> expectedGroupSet = new HashMap<>();
      //noinspection unchecked
      for (InternalCacheEntry<GroupKey, String> entry :
            (DataContainer<GroupKey, String>) TestingUtil.extractComponent(extractTargetCache(testCache), InternalDataContainer.class)) {
         if (entry.getKey().getGroup().equals(GROUP)) {
            expectedGroupSet.put(entry.getKey(), entry.getValue());
         }
      }
      assertEquals(expectedGroupSet, groupKeySet);
   }

   public void testRemoveGroupKeys() {
      TestCache testCache = createTestCacheAndReset(GROUP, caches());
      initCache(testCache.primaryOwner);
      Map<GroupKey, String> groupKeySet = testCache.testCache.getGroup(GROUP);
      Map<GroupKey, String> expectedGroupSet = createMap(0, 10);
      assertEquals(expectedGroupSet, groupKeySet);

      testCache.testCache.removeGroup(GROUP);
      assertEquals(Collections.emptyMap(), testCache.testCache.getGroup(GROUP));
   }

   public void testRemoveGroupKeysWithPersistence() {
      TestCache testCache = createTestCacheAndReset(GROUP, caches(PERSISTENCE_CACHE));
      initCache(testCache.primaryOwner);
      Map<GroupKey, String> groupKeySet = testCache.testCache.getGroup(GROUP);
      Map<GroupKey, String> expectedGroupSet = createMap(0, 10);
      assertEquals(expectedGroupSet, groupKeySet);

      testCache.testCache.removeGroup(GROUP);
      assertEquals(Collections.emptyMap(), testCache.testCache.getGroup(GROUP));
   }

   public void testRemoveGroupKeysWithPersistenceAndPassivation() {
      TestCache testCache = createTestCacheAndReset(GROUP, caches(PERSISTENCE_PASSIVATION_CACHE));
      initCache(testCache.primaryOwner);
      Map<GroupKey, String> groupKeySet = testCache.testCache.getGroup(GROUP);
      Map<GroupKey, String> expectedGroupSet = createMap(0, 10);
      assertEquals(expectedGroupSet, groupKeySet);

      testCache.testCache.removeGroup(GROUP);
      assertEquals(Collections.emptyMap(), testCache.testCache.getGroup(GROUP));
   }

   public void testRemoveGroupKeysWithPersistenceAndSkipCacheWriter() {
      TestCache testCache = createTestCacheAndReset(GROUP, caches(PERSISTENCE_CACHE));
      initCache(testCache.primaryOwner);
      Map<GroupKey, String> groupKeySet = testCache.testCache.getGroup(GROUP);
      Map<GroupKey, String> expectedGroupSet = createMap(0, 10);
      assertEquals(expectedGroupSet, groupKeySet);

      testCache.testCache.withFlags(Flag.SKIP_CACHE_STORE).removeGroup(GROUP);
      Map<GroupKey, String> expectedGroupSet2 = new ConcurrentHashMap<>();
      Flowable<MarshallableEntry<GroupKey, String>> flowable = Flowable.fromPublisher(
            TestingUtil.extractComponent(extractTargetCache(testCache), PersistenceManager.class)
                  .publishEntries(true, true));
      flowable.filter(me -> GROUP.equals(me.getKey().getGroup()))
            .blockingForEach(me -> expectedGroupSet2.put(me.getKey(), me.getValue()));
      assertEquals(new HashMap<>(expectedGroupSet2), testCache.testCache.getGroup(GROUP));
   }


   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(3, GroupTestsSCI.INSTANCE, amendConfiguration(createConfigurationBuilder(transactional)));
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
         builder.memory().maxCount(2);
      }
      builder.persistence().passivation(passivation)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class);
      return builder;
   }

}
