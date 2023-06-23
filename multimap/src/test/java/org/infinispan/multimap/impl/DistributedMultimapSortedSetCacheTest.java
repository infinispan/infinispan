package org.infinispan.multimap.impl;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.functional.FunctionalTestUtils;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.multimap.api.embedded.EmbeddedMultimapCacheManagerFactory;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.data.Person;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.multimap.impl.MultimapTestUtils.ELAIA;
import static org.infinispan.multimap.impl.MultimapTestUtils.FELIX;
import static org.infinispan.multimap.impl.MultimapTestUtils.IGOR;
import static org.infinispan.multimap.impl.MultimapTestUtils.IZARO;
import static org.infinispan.multimap.impl.MultimapTestUtils.JULIEN;
import static org.infinispan.multimap.impl.MultimapTestUtils.NAMES_KEY;
import static org.infinispan.multimap.impl.MultimapTestUtils.OIHANA;
import static org.infinispan.multimap.impl.MultimapTestUtils.PEPE;
import static org.infinispan.multimap.impl.MultimapTestUtils.RAMON;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

@Test(groups = "functional", testName = "distribution.DistributedMultimapSortedSetCacheTest")
public class DistributedMultimapSortedSetCacheTest extends BaseDistFunctionalTest<String, Collection<Person>> {

   protected Map<Address, EmbeddedMultimapSortedSetCache<String, Person>> sortedSetCluster = new HashMap<>();
   protected boolean fromOwner;

   public DistributedMultimapSortedSetCacheTest fromOwner(boolean fromOwner) {
      this.fromOwner = fromOwner;
      return this;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();

      for (EmbeddedCacheManager cacheManager : cacheManagers) {
         EmbeddedMultimapCacheManager multimapCacheManager = (EmbeddedMultimapCacheManager) EmbeddedMultimapCacheManagerFactory.from(cacheManager);
         sortedSetCluster.put(cacheManager.getAddress(), multimapCacheManager.getMultimapSortedSet(cacheName));
      }
   }

   @Override
   protected SerializationContextInitializer getSerializationContext() {
      return MultimapSCI.INSTANCE;
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), "fromOwner");
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), fromOwner ? Boolean.TRUE : Boolean.FALSE);
   }

   @Override
   public Object[] factory() {
      return new Object[]{
            new DistributedMultimapSortedSetCacheTest().fromOwner(false).cacheMode(CacheMode.DIST_SYNC).transactional(false),
            new DistributedMultimapSortedSetCacheTest().fromOwner(true).cacheMode(CacheMode.DIST_SYNC).transactional(false),
      };
   }

   @Override
   protected void initAndTest() {
      for (EmbeddedMultimapSortedSetCache sortedSet : sortedSetCluster.values()) {
         assertThat(await(sortedSet.size(NAMES_KEY))).isEqualTo(0L);
      }
   }

   protected EmbeddedMultimapSortedSetCache<String, Person> getMultimapCacheMember() {
      return sortedSetCluster.
            values().stream().findFirst().orElseThrow(() -> new IllegalStateException("Cluster is empty"));
   }

   public void testAddMany() {
      initAndTest();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapCacheMember();
      await(sortedSet.addMany(NAMES_KEY, new double[] { 1.1, 9.1 }, new Person[] { OIHANA, ELAIA }, SortedSetAddArgs.create().build()));
      assertValuesAndOwnership(NAMES_KEY, SortedSetBucket.ScoredValue.of(1.1, OIHANA));
      assertValuesAndOwnership(NAMES_KEY, SortedSetBucket.ScoredValue.of(9.1, ELAIA));
   }

   public void testCount() {
      initAndTest();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapCacheMember();
      await(sortedSet.addMany(NAMES_KEY,
            new double[] { 1, 1, 2, 2, 2, 3, 3, 3 },
            new Person[] { OIHANA, ELAIA, FELIX, RAMON, JULIEN, PEPE, IGOR, IZARO },
            SortedSetAddArgs.create().build()));
      assertThat(await(sortedSet.size(NAMES_KEY))).isEqualTo(8);
      assertThat(await(sortedSet.count(NAMES_KEY, 1, true, 3, true))).isEqualTo(8);
   }

   protected void assertValuesAndOwnership(String key, SortedSetBucket.ScoredValue<Person> value) {
      assertOwnershipAndNonOwnership(key, l1CacheEnabled);
      assertOnAllCaches(key, value);
   }

   protected void assertOnAllCaches(Object key, SortedSetBucket.ScoredValue<Person> value) {
      for (Map.Entry<Address, EmbeddedMultimapSortedSetCache<String, Person>> entry : sortedSetCluster.entrySet()) {
         FunctionalTestUtils.await(entry.getValue().get((String) key).thenAccept(v -> {
                  assertNotNull(format("values on the key %s must be not null", key), v);
                  assertTrue(format("values on the key '%s' must contain '%s' on node '%s'", key, value, entry.getKey()),
                        v.contains(value));
               })
         );
      }
   }
}
