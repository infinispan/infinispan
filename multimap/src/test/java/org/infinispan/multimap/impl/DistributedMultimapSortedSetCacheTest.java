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
import static org.assertj.core.util.Lists.list;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.multimap.impl.MultimapTestUtils.CHARY;
import static org.infinispan.multimap.impl.MultimapTestUtils.ELAIA;
import static org.infinispan.multimap.impl.MultimapTestUtils.FELIX;
import static org.infinispan.multimap.impl.MultimapTestUtils.IGOR;
import static org.infinispan.multimap.impl.MultimapTestUtils.IZARO;
import static org.infinispan.multimap.impl.MultimapTestUtils.JULIEN;
import static org.infinispan.multimap.impl.MultimapTestUtils.NAMES_KEY;
import static org.infinispan.multimap.impl.MultimapTestUtils.OIHANA;
import static org.infinispan.multimap.impl.MultimapTestUtils.PEPE;
import static org.infinispan.multimap.impl.MultimapTestUtils.RAMON;
import static org.infinispan.multimap.impl.SortedSetBucket.AggregateFunction.SUM;
import static org.infinispan.multimap.impl.SortedSetBucket.ScoredValue.of;
import static org.infinispan.multimap.impl.SortedSetSubsetArgs.create;
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
      await(sortedSet.addMany(NAMES_KEY, list(of(1.1, OIHANA), of(9.1, ELAIA)),
            SortedSetAddArgs.create().build()));
      assertValuesAndOwnership(NAMES_KEY, of(1.1, OIHANA));
      assertValuesAndOwnership(NAMES_KEY, of(9.1, ELAIA));
   }

   public void testCount() {
      initAndTest();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapCacheMember();
      await(sortedSet.addMany(NAMES_KEY,
            list(of(1, OIHANA), of(1, ELAIA), of(2, FELIX),
                  of(2, RAMON), of(2, JULIEN), of(3, PEPE), of(3, IGOR),
                  of(3, IZARO)), SortedSetAddArgs.create().build()));
      assertThat(await(sortedSet.size(NAMES_KEY))).isEqualTo(8);
      assertThat(await(sortedSet.count(NAMES_KEY, 1, true, 3, true))).isEqualTo(8);
   }

   public void testCountByLex() {
      initAndTest();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapCacheMember();
      await(sortedSet.addMany(NAMES_KEY,
            list(of(1, ELAIA), of(1, FELIX), of(1, OIHANA)), SortedSetAddArgs.create().build()));
      assertThat(await(sortedSet.size(NAMES_KEY))).isEqualTo(3);
      assertThat(await(sortedSet.count(NAMES_KEY, ELAIA, true, OIHANA, true))).isEqualTo(3);
   }

   public void testPop() {
      initAndTest();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapCacheMember();
      await(sortedSet.addMany(NAMES_KEY,
            list(of(1, OIHANA), of(1, ELAIA), of(2, FELIX),
                  of(2, RAMON), of(2, JULIEN), of(3, PEPE), of(3, IGOR),
                  of(3, IZARO)), SortedSetAddArgs.create().build()));
      assertThat(await(sortedSet.size(NAMES_KEY))).isEqualTo(8);
      assertThat(await(sortedSet.pop(NAMES_KEY, false, 3)))
            .containsExactly(of(3, PEPE), of(3, IZARO), of(3, IGOR));
   }

   public void testScore() {
      initAndTest();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapCacheMember();
      await(sortedSet.addMany(NAMES_KEY,
            list(of(1.1, OIHANA), of(9.1, ELAIA)), SortedSetAddArgs.create().build()));
      assertThat(await(sortedSet.score(NAMES_KEY, OIHANA))).isEqualTo(1.1);
      assertThat(await(sortedSet.score(NAMES_KEY, ELAIA))).isEqualTo(9.1);
   }

   public void testSubset() {
      initAndTest();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapCacheMember();
      await(sortedSet.addMany(NAMES_KEY,
            list(of(1, ELAIA), of(1, FELIX), of(1, IZARO), of(1, OIHANA)),
            SortedSetAddArgs.create().build()));
      SortedSetSubsetArgs.Builder<Long> argsIndex = create();
      assertThat(await(sortedSet.subsetByIndex(NAMES_KEY, argsIndex.start(0L).stop(-1L).isRev(false).build())))
            .containsExactly(
                  of(1, ELAIA),
                  of(1, FELIX),
                  of(1, IZARO),
                  of(1, OIHANA));

      SortedSetSubsetArgs.Builder<Double> argsScore = create();
      assertThat(await(sortedSet.subsetByScore(NAMES_KEY, argsScore.start(0d).stop(2d).isRev(false).build())))
            .containsExactly(
                  of(1, ELAIA),
                  of(1, FELIX),
                  of(1, IZARO),
                  of(1, OIHANA));

      SortedSetSubsetArgs.Builder<Person> argsLex = create();
      assertThat(await(sortedSet.subsetByLex(NAMES_KEY,
            argsLex.start(FELIX).includeStart(true).stop(OIHANA).includeStop(true).isRev(false).build())))
            .containsExactly(
                  of(1, FELIX),
                  of(1, IZARO),
                  of(1, OIHANA));
   }

   public void testIncrScore() {
      initAndTest();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapCacheMember();
      SortedSetAddArgs args = SortedSetAddArgs.create().build();
      await(sortedSet.addMany(NAMES_KEY,
            list(of(1.1, OIHANA), of(9.1, ELAIA)), args));
      assertThat(await(sortedSet.incrementScore(NAMES_KEY, 12, OIHANA, args))).isEqualTo(13.1);
      assertThat(await(sortedSet.score(NAMES_KEY, ELAIA))).isEqualTo(9.1);
   }

   public void testInter() {
      initAndTest();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapCacheMember();
      SortedSetAddArgs args = SortedSetAddArgs.create().build();
      await(sortedSet.addMany(NAMES_KEY,
            list(of(1.1, OIHANA), of(9.1, ELAIA)), args));
      assertThat(await(sortedSet.inter(NAMES_KEY, null, 1, SUM)))
            .containsExactly(of(1.1, OIHANA), of(9.1, ELAIA));
      assertThat(await(sortedSet.score(NAMES_KEY, ELAIA))).isEqualTo(9.1);
   }

   public void testUnion() {
      initAndTest();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapCacheMember();
      SortedSetAddArgs args = SortedSetAddArgs.create().build();
      await(sortedSet.addMany(NAMES_KEY,
            list(of(1.1, OIHANA), of(9.1, ELAIA)), args));
      assertThat(await(sortedSet.union(NAMES_KEY, null, 1, SUM)))
            .containsExactly(of(1.1, OIHANA), of(9.1, ELAIA));
      assertThat(await(sortedSet.score(NAMES_KEY, ELAIA))).isEqualTo(9.1);
   }

   public void testRemoveAll() {
      initAndTest();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapCacheMember();
      await(sortedSet.addMany(NAMES_KEY,
            list(of(1, OIHANA), of(1, ELAIA), of(2, FELIX),
                  of(2, RAMON), of(2, JULIEN), of(3, PEPE), of(3, IGOR),
                  of(3, IZARO)), SortedSetAddArgs.create().build()));
      assertThat(await(sortedSet.size(NAMES_KEY))).isEqualTo(8);
      assertThat(await(sortedSet.removeAll(NAMES_KEY, list(OIHANA, FELIX, CHARY)))).isEqualTo(2);
      assertThat(await(sortedSet.size(NAMES_KEY))).isEqualTo(6);
   }

   public void testRemoveAllRange() {
      initAndTest();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapCacheMember();
      await(sortedSet.addMany(NAMES_KEY,
            list(of(1, OIHANA), of(1, ELAIA), of(2, FELIX),
                  of(2, RAMON), of(2, JULIEN), of(3, IGOR),
                  of(3, IZARO), of(3, PEPE)), SortedSetAddArgs.create().build()));
      assertThat(await(sortedSet.size(NAMES_KEY))).isEqualTo(8);
      assertThat(await(sortedSet.removeAll(NAMES_KEY, 0L, 2L))).isEqualTo(3);
      assertThat(await(sortedSet.removeAll(NAMES_KEY, 2d, true, 2d, true))).isEqualTo(2);
      assertThat(await(sortedSet.removeAll(NAMES_KEY, IGOR, true, PEPE, true))).isEqualTo(3);
      assertThat(await(sortedSet.size(NAMES_KEY))).isZero();
      await(sortedSet.addMany(NAMES_KEY,
            list(of(1, OIHANA), of(1, ELAIA), of(2, FELIX),
                  of(2, RAMON), of(2, JULIEN), of(3, IGOR),
                  of(3, IZARO), of(3, PEPE)), SortedSetAddArgs.create().build()));
      assertThat(await(sortedSet.removeAll(NAMES_KEY, null, true, 3d, true))).isEqualTo(8);
      await(sortedSet.addMany(NAMES_KEY,
            list(of(0, OIHANA), of(0, ELAIA), of(0, FELIX),
                  of(0, RAMON), of(0, JULIEN), of(0, IGOR),
                  of(0, IZARO), of(0, PEPE)), SortedSetAddArgs.create().build()));
      assertThat(await(sortedSet.removeAll(NAMES_KEY, null, true, RAMON, true))).isEqualTo(8);
   }

   public void testRandomMembers() {
      initAndTest();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapCacheMember();
      SortedSetAddArgs args = SortedSetAddArgs.create().build();
      await(sortedSet.addMany(NAMES_KEY, list(of(1.1, OIHANA)), args));
      assertThat(await(sortedSet.randomMembers(NAMES_KEY, 1))).containsExactly(of(1.1, OIHANA));
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
