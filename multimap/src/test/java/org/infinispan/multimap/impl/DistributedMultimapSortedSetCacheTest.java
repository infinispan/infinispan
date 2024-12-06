package org.infinispan.multimap.impl;

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
import static org.infinispan.multimap.impl.MultimapTestUtils.OIHANA;
import static org.infinispan.multimap.impl.MultimapTestUtils.PEPE;
import static org.infinispan.multimap.impl.MultimapTestUtils.RAMON;
import static org.infinispan.multimap.impl.ScoredValue.of;
import static org.infinispan.multimap.impl.SortedSetBucket.AggregateFunction.SUM;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Map;

import org.infinispan.functional.FunctionalTestUtils;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.data.Person;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DistributedMultimapSortedSetCacheTest")
public class DistributedMultimapSortedSetCacheTest extends BaseDistributedMultimapTest<EmbeddedMultimapSortedSetCache<String, Person>, Person> {

   @Override
   protected EmbeddedMultimapSortedSetCache<String, Person> create(EmbeddedMultimapCacheManager<String, Person> manager) {
      return manager.getMultimapSortedSet(cacheName);
   }

   @Override
   public Object[] factory() {
      return super.factory(DistributedMultimapSortedSetCacheTest::new);
   }

   public void testAddMany() {
      String key = getEntryKey();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapMember();
      await(sortedSet.addMany(key, list(of(1.1, OIHANA), of(9.1, ELAIA)),
            SortedSetAddArgs.create().build()));
      assertValuesAndOwnership(key, of(1.1, OIHANA));
      assertValuesAndOwnership(key, of(9.1, ELAIA));
   }

   public void testCount() {
      String key = getEntryKey();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapMember();
      await(sortedSet.addMany(key,
            list(of(1, OIHANA), of(1, ELAIA), of(2, FELIX),
                  of(2, RAMON), of(2, JULIEN), of(3, PEPE), of(3, IGOR),
                  of(3, IZARO)), SortedSetAddArgs.create().build()));
      assertThat(await(sortedSet.size(key))).isEqualTo(8);
      assertThat(await(sortedSet.count(key, 1, true, 3, true))).isEqualTo(8);
   }

   public void testCountByLex() {
      String key = getEntryKey();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapMember();
      await(sortedSet.addMany(key,
            list(of(1, ELAIA), of(1, FELIX), of(1, OIHANA)), SortedSetAddArgs.create().build()));
      assertThat(await(sortedSet.size(key))).isEqualTo(3);
      assertThat(await(sortedSet.count(key, ELAIA, true, OIHANA, true))).isEqualTo(3);
   }

   public void testPop() {
      String key = getEntryKey();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapMember();
      await(sortedSet.addMany(key,
            list(of(1, OIHANA), of(1, ELAIA), of(2, FELIX),
                  of(2, RAMON), of(2, JULIEN), of(3, PEPE), of(3, IGOR),
                  of(3, IZARO)), SortedSetAddArgs.create().build()));
      assertThat(await(sortedSet.size(key))).isEqualTo(8);
      assertThat(await(sortedSet.pop(key, false, 3)))
            .containsExactly(of(3, PEPE), of(3, IZARO), of(3, IGOR));
   }

   public void testScore() {
      String key = getEntryKey();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapMember();
      await(sortedSet.addMany(key,
            list(of(1.1, OIHANA), of(9.1, ELAIA)), SortedSetAddArgs.create().build()));
      assertThat(await(sortedSet.score(key, OIHANA))).isEqualTo(1.1);
      assertThat(await(sortedSet.score(key, ELAIA))).isEqualTo(9.1);
   }

   public void testSubset() {
      String key = getEntryKey();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapMember();
      await(sortedSet.addMany(key,
            list(of(1, ELAIA), of(1, FELIX), of(1, IZARO), of(1, OIHANA)),
            SortedSetAddArgs.create().build()));
      SortedSetSubsetArgs.Builder<Long> argsIndex = SortedSetSubsetArgs.create();
      assertThat(await(sortedSet.subsetByIndex(key, argsIndex.start(0L).stop(-1L).isRev(false).build())))
            .containsExactly(
                  of(1, ELAIA),
                  of(1, FELIX),
                  of(1, IZARO),
                  of(1, OIHANA));

      SortedSetSubsetArgs.Builder<Double> argsScore = SortedSetSubsetArgs.create();
      assertThat(await(sortedSet.subsetByScore(key, argsScore.start(0d).stop(2d).isRev(false).build())))
            .containsExactly(
                  of(1, ELAIA),
                  of(1, FELIX),
                  of(1, IZARO),
                  of(1, OIHANA));

      SortedSetSubsetArgs.Builder<Person> argsLex = SortedSetSubsetArgs.create();
      assertThat(await(sortedSet.subsetByLex(key,
            argsLex.start(FELIX).includeStart(true).stop(OIHANA).includeStop(true).isRev(false).build())))
            .containsExactly(
                  of(1, FELIX),
                  of(1, IZARO),
                  of(1, OIHANA));
   }

   public void testIncrScore() {
      String key = getEntryKey();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapMember();
      SortedSetAddArgs args = SortedSetAddArgs.create().build();
      await(sortedSet.addMany(key,
            list(of(1.1, OIHANA), of(9.1, ELAIA)), args));
      assertThat(await(sortedSet.incrementScore(key, 12, OIHANA, args))).isEqualTo(13.1);
      assertThat(await(sortedSet.score(key, ELAIA))).isEqualTo(9.1);
   }

   public void testInter() {
      String key = getEntryKey();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapMember();
      SortedSetAddArgs args = SortedSetAddArgs.create().build();
      await(sortedSet.addMany(key,
            list(of(1.1, OIHANA), of(9.1, ELAIA)), args));
      assertThat(await(sortedSet.inter(key, null, 1, SUM)))
            .containsExactly(of(1.1, OIHANA), of(9.1, ELAIA));
      assertThat(await(sortedSet.score(key, ELAIA))).isEqualTo(9.1);
   }

   public void testUnion() {
      String key = getEntryKey();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapMember();
      SortedSetAddArgs args = SortedSetAddArgs.create().build();
      await(sortedSet.addMany(key,
            list(of(1.1, OIHANA), of(9.1, ELAIA)), args));
      assertThat(await(sortedSet.union(key, null, 1, SUM)))
            .containsExactly(of(1.1, OIHANA), of(9.1, ELAIA));
      assertThat(await(sortedSet.score(key, ELAIA))).isEqualTo(9.1);
   }

   public void testRemoveAll() {
      String key = getEntryKey();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapMember();
      await(sortedSet.addMany(key,
            list(of(1, OIHANA), of(1, ELAIA), of(2, FELIX),
                  of(2, RAMON), of(2, JULIEN), of(3, PEPE), of(3, IGOR),
                  of(3, IZARO)), SortedSetAddArgs.create().build()));
      assertThat(await(sortedSet.size(key))).isEqualTo(8);
      assertThat(await(sortedSet.removeAll(key, list(OIHANA, FELIX, CHARY)))).isEqualTo(2);
      assertThat(await(sortedSet.size(key))).isEqualTo(6);
   }

   public void testRemoveAllRange() {
      String key = getEntryKey();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapMember();
      await(sortedSet.addMany(key,
            list(of(1, OIHANA), of(1, ELAIA), of(2, FELIX),
                  of(2, RAMON), of(2, JULIEN), of(3, IGOR),
                  of(3, IZARO), of(3, PEPE)), SortedSetAddArgs.create().build()));
      assertThat(await(sortedSet.size(key))).isEqualTo(8);
      assertThat(await(sortedSet.removeAll(key, 0L, 2L))).isEqualTo(3);
      assertThat(await(sortedSet.removeAll(key, 2d, true, 2d, true))).isEqualTo(2);
      assertThat(await(sortedSet.removeAll(key, IGOR, true, PEPE, true))).isEqualTo(3);
      assertThat(await(sortedSet.size(key))).isZero();
      await(sortedSet.addMany(key,
            list(of(1, OIHANA), of(1, ELAIA), of(2, FELIX),
                  of(2, RAMON), of(2, JULIEN), of(3, IGOR),
                  of(3, IZARO), of(3, PEPE)), SortedSetAddArgs.create().build()));
      assertThat(await(sortedSet.removeAll(key, null, true, 3d, true))).isEqualTo(8);
      await(sortedSet.addMany(key,
            list(of(0, OIHANA), of(0, ELAIA), of(0, FELIX),
                  of(0, RAMON), of(0, JULIEN), of(0, IGOR),
                  of(0, IZARO), of(0, PEPE)), SortedSetAddArgs.create().build()));
      assertThat(await(sortedSet.removeAll(key, null, true, RAMON, true))).isEqualTo(8);
   }

   public void testRandomMembers() {
      String key = getEntryKey();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapMember();
      SortedSetAddArgs args = SortedSetAddArgs.create().build();
      await(sortedSet.addMany(key, list(of(1.1, OIHANA)), args));
      assertThat(await(sortedSet.randomMembers(key, 1))).containsExactly(of(1.1, OIHANA));
   }

   protected void assertValuesAndOwnership(String key, ScoredValue<Person> value) {
      assertOwnershipAndNonOwnership(key, l1CacheEnabled);
      assertOnAllCaches(key, value);
   }

   protected void assertOnAllCaches(Object key, ScoredValue<Person> value) {
      for (Map.Entry<Address, EmbeddedMultimapSortedSetCache<String, Person>> entry : cluster.entrySet()) {
         FunctionalTestUtils.await(entry.getValue().get((String) key).thenAccept(v -> {
                  assertNotNull(format("values on the key %s must be not null", key), v);
                  assertTrue(format("values on the key '%s' must contain '%s' on node '%s'", key, value, entry.getKey()),
                        v.contains(value));
               })
         );
      }
   }
}
