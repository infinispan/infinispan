package org.infinispan.multimap.impl.tx;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.multimap.impl.MultimapTestUtils.ELAIA;
import static org.infinispan.multimap.impl.MultimapTestUtils.FELIX;
import static org.infinispan.multimap.impl.MultimapTestUtils.OIHANA;
import static org.infinispan.multimap.impl.MultimapTestUtils.PEPE;

import java.util.Collection;
import java.util.List;

import org.infinispan.multimap.impl.BaseDistributedMultimapTest;
import org.infinispan.multimap.impl.EmbeddedMultimapCacheManager;
import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.multimap.impl.ScoredValue;
import org.infinispan.multimap.impl.SortedSetAddArgs;
import org.infinispan.test.data.Person;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.tx.TxDistributedMultimapSortedSetCacheTest")
public class TxDistributedMultimapSortedSetCacheTest extends BaseDistributedMultimapTest<EmbeddedMultimapSortedSetCache<String, Person>, Person> {

   @Override
   protected EmbeddedMultimapSortedSetCache<String, Person> create(EmbeddedMultimapCacheManager<String, Person> manager) {
      return manager.getMultimapSortedSet(cacheName);
   }

   @Override
   public Object[] factory() {
      return super.transactionalFactory(TxDistributedMultimapSortedSetCacheTest::new);
   }

   @Test(dataProvider = "sorted-set-args")
   public void testRollbackAddManyOperation(SortedSetAddArgs args) throws Throwable {
      String key = getEntryKey();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapMember();

      List<ScoredValue<Person>> initial = List.of(ScoredValue.of(4, PEPE), ScoredValue.of(5, OIHANA));
      await(sortedSet.addMany(key, initial, SortedSetAddArgs.create().build()));

      List<ScoredValue<Person>> values = List.of(
            // For smaller score only.
            ScoredValue.of(3, ELAIA),

            // For greater score only.
            ScoredValue.of(9, FELIX),

            // For update only.
            ScoredValue.of(7, OIHANA),

            // For update smaller.
            ScoredValue.of(2, PEPE)
      );
      runAndRollback(() -> sortedSet.addMany(key, values, args));

      Collection<ScoredValue<Person>> actual = await(sortedSet.get(key));
      assertThat(actual)
            .hasSize(2)
            .containsExactly(initial.get(0), initial.get(1));
   }

   @Test(dataProvider = "sorted-set-args")
   public void testRollbackIncrementScoreOperation(SortedSetAddArgs args) throws Throwable {
      String key = getEntryKey();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapMember();

      List<ScoredValue<Person>> initial = List.of(ScoredValue.of(5, OIHANA));
      await(sortedSet.addMany(key, initial, SortedSetAddArgs.create().build()));

      tm(0, cacheName).begin();
      await(sortedSet.incrementScore(key, 7d, OIHANA, args));
      await(sortedSet.incrementScore(key, 3d, ELAIA, args));
      tm(0, cacheName).rollback();

      Collection<ScoredValue<Person>> actual = await(sortedSet.get(key));
      assertThat(actual)
            .hasSize(1)
            .satisfiesExactly(entry -> {
               assertThat((Object) entry.getValue()).isEqualTo(OIHANA);
               assertThat(entry.score()).isIn(5d, 12d);
            });
   }

   @Test(dataProvider = "pop-args")
   public void testRollbackPopOperation(boolean min, int count) throws Throwable {
      String key = getEntryKey();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapMember();

      List<ScoredValue<Person>> initial = List.of(ScoredValue.of(4, PEPE), ScoredValue.of(5, OIHANA), ScoredValue.of(6, ELAIA));
      await(sortedSet.addMany(key, initial, SortedSetAddArgs.create().build()));

      runAndRollback(() -> sortedSet.pop(key, min, count));

      Collection<ScoredValue<Person>> actual = await(sortedSet.get(key));
      assertThat(actual)
            .hasSize(initial.size())
            .containsExactlyElementsOf(initial);
   }

   @Test(dataProvider = "remove-all-args")
   public void testRollbackRemoveAllByScoreOperation(boolean min, boolean max) throws Throwable {
      String key = getEntryKey();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapMember();

      List<ScoredValue<Person>> initial = List.of(ScoredValue.of(4, PEPE), ScoredValue.of(5, OIHANA), ScoredValue.of(6, ELAIA));
      await(sortedSet.addMany(key, initial, SortedSetAddArgs.create().build()));

      runAndRollback(() -> sortedSet.removeAll(key, 4d, min, 6d, max));

      Collection<ScoredValue<Person>> actual = await(sortedSet.get(key));
      assertThat(actual)
            .hasSize(initial.size())
            .containsExactlyElementsOf(initial);
   }

   @Test(dataProvider = "remove-all-args")
   public void testRollbackRemoveAllByLexOperation(boolean min, boolean max) throws Throwable {
      String key = getEntryKey();
      EmbeddedMultimapSortedSetCache<String, Person> sortedSet = getMultimapMember();

      List<ScoredValue<Person>> initial = List.of(ScoredValue.of(1, ELAIA), ScoredValue.of(1, FELIX), ScoredValue.of(1, OIHANA));
      await(sortedSet.addMany(key, initial, SortedSetAddArgs.create().build()));

      runAndRollback(() -> sortedSet.removeAll(key, ELAIA, min, OIHANA, max));

      Collection<ScoredValue<Person>> actual = await(sortedSet.get(key));
      assertThat(actual)
            .hasSize(initial.size())
            .containsExactlyElementsOf(initial);
   }

   @DataProvider(name = "sorted-set-args")
   Object[][] sortedSetArguments() {
      return new Object[][]{
            {SortedSetAddArgs.create().build()},
            {SortedSetAddArgs.create().addOnly().build()},
            {SortedSetAddArgs.create().updateOnly().build()},
            {SortedSetAddArgs.create().updateLessScoresOnly().build()},
            {SortedSetAddArgs.create().updateGreaterScoresOnly().build()},
            {SortedSetAddArgs.create().returnChangedCount().build()}
      };
   }

   @DataProvider(name = "pop-args")
   Object[][] popArgs() {
      return new Object[][]{
            {Boolean.TRUE, 1},
            {Boolean.TRUE, 2},
            {Boolean.TRUE, Integer.MAX_VALUE},
            {Boolean.FALSE, 1},
            {Boolean.FALSE, 2},
            {Boolean.FALSE, Integer.MAX_VALUE},
      };
   }

   @DataProvider(name = "remove-all-args")
   Object[][] removeAllArgs() {
      return new Object[][] {
            {Boolean.FALSE, Boolean.FALSE},
            {Boolean.FALSE, Boolean.TRUE},
            {Boolean.TRUE, Boolean.FALSE},
            {Boolean.TRUE, Boolean.TRUE},
      };
   }
}
