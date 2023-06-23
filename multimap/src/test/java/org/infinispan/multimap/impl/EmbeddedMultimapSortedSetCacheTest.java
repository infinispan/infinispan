package org.infinispan.multimap.impl;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.data.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache.ERR_KEY_CAN_T_BE_NULL;
import static org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache.ERR_SCORES_CAN_T_BE_NULL;
import static org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache.ERR_SCORES_VALUES_MUST_HAVE_SAME_SIZE;
import static org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache.ERR_VALUES_CAN_T_BE_NULL;
import static org.infinispan.multimap.impl.MultimapTestUtils.ELAIA;
import static org.infinispan.multimap.impl.MultimapTestUtils.FELIX;
import static org.infinispan.multimap.impl.MultimapTestUtils.IGOR;
import static org.infinispan.multimap.impl.MultimapTestUtils.IZARO;
import static org.infinispan.multimap.impl.MultimapTestUtils.JULIEN;
import static org.infinispan.multimap.impl.MultimapTestUtils.KOLDO;
import static org.infinispan.multimap.impl.MultimapTestUtils.NAMES_KEY;
import static org.infinispan.multimap.impl.MultimapTestUtils.OIHANA;
import static org.infinispan.multimap.impl.MultimapTestUtils.PEPE;
import static org.infinispan.multimap.impl.MultimapTestUtils.RAMON;
import static org.infinispan.multimap.impl.SortedSetBucket.ScoredValue.of;

/**
 * Single Multimap Cache Test with Linked List
 *
 * @since 15.0
 */
@Test(groups = "functional", testName = "multimap.EmbeddedMultimapSortedSetCacheTest")
public class EmbeddedMultimapSortedSetCacheTest extends SingleCacheManagerTest {

   EmbeddedMultimapSortedSetCache<String, Person> sortedSetCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // start a single cache instance
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(MultimapSCI.INSTANCE);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      cm.createCache("test", builder.build());
      cm.getClassAllowList().addClasses(Person.class);
      sortedSetCache = new EmbeddedMultimapSortedSetCache<>(cm.getCache("test"));
      return cm;
   }

   public void validateSortedSetAddArgs() {
      SortedSetAddArgs emptyArgs = SortedSetAddArgs.create().build();
      assertThat(emptyArgs).isNotNull();
      assertThat(emptyArgs.addOnly).isFalse();
      assertThat(emptyArgs.updateOnly).isFalse();
      assertThat(emptyArgs.updateGreaterScoresOnly).isFalse();
      assertThat(emptyArgs.updateLessScoresOnly).isFalse();
      assertThat(emptyArgs.returnChangedCount).isFalse();

      // updateOnly and addOnly can't both be true
      assertThat(SortedSetAddArgs.create().addOnly().build().addOnly).isTrue();
      assertThat(SortedSetAddArgs.create().updateOnly().build().updateOnly).isTrue();
      assertThatThrownBy(() -> SortedSetAddArgs.create().addOnly().updateOnly().build()).isInstanceOf(
            IllegalStateException.class).hasMessageContaining(SortedSetAddArgs.ADD_AND_UPDATE_ONLY_INCOMPATIBLE_ERROR);

      //  updateGt, updateLt and addOnly can't be all true
      assertThat(SortedSetAddArgs.create().updateGreaterScoresOnly().build().updateGreaterScoresOnly).isTrue();
      assertThat(SortedSetAddArgs.create().updateLessScoresOnly().build().updateLessScoresOnly).isTrue();
      assertThatThrownBy(() -> SortedSetAddArgs.create().addOnly().updateLessScoresOnly().build()).isInstanceOf(
            IllegalStateException.class).hasMessageContaining(SortedSetAddArgs.ADD_AND_UPDATE_OPTIONS_INCOMPATIBLE_ERROR);
      assertThatThrownBy(() -> SortedSetAddArgs.create().addOnly().updateGreaterScoresOnly().build()).isInstanceOf(
            IllegalStateException.class).hasMessageContaining(SortedSetAddArgs.ADD_AND_UPDATE_OPTIONS_INCOMPATIBLE_ERROR);
      assertThatThrownBy(() -> SortedSetAddArgs.create().updateLessScoresOnly().updateGreaterScoresOnly().build()).isInstanceOf(
            IllegalStateException.class).hasMessageContaining(SortedSetAddArgs.ADD_AND_UPDATE_OPTIONS_INCOMPATIBLE_ERROR);

   }

   public void testAddMany() {
      SortedSetAddArgs emptyArgs = SortedSetAddArgs.create().build();

      assertThat(await(sortedSetCache.addMany(NAMES_KEY, new double[] {}, new Person[] {}, emptyArgs))).isEqualTo(0);
      assertThat(await(sortedSetCache.addMany(NAMES_KEY, new double[] { 2, 4.2 }, new Person[] { OIHANA, ELAIA }, emptyArgs))).isEqualTo(2);
      assertThat(await(sortedSetCache.getValue(NAMES_KEY))).containsExactly(of(2, OIHANA), of(4.2, ELAIA) );
      assertThat(await(sortedSetCache.addMany(NAMES_KEY, new double[] { 9 }, new Person[] { OIHANA }, emptyArgs))).isEqualTo(0);
      assertThat(await(sortedSetCache.getValue(NAMES_KEY))).containsExactly(of(4.2, ELAIA), of(9, OIHANA) );
      assertThat(await(sortedSetCache.addMany(NAMES_KEY, new double[] { 10, 90 }, new Person[] { OIHANA, KOLDO }, emptyArgs))).isEqualTo(1);
      assertThat(await(sortedSetCache.getValue(NAMES_KEY))).containsExactly(of(4.2, ELAIA), of(10, OIHANA), of(90, KOLDO) );
      // count updates
      assertThat(await(sortedSetCache.addMany(NAMES_KEY, new double[] { 7.9 }, new Person[] { ELAIA },
            SortedSetAddArgs.create().returnChangedCount().build()))).isEqualTo(1);
      assertThat(await(sortedSetCache.getValue(NAMES_KEY))).containsExactly(of(7.9, ELAIA), of(10, OIHANA), of(90, KOLDO) );
      // add only
      assertThat(await(sortedSetCache.addMany(NAMES_KEY, new double[] { 9.9, 1 }, new Person[] { ELAIA, JULIEN },
            SortedSetAddArgs.create().addOnly().build()))).isEqualTo(1);
      assertThat(await(sortedSetCache.getValue(NAMES_KEY))).containsExactly(of(1, JULIEN), of(7.9, ELAIA), of(10, OIHANA), of(90, KOLDO) );
      // update only (does not create)
      assertThat(await(sortedSetCache.addMany(NAMES_KEY, new double[] { 9.9, 2.2 }, new Person[] { ELAIA, RAMON },
            SortedSetAddArgs.create().updateOnly().build()))).isEqualTo(0);
      assertThat(await(sortedSetCache.getValue(NAMES_KEY))).containsExactly(of(1, JULIEN), of(9.9, ELAIA), of(10, OIHANA), of(90, KOLDO) );
      // update less than provided scores and create new ones
      assertThat(await(sortedSetCache.addMany(NAMES_KEY, new double[] { 8.9, 11.1, 0.8 }, new Person[] { ELAIA, OIHANA, RAMON },
            SortedSetAddArgs.create().updateLessScoresOnly().build()))).isEqualTo(1);
      assertThat(await(sortedSetCache.getValue(NAMES_KEY))).containsExactly(of(0.8, RAMON), of(1, JULIEN), of(8.9, ELAIA), of(10, OIHANA), of(90, KOLDO) );
      // update greater than provided scores and create new ones
      assertThat(await(sortedSetCache.addMany(NAMES_KEY, new double[] { 6.9, 12.1, 32.98 }, new Person[] { ELAIA, OIHANA, FELIX },
            SortedSetAddArgs.create().updateGreaterScoresOnly().build()))).isEqualTo(1);
      assertThat(await(sortedSetCache.getValue(NAMES_KEY))).containsExactly(of(0.8, RAMON), of(1, JULIEN), of(8.9, ELAIA), of(12.1, OIHANA), of(32.98, FELIX), of(90, KOLDO) );

      // add a new value with the same score as other
      assertThat(await(sortedSetCache.addMany(NAMES_KEY, new double[] { 1 }, new Person[] { PEPE }, emptyArgs))).isEqualTo(1);
      assertThat(await(sortedSetCache.getValue(NAMES_KEY))).containsExactly(of(0.8, RAMON), of(1, JULIEN),  of(1, PEPE), of(8.9, ELAIA), of(12.1, OIHANA), of(32.98, FELIX), of(90, KOLDO) );

      // Errors
      assertThatThrownBy(() -> await(sortedSetCache.addMany(NAMES_KEY, new double[] { 1 }, new Person[] {}, emptyArgs)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(ERR_SCORES_VALUES_MUST_HAVE_SAME_SIZE);

      assertThatThrownBy(() -> await(sortedSetCache.addMany(NAMES_KEY, new double[] {}, new Person[] { OIHANA }, emptyArgs)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(ERR_SCORES_VALUES_MUST_HAVE_SAME_SIZE);

      assertThatThrownBy(() -> await(sortedSetCache.addMany(null, null, null, emptyArgs)))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_KEY_CAN_T_BE_NULL);

      assertThatThrownBy(() -> await(sortedSetCache.addMany(null, null, null, emptyArgs)))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_KEY_CAN_T_BE_NULL);

      assertThatThrownBy(() -> await(sortedSetCache.addMany(NAMES_KEY, null, null, emptyArgs)))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_SCORES_CAN_T_BE_NULL);

      assertThatThrownBy(() -> await(sortedSetCache.addMany(NAMES_KEY, new double[0], null, emptyArgs)))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_VALUES_CAN_T_BE_NULL);
   }

   public void testCount() {
      assertThat(await(sortedSetCache.count(NAMES_KEY, 0, false, 0, false))).isEqualTo(0);
      await(sortedSetCache.addMany(NAMES_KEY,
            new double[] {-5, 1, 2, 5, 8, 8, 8, 9, 10},
            new Person[] {OIHANA, ELAIA, KOLDO, RAMON, JULIEN, FELIX, PEPE, IGOR, IZARO},
            SortedSetAddArgs.create().build()));

      assertThat(await(sortedSetCache.count(NAMES_KEY, 8, true, 8, true))).isEqualTo(3);
      assertThat(await(sortedSetCache.count(NAMES_KEY, 8, false, 8, false))).isEqualTo(0);
      assertThat(await(sortedSetCache.count(NAMES_KEY, 8, true, 8, false))).isEqualTo(0);
      assertThat(await(sortedSetCache.count(NAMES_KEY, 8, false, 8, true))).isEqualTo(0);
      assertThat(await(sortedSetCache.count(NAMES_KEY, 1, true, 9, true))).isEqualTo(7);
      assertThat(await(sortedSetCache.count(NAMES_KEY, 1, true, 9, false))).isEqualTo(6);
      assertThat(await(sortedSetCache.count(NAMES_KEY, 1, false, 9, false))).isEqualTo(5);
      assertThat(await(sortedSetCache.count(NAMES_KEY, 1, false, 9, true))).isEqualTo(6);

      assertThatThrownBy(() -> await(sortedSetCache.count(null, 0, false, 0, false)))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_KEY_CAN_T_BE_NULL);
   }

}
