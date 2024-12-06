package org.infinispan.multimap.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.multimap.impl.MultimapTestUtils.ELAIA;
import static org.infinispan.multimap.impl.MultimapTestUtils.FELIX;
import static org.infinispan.multimap.impl.MultimapTestUtils.OIHANA;
import static org.infinispan.multimap.impl.MultimapTestUtils.RAMON;

import java.util.Arrays;
import java.util.Map;

import org.infinispan.remoting.transport.Address;
import org.infinispan.test.data.Person;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DistributedMultimapListCacheTest")
public class DistributedMultimapListCacheTest
      extends BaseDistributedMultimapTest<EmbeddedMultimapListCache<String, Person>, Person> {

   @Override
   protected EmbeddedMultimapListCache<String, Person> create(EmbeddedMultimapCacheManager<String, Person> manager) {
      return manager.getMultimapList(cacheName);
   }

   @Override
   public Object[] factory() {
      return super.factory(DistributedMultimapListCacheTest::new);
   }

   public void testOfferFirstAndLast() {
      String key = getEntryKey();
      EmbeddedMultimapListCache<String, Person> list = getMultimapMember();
      await(list.offerFirst(key, OIHANA));
      assertValuesAndOwnership(key, OIHANA);

      await(list.offerLast(key, ELAIA));
      assertValuesAndOwnership(key, ELAIA);
   }

   public void testPollFirstAndLast() {
      String key = getEntryKey();
      EmbeddedMultimapListCache<String, Person> list = getMultimapMember();
      await(
            list.offerLast(key, OIHANA)
                  .thenCompose(r1 -> list.offerLast(key, ELAIA))
                  .thenCompose(r1 -> list.offerLast(key, OIHANA))
                  .thenCompose(r1 -> list.offerLast(key, ELAIA))
                  .thenCompose(r1 -> list.size(key))
                  .thenAccept(size -> assertThat(size).isEqualTo(4))

      );

      await(
            list.pollLast(key, 2)
                  .thenAccept(r1 -> assertThat(r1).containsExactly(ELAIA, OIHANA))

      );
      await(
            list.pollFirst(key, 1)
                  .thenAccept(r1 -> assertThat(r1).containsExactly(OIHANA))

      );

      await(
            list.size(key)
                  .thenAccept(r1 -> assertThat(r1).isEqualTo(1))

      );
   }

   public void testSize() {
      String key = getEntryKey();
      EmbeddedMultimapListCache<String, Person> list = getMultimapMember();
      await(
            list.offerFirst(key, OIHANA)
                  .thenCompose(r1 -> list.offerFirst(key, OIHANA))
                  .thenCompose(r1 -> list.offerFirst(key, OIHANA))
                  .thenCompose(r1 -> list.offerFirst(key, OIHANA))
                  .thenCompose(r1 -> list.size(key))
                  .thenAccept(size -> assertThat(size).isEqualTo(4))

      );
   }

   public void testIndex() {
      String key = getEntryKey();
      EmbeddedMultimapListCache<String, Person> list = getMultimapMember();
      await(
            list.offerLast(key, OIHANA)
                  .thenCompose(r1 -> list.offerLast(key, OIHANA))
                  .thenCompose(r1 -> list.offerLast(key, ELAIA))
                  .thenCompose(r1 -> list.offerLast(key, OIHANA))
                  .thenCompose(r1 -> list.index(key, 2))
                  .thenAccept(v -> assertThat(v).isEqualTo(ELAIA))

      );
   }

   public void testSubList() {
      String key = getEntryKey();
      EmbeddedMultimapListCache<String, Person> list = getMultimapMember();
      await(
            list.offerLast(key, OIHANA)
                  .thenCompose(r1 -> list.offerLast(key, OIHANA))
                  .thenCompose(r1 -> list.offerLast(key, ELAIA))
                  .thenCompose(r1 -> list.offerLast(key, OIHANA))
                  .thenCompose(r1 -> list.subList(key, 1, 3))
                  .thenAccept(c -> assertThat(c).containsExactly(OIHANA, ELAIA, OIHANA))

      );
   }

   public void testSet() {
      String key = getEntryKey();
      EmbeddedMultimapListCache<String, Person> list = getMultimapMember();
      await(
            list.offerLast(key, OIHANA)
                  .thenCompose(r1 -> list.offerLast(key, OIHANA))
                  .thenCompose(r1 -> list.offerLast(key, ELAIA))
                  .thenCompose(r1 -> list.offerLast(key, OIHANA))
                  .thenCompose(r1 -> list.index(key, 2))
                  .thenAccept(v -> assertThat(v).isEqualTo(ELAIA))

      );

      await(
            list.set(key, 2, OIHANA)
                  .thenCompose(r1 -> list.index(key, 2))
                  .thenAccept(v -> assertThat(v).isEqualTo(OIHANA))

      );

      // Set at the head.
      await(
            list.set(key, 0, ELAIA)
                  .thenCompose(r1 -> list.index(key, 0))
                  .thenAccept(v -> assertThat(v).isEqualTo(ELAIA))

      );

      // Set at the tail.
      await(
            list.set(key, -1, ELAIA)
                  .thenCompose(r1 -> list.index(key, -1))
                  .thenAccept(v -> assertThat(v).isEqualTo(ELAIA))

      );
   }

   public void testIndexOf() {
      String key = getEntryKey();
      EmbeddedMultimapListCache<String, Person> list = getMultimapMember();
      await(
            list.offerLast(key, OIHANA)
                  .thenCompose(r1 -> list.offerLast(key, OIHANA))
                  .thenCompose(r1 -> list.offerLast(key, ELAIA))
                  .thenCompose(r1 -> list.offerLast(key, OIHANA))
                  .thenCompose(r1 -> list.indexOf(key, OIHANA, 0L, null, null))
                  .thenAccept(v -> assertThat(v).containsExactly(0L, 1L, 3L))

      );
   }

   public void testInsert() {
      String key = getEntryKey();
      EmbeddedMultimapListCache<String, Person> list = getMultimapMember();
      await(
            list.offerLast(key, OIHANA)
                  .thenCompose(r1 -> list.insert(key, true, OIHANA, ELAIA))
                  .thenAccept(v -> assertThat(v).isEqualTo(2))

      );
   }

   public void testRemove() {
      String key = getEntryKey();
      EmbeddedMultimapListCache<String, Person> list = getMultimapMember();
      await(
            list.offerLast(key, OIHANA)
                  .thenCompose(ignore -> list.offerLast(key, OIHANA))
                  .thenCompose(r1 -> list.remove(key, 1, OIHANA))
                  .thenAccept(v -> assertThat(v).isEqualTo(1))
      );

      assertThat(await(list.size(key))).isOne();
   }

   public void testTrim() {
      String key = getEntryKey();
      EmbeddedMultimapListCache<String, Person> list = getMultimapMember();
      await(
            list.offerLast(key, OIHANA)
                  .thenCompose(r -> list.offerLast(key, ELAIA))
                  .thenCompose(r -> list.offerLast(key, RAMON))
                  .thenCompose(r -> list.trim(key, 1, 2))
                  .thenCompose(r -> list.subList(key, 0, -1))
                  .thenAccept(v -> assertThat(v).containsExactly(ELAIA, RAMON))
      );
   }

   public void testRotate() {
      String key = getEntryKey();
      EmbeddedMultimapListCache<String, Person> list = getMultimapMember();
      await(
            list.offerLast(key, OIHANA)
                  .thenCompose(r1 -> list.offerLast(key, ELAIA))
                  .thenCompose(r1 -> list.offerLast(key, RAMON))
      );

      await(
            list.rotate(key, false)
                  .thenAccept(v -> assertThat(v).isEqualTo(RAMON))

      );
      await(
            list.subList(key, 0, -1)
                  .thenAccept(v -> assertThat(v).containsExactly(RAMON, OIHANA, ELAIA))

      );
   }

   public void testReplace() {
      String key = getEntryKey();
      EmbeddedMultimapListCache<String, Person> list = getMultimapMember();
      await(
            list.offerLast(key, OIHANA)
                  .thenCompose(r1 -> list.offerLast(key, ELAIA))
                  .thenCompose(r1 -> list.offerLast(key, RAMON))
      );

      await(
            list.subList(key, 0, -1)
                  .thenAccept(v -> assertThat(v).containsExactly(OIHANA, ELAIA, RAMON))

      );

      await(
            list.replace(key, Arrays.asList(FELIX))
                  .thenAccept(s -> assertThat(s).isEqualTo(1))
      );

      await(
            list.subList(key, 0, -1)
                  .thenAccept(v -> assertThat(v).containsExactly(FELIX))

      );
   }

   protected void assertValuesAndOwnership(String key, Person value) {
      assertOwnershipAndNonOwnership(key, l1CacheEnabled);
      assertOnAllCaches(key, value);
   }

   protected void assertOnAllCaches(Object key, Person value) {
      for (Map.Entry<Address, EmbeddedMultimapListCache<String, Person>> entry : cluster.entrySet()) {
         await(entry.getValue().get((String) key).thenAccept(v -> {
            assertThat(v)
                  .isNotNull()
                  .containsOnlyOnce(value);
               })
         );
      }
   }
}
