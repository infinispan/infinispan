package org.infinispan.multimap.impl;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.multimap.impl.MultimapTestUtils.ELAIA;
import static org.infinispan.multimap.impl.MultimapTestUtils.FELIX;
import static org.infinispan.multimap.impl.MultimapTestUtils.OIHANA;
import static org.infinispan.multimap.impl.MultimapTestUtils.RAMON;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.remoting.transport.Address;
import org.infinispan.test.data.Person;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DistributedSetTest")
public class DistributedSetTest extends BaseDistributedMultimapTest<EmbeddedSetCache<String, Person>, Person> {

   @Override
   protected EmbeddedSetCache<String, Person> create(EmbeddedMultimapCacheManager<String, Person> manager) {
      return manager.getMultimapSet(cacheName);
   }

   @Override
   public Object[] factory() {
      return super.factory(DistributedSetTest::new);
   }

   @Test
   public void testAdd() {
      String key = getEntryKey();
      EmbeddedSetCache<String, Person> set = getMultimapMember();
      await(set.add(key, OIHANA));
      assertValuesAndOwnership(key, OIHANA);

      await(set.add(key, ELAIA));
      assertValuesAndOwnership(key, ELAIA);

   }

   @Test
   public void testGet() {
      String key = getEntryKey();
      EmbeddedSetCache<String, Person> set = getMultimapMember();

      await(set.add(key, List.of(OIHANA, ELAIA)));

      Set<Person> actual = await(set.get(key)).toSet();
      assertThat(actual).containsExactlyInAnyOrder(ELAIA, OIHANA);
   }

   @Test
   public void testSize() {
      String key = getEntryKey();
      EmbeddedSetCache<String, Person> set = getMultimapMember();
      await(
            set.add(key, OIHANA)
                  .thenCompose(r1 -> set.add(key, ELAIA))
                  .thenCompose(r1 -> set.add(key, FELIX))
                  .thenCompose(r1 -> set.add(key, OIHANA))
                  .thenCompose(r1 -> set.size(key))
                  .thenAccept(size -> assertThat(size).isEqualTo(3))
      );
   }

   @Test
   public void testSet() {
      String key = getEntryKey();
      EmbeddedSetCache<String, Person> set = getMultimapMember();
      Set<Person> pers = Set.of(OIHANA, ELAIA);
      await(set.set(key, pers));

      assertValuesAndOwnership(key, OIHANA);
      assertValuesAndOwnership(key, ELAIA);
   }

   @Test
   public void testPutAll() {
      String key = getEntryKey();

      EmbeddedSetCache<String, Person> set = getMultimapMember();
      Set<Person> pers = Set.of(OIHANA, ELAIA);
      await(set.set(key, pers));

      String name_key2 = "names2";
      Set<Person> pers2 = Set.of(FELIX, RAMON);
      await(set.set(key, pers));
      await(set.set(name_key2, pers2));

      assertValuesAndOwnership(key, OIHANA);
      assertValuesAndOwnership(key, ELAIA);

      assertValuesAndOwnership(name_key2, FELIX);
      assertValuesAndOwnership(name_key2, RAMON);
   }

   protected void assertValuesAndOwnership(String key, Person value) {
      assertOwnershipAndNonOwnership(key, l1CacheEnabled);
      assertOnAllCaches(key, value);
   }

   protected void assertOnAllCaches(Object key, Person value) {
      for (Map.Entry<Address, EmbeddedSetCache<String, Person>> entry : cluster.entrySet()) {
         var set = await(entry.getValue().get((String) key));
         assertNotNull(format("values on the key %s must be not null", key), set);
         assertTrue(format("values on the key '%s' must contain '%s' on node '%s'", key, value, entry.getKey()),
                  set.contains(value));
      }
   }
}
