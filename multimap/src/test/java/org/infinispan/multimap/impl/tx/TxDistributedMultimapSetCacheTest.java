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
import org.infinispan.multimap.impl.EmbeddedSetCache;
import org.infinispan.test.data.Person;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.tx.TxDistributedMultimapSetCacheTest")
public class TxDistributedMultimapSetCacheTest extends BaseDistributedMultimapTest<EmbeddedSetCache<String, Person>, Person> {

   @Override
   protected EmbeddedSetCache<String, Person> create(EmbeddedMultimapCacheManager<String, Person> manager) {
      return manager.getMultimapSet(cacheName);
   }

   @Override
   public Object[] factory() {
      return super.transactionalFactory(TxDistributedMultimapSetCacheTest::new);
   }

   public void testRollbackAddOperation() throws Throwable {
      String key = getEntryKey();
      EmbeddedSetCache<String, Person> set = getMultimapMember();

      assertThat(await(set.add(key, OIHANA))).isOne();

      runAndRollback(() -> set.add(key, List.of(ELAIA, FELIX)));

      Collection<Person> actual = await(set.getAsSet(key));
      assertThat(actual)
            .hasSize(1)
            .containsOnlyOnce(OIHANA);
   }

   public void testRollbackRemoveOperation() throws Throwable {
      String key = getEntryKey();
      EmbeddedSetCache<String, Person> set = getMultimapMember();

      await(set.add(key, List.of(OIHANA, ELAIA, FELIX)));

      runAndRollback(() -> set.remove(key, List.of(ELAIA, FELIX, PEPE)));

      Collection<Person> actual = await(set.getAsSet(key));
      assertThat(actual)
            .hasSize(3)
            .containsExactlyInAnyOrder(OIHANA, ELAIA, FELIX);
   }

   public void testRollbackSetOperation() throws Throwable {
      String key = getEntryKey();
      EmbeddedSetCache<String, Person> set = getMultimapMember();

      await(set.add(key, List.of(OIHANA, ELAIA)));

      runAndRollback(() -> set.set(key, List.of(FELIX, PEPE)));

      Collection<Person> actual = await(set.getAsSet(key));
      assertThat(actual)
            .hasSize(2)
            .containsExactlyInAnyOrder(OIHANA, ELAIA);
   }

   @Test(dataProvider = "pop-args")
   public void testRollbackPopOperation(long count) throws Throwable {
      String key = getEntryKey();
      EmbeddedSetCache<String, Person> set = getMultimapMember();

      await(set.add(key, List.of(OIHANA, ELAIA, PEPE)));

      runAndRollback(() -> set.pop(key, count, true));

      Collection<Person> actual = await(set.getAsSet(key));
      assertThat(actual)
            .hasSize(3)
            .containsExactlyInAnyOrder(OIHANA, ELAIA, PEPE);
   }

   @DataProvider(name = "pop-args")
   Object[][] popArgs() {
      return new Object[][]{
            {1L},
            {2L},
            {Long.MAX_VALUE},
      };
   }
}
