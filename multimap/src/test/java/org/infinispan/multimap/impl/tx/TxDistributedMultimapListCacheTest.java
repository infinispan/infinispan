package org.infinispan.multimap.impl.tx;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.multimap.impl.MultimapTestUtils.ELAIA;
import static org.infinispan.multimap.impl.MultimapTestUtils.FELIX;
import static org.infinispan.multimap.impl.MultimapTestUtils.OIHANA;
import static org.infinispan.multimap.impl.MultimapTestUtils.PEPE;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.BaseDistributedMultimapTest;
import org.infinispan.multimap.impl.EmbeddedMultimapCacheManager;
import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.test.data.Person;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.tx.TxDistributedMultimapPairCacheTest")
public class TxDistributedMultimapListCacheTest extends BaseDistributedMultimapTest<EmbeddedMultimapListCache<String, Person>, Person> {

   @Override
   protected EmbeddedMultimapListCache<String, Person> create(EmbeddedMultimapCacheManager<String, Person> manager) {
      return manager.getMultimapList(cacheName);
   }

   @Override
   public Object[] factory() {
      return transactionalFactory(TxDistributedMultimapListCacheTest::new);
   }

   @Test(dataProvider = "booleans")
   public void testRollbackOfferOperation(boolean front) throws Throwable {
      String key = getEntryKey();
      EmbeddedMultimapListCache<String, Person> list = getMultimapMember();
      await(list.offerFirst(key, OIHANA));

      tm(0, cacheName).begin();
      CompletionStage<Void> cs;
      if (front) {
         cs = list.offerFirst(key, ELAIA);
      } else {
         cs = list.offerLast(key, ELAIA);
      }
      await(cs);
      tm(0, cacheName).rollback();

      assertThat(await(list.get(key)))
            .hasSize(1)
            .containsOnlyOnce(OIHANA);
   }

   @Test(dataProvider = "booleans")
   public void testRollbackPollOperation(boolean front) throws Throwable {
      String key = getEntryKey();
      EmbeddedMultimapListCache<String, Person> list = getMultimapMember();
      await(list.offerFirst(key, List.of(OIHANA, ELAIA)));

      tm(0, cacheName).begin();
      CompletionStage<?> cs;
      if (front) {
         cs = list.pollFirst(key, 1);
      } else {
         cs = list.pollLast(key, 1);
      }
      await(cs);
      tm(0, cacheName).rollback();

      assertThat(await(list.get(key)))
            .hasSize(2)
            .containsOnlyOnce(OIHANA, ELAIA);
   }

   public void testRollbackSetOperation() throws Throwable {
      String key = getEntryKey();
      EmbeddedMultimapListCache<String, Person> list = getMultimapMember();
      await(list.offerFirst(key, List.of(OIHANA, ELAIA)));

      tm(0, cacheName).begin();
      await(list.set(key, 1, FELIX));
      tm(0, cacheName).rollback();

      assertThat(await(list.get(key)))
            .hasSize(2)
            .containsOnlyOnce(OIHANA, ELAIA);
   }

   @Test(dataProvider = "booleans")
   public void testRollbackInsertOperation(boolean front) throws Throwable {
      String key = getEntryKey();
      EmbeddedMultimapListCache<String, Person> list = getMultimapMember();
      await(list.offerFirst(key, List.of(OIHANA, ELAIA)));

      tm(0, cacheName).begin();
      await(list.insert(key, front, ELAIA, FELIX));
      tm(0, cacheName).rollback();

      assertThat(await(list.get(key)))
            .hasSize(2)
            .containsOnlyOnce(OIHANA, ELAIA);
   }

   public void testRollbackRemoveOperation() throws Throwable {
      String key = getEntryKey();
      EmbeddedMultimapListCache<String, Person> list = getMultimapMember();
      await(list.offerFirst(key, List.of(OIHANA, ELAIA)));

      tm(0, cacheName).begin();
      await(list.remove(key, 1, ELAIA));
      tm(0, cacheName).rollback();

      assertThat(await(list.get(key)))
            .hasSize(2)
            .containsOnlyOnce(OIHANA, ELAIA);
   }

   public void testRollbackTrimOperation() throws Throwable {
      String key = getEntryKey();
      EmbeddedMultimapListCache<String, Person> list = getMultimapMember();
      await(list.offerFirst(key, List.of(OIHANA, ELAIA, FELIX)));

      tm(0, cacheName).begin();
      await(list.trim(key, 0, 1));
      tm(0, cacheName).rollback();

      assertThat(await(list.get(key)))
            .hasSize(3)
            .containsOnlyOnce(OIHANA, ELAIA, FELIX);
   }

   public void testRollbackReplaceOperation() throws Throwable {
      String key = getEntryKey();
      EmbeddedMultimapListCache<String, Person> list = getMultimapMember();
      await(list.offerFirst(key, List.of(OIHANA, ELAIA)));

      tm(0, cacheName).begin();
      await(list.replace(key, List.of(FELIX, PEPE)));
      tm(0, cacheName).rollback();

      assertThat(await(list.get(key)))
            .hasSize(2)
            .containsOnlyOnce(OIHANA, ELAIA);
   }

   @Test(dataProvider = "booleans")
   public void testRollbackRotateOperation(boolean front) throws Throwable {
      String key = getEntryKey();
      EmbeddedMultimapListCache<String, Person> list = getMultimapMember();
      await(list.offerLast(key, List.of(OIHANA, ELAIA, FELIX)));

      tm(0, cacheName).begin();
      await(list.rotate(key, front));
      tm(0, cacheName).rollback();

      assertThat(await(list.get(key)))
            .hasSize(3)
            .containsExactly(OIHANA, ELAIA, FELIX);
   }

   @DataProvider(name = "booleans")
   Object[][] booleans() {
      return new Object[][]{
            {Boolean.TRUE}, {Boolean.FALSE}};
   }
}
