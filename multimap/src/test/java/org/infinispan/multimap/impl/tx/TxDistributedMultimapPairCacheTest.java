package org.infinispan.multimap.impl.tx;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.multimap.impl.MultimapTestUtils.ELAIA;
import static org.infinispan.multimap.impl.MultimapTestUtils.FELIX;
import static org.infinispan.multimap.impl.MultimapTestUtils.OIHANA;

import java.util.Map;

import org.infinispan.multimap.impl.BaseDistributedMultimapTest;
import org.infinispan.multimap.impl.EmbeddedMultimapCacheManager;
import org.infinispan.multimap.impl.EmbeddedMultimapPairCache;
import org.infinispan.test.data.Person;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.tx.TxDistributedMultimapPairCacheTest")
public class TxDistributedMultimapPairCacheTest extends BaseDistributedMultimapTest<EmbeddedMultimapPairCache<String, String, Person>, Map<String, Person>> {

   @Override
   protected EmbeddedMultimapPairCache<String, String, Person> create(EmbeddedMultimapCacheManager<String, Map<String, Person>> manager) {
      return manager.getMultimapPair(cacheName);
   }

   @Override
   public Object[] factory() {
      return transactionalFactory(TxDistributedMultimapPairCacheTest::new);
   }

   public void testRollbackSetOperation() throws Throwable {
      String key = getEntryKey();
      EmbeddedMultimapPairCache<String, String, Person> multimap = getMultimapMember();
      assertThat(await(multimap.set(key, Map.entry("oihana", OIHANA))))
            .isEqualTo(1);

      assertThat(await(multimap.size(key))).isOne();

      tm(0, cacheName).begin();
      await(multimap.set(key, Map.entry("felix", FELIX)));
      tm(0, cacheName).rollback();

      assertThat(await(multimap.size(key))).isOne();
      assertThat(await(multimap.keySet(key))).containsOnlyOnce("oihana");
   }

   public void testRollbackRemoveOperation() throws Throwable {
      String key = getEntryKey();
      EmbeddedMultimapPairCache<String, String, Person> multimap = getMultimapMember();
      assertThat(await(multimap.set(key, Map.entry("oihana", OIHANA), Map.entry("elaia", ELAIA))))
            .isEqualTo(2);

      assertThat(await(multimap.size(key))).isEqualTo(2);

      tm(0, cacheName).begin();
      await(multimap.remove(key, "oihana"));
      tm(0, cacheName).rollback();

      assertThat(await(multimap.size(key))).isEqualTo(2);
      assertThat(await(multimap.keySet(key)))
            .containsOnlyOnce("oihana", "elaia");
   }

   public void testRollbackComputeOperation() throws Throwable {
      String key = getEntryKey();
      EmbeddedMultimapPairCache<String, String, Person> multimap = getMultimapMember();
      assertThat(await(multimap.set(key, Map.entry("oihana", OIHANA))))
            .isEqualTo(1);

      assertThat(await(multimap.size(key))).isOne();

      tm(0, cacheName).begin();
      await(multimap.compute(key, "oihana", (k, v) -> {
         assertThat(k).isEqualTo("oihana");
         assertThat((Object) v).isEqualTo(OIHANA);
         return ELAIA;
      }));
      tm(0, cacheName).rollback();

      assertThat(await(multimap.size(key))).isOne();
      assertThat((Object) await(multimap.get(key, "oihana"))).isEqualTo(OIHANA);
   }

   public void testRollbackSetIfAbsentOperation() throws Throwable {
      String key = getEntryKey();
      EmbeddedMultimapPairCache<String, String, Person> multimap = getMultimapMember();
      assertThat(await(multimap.set(key, Map.entry("oihana", OIHANA))))
            .isEqualTo(1);

      assertThat(await(multimap.size(key))).isOne();

      tm(0, cacheName).begin();
      await(multimap.setIfAbsent(key, "felix", FELIX));
      tm(0, cacheName).rollback();

      assertThat(await(multimap.size(key))).isOne();
      assertThat(await(multimap.keySet(key))).containsOnlyOnce("oihana");
   }
}
