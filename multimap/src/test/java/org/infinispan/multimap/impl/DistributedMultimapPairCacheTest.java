package org.infinispan.multimap.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.multimap.impl.MultimapTestUtils.FELIX;
import static org.infinispan.multimap.impl.MultimapTestUtils.KOLDO;
import static org.infinispan.multimap.impl.MultimapTestUtils.OIHANA;
import static org.infinispan.multimap.impl.MultimapTestUtils.RAMON;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.test.data.Person;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DistributedMultimapPairCacheTest")
public class DistributedMultimapPairCacheTest extends BaseDistributedMultimapTest<EmbeddedMultimapPairCache<String, byte[], Person>, Map<byte[], Person>> {

   @Override
   protected EmbeddedMultimapPairCache<String, byte[], Person> create(EmbeddedMultimapCacheManager<String, Map<byte[], Person>> manager) {
      return manager.getMultimapPair(cacheName);
   }

   @Override
   public Object[] factory() {
      return super.factory(DistributedMultimapPairCacheTest::new);
   }

   public void testSetGetOperations() {
      String key = getEntryKey();
      EmbeddedMultimapPairCache<String, byte[], Person> multimap = getMultimapMember();
      assertThat(await(multimap.set(key, Map.entry(toBytes("oihana"), OIHANA))))
            .isEqualTo(1);
      assertThat(await(multimap.set(key, Map.entry(toBytes("koldo"), KOLDO), Map.entry(toBytes("felix"), RAMON))))
            .isEqualTo(2);

      assertThat(await(multimap.set(key, Map.entry(toBytes("felix"), FELIX)))).isZero();

      assertFromAllCaches(key, Map.of("oihana", OIHANA, "koldo", KOLDO, "felix", FELIX));
      assertThat(await(multimap.size(key))).isEqualTo(3);

      assertThat(await(multimap.get(key, toBytes("oihana")))).isEqualTo(OIHANA);
      assertThat(await(multimap.get(key, toBytes("unknown")))).isNull();
      assertThat(await(multimap.get("unknown", toBytes("unknown")))).isNull();
   }

   public void testSizeOperation() {
      String key = getEntryKey();
      EmbeddedMultimapPairCache<String, byte[], Person> multimap = getMultimapMember();
      CompletionStage<Integer> cs = multimap.set(key, Map.entry(toBytes("oihana"), OIHANA), Map.entry(toBytes("ramon"), RAMON))
            .thenCompose(ignore -> multimap.size(key));
      assertThat(await(cs)).isEqualTo(2);
   }

   public void testKeySetOperation() {
      String key = getEntryKey();
      EmbeddedMultimapPairCache<String, byte[], Person> multimap = getMultimapMember();
      CompletionStage<List<String>> cs = multimap.set(key, Map.entry(toBytes("oihana"), OIHANA), Map.entry(toBytes("koldo"), KOLDO))
            .thenCompose(ignore -> multimap.keySet(key))
            .thenApply(s -> s.stream().map(String::new).toList());
      assertThat(await(cs)).containsOnly("oihana", "koldo");
   }

   public void testSetAndRemove() {
      String key = getEntryKey();
      EmbeddedMultimapPairCache<String, byte[], Person> multimap = getMultimapMember();
      await(multimap.set(key, Map.entry(toBytes("oihana"), OIHANA), Map.entry(toBytes("koldo"), KOLDO))
            .thenCompose(ignore -> multimap.remove(key, toBytes("oihana")))
            .thenCompose(v -> {
               assertThat(v).isEqualTo(1);
               return multimap.remove(key, toBytes("koldo"), toBytes("ramon"));
            })
            .thenAccept(v -> assertThat(v).isEqualTo(1)));
      assertThatThrownBy(() -> await(multimap.remove(key, null)))
            .isInstanceOf(NullPointerException.class);
      assertThatThrownBy(() -> await(multimap.remove(null, new byte[] { 1 })))
            .isInstanceOf(NullPointerException.class);
   }

   protected void assertFromAllCaches(String key, Map<String, Person> expected) {
      for (EmbeddedMultimapPairCache<String, byte[], Person> multimap : cluster.values()) {
         assertThat(await(multimap.get(key)
               .thenApply(DistributedMultimapPairCacheTest::convertKeys)))
               .containsAllEntriesOf(expected);
      }
   }

   protected void assertFromAllCaches(Function<EmbeddedMultimapPairCache<String, byte[], Person>, CompletionStage<?>> f) {
      for (EmbeddedMultimapPairCache<String, byte[], Person> multimap : cluster.values()) {
         await(f.apply(multimap));
      }
   }

   private static byte[] toBytes(String s) {
      return s.getBytes();
   }

   private static Map<String, Person> convertKeys(Map<byte[], Person> map) {
      return map.entrySet().stream()
            .map(e -> Map.entry(toString(e.getKey()), e.getValue()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
   }

   private static String toString(byte[] b) {
      return new String(b);
   }

   public void testValuesOperation() {
      String key = getEntryKey();
      EmbeddedMultimapPairCache<String, byte[], Person> multimap = getMultimapMember();
      CompletionStage<Collection<Person>> cs = multimap.set(key, Map.entry(toBytes("oihana"), OIHANA), Map.entry(toBytes("koldo"), KOLDO))
                  .thenCompose(ignore -> multimap.values(key));
      assertThat(await(cs)).containsOnly(OIHANA, KOLDO);
   }

   public void testContainsProperty() {
      String key = getEntryKey();
      EmbeddedMultimapPairCache<String, byte[], Person> multimap = getMultimapMember();
      await(multimap.set(key, Map.entry(toBytes("oihana"), OIHANA))
            .thenAccept(ignore -> assertFromAllCaches(key, Map.of("oihana", OIHANA)))
            .thenCompose(ignore -> multimap.contains(key, toBytes("oihana"))
            .thenCompose(b -> {
               assertThat(b).isTrue();
               return multimap.contains(key, toBytes("unknown"));
            })
            .thenCompose(b -> {
               assertThat(b).isFalse();
               return multimap.contains("unknown", toBytes("unknown"));
            })
            .thenAccept(b -> assertThat(b).isFalse())));
   }

   public void testCompute() {
      String key = getEntryKey();
      EmbeddedMultimapPairCache<String, byte[], Person> multimap = getMultimapMember();
      byte[] oihana = toBytes("oihana");
      await(multimap.set(key, Map.entry(oihana, OIHANA))
            .thenAccept(ignore -> assertFromAllCaches(key, Map.of("oihana", OIHANA)))
            .thenCompose(ignore -> multimap.compute(key, oihana, (k, v) -> {
               assertThat(k).isEqualTo(oihana);
               assertThat(v).isEqualTo(OIHANA);
               return FELIX;
            }))
            .thenAccept(ignore -> assertFromAllCaches(key, Map.of("oihana", FELIX))));
   }

   public void testSubSelect() {
      String key = getEntryKey();
      // Not existent key.
      assertFromAllCaches(m -> m.subSelect("something-not-existent", 10)
            .thenAccept(v -> assertThat(v).isNull()));
      assertFromAllCaches(multimap -> multimap.set(key, Map.entry(toBytes("oihana"), OIHANA), Map.entry(toBytes("koldo"), KOLDO))
            .thenCompose(ignore -> multimap.subSelect(key, 1))
            .thenAccept(m ->
                  assertThat(convertKeys(m))
                        .hasSize(1)
                        .containsAnyOf(Map.entry("oihana", OIHANA), Map.entry("koldo", KOLDO))));
   }

   public void testGetMultiple() {
      String key = getEntryKey();
      assertFromAllCaches(multimap -> multimap.set(key, Map.entry(toBytes("oihana"), OIHANA), Map.entry(toBytes("koldo"), KOLDO))
            .thenCompose(ignore -> multimap.get(key, toBytes("oihana"), toBytes("koldo")))
            .thenAccept(m -> assertThat(convertKeys(m))
                  .hasSize(2)
                  .containsEntry("oihana", OIHANA)
                  .containsEntry("koldo", KOLDO)));
   }
}
