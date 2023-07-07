package org.infinispan.multimap.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.multimap.impl.MultimapTestUtils.FELIX;
import static org.infinispan.multimap.impl.MultimapTestUtils.KOLDO;
import static org.infinispan.multimap.impl.MultimapTestUtils.OIHANA;
import static org.infinispan.multimap.impl.MultimapTestUtils.RAMON;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.multimap.api.embedded.EmbeddedMultimapCacheManagerFactory;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.data.Person;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DistributedMultimapPairCacheTest")
public class DistributedMultimapPairCacheTest extends BaseDistFunctionalTest<String, Map<byte[], Person>> {

   protected Map<Address, EmbeddedMultimapPairCache<String, byte[], Person>> pairCacheCluster = new HashMap<>();

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();

      for (EmbeddedCacheManager cacheManager : cacheManagers) {
         EmbeddedMultimapCacheManager multimapCacheManager = (EmbeddedMultimapCacheManager) EmbeddedMultimapCacheManagerFactory.from(cacheManager);
         pairCacheCluster.put(cacheManager.getAddress(), multimapCacheManager.getMultimapPair(cacheName));
      }
   }

   @Override
   protected SerializationContextInitializer getSerializationContext() {
      return MultimapSCI.INSTANCE;
   }

   private EmbeddedMultimapPairCache<String, byte[], Person> getMultimapMember() {
      return pairCacheCluster.values()
            .stream().findFirst().orElseThrow(() -> new IllegalStateException("No multimap cluster found!"));
   }

   public void testSetGetOperations() {
      EmbeddedMultimapPairCache<String, byte[], Person> multimap = getMultimapMember();
      assertThat(await(multimap.set("person1", Map.entry(toBytes("oihana"), OIHANA))))
            .isEqualTo(1);
      assertThat(await(multimap.set("person1", Map.entry(toBytes("koldo"), KOLDO), Map.entry(toBytes("felix"), RAMON))))
            .isEqualTo(2);

      assertThat(await(multimap.set("person1", Map.entry(toBytes("felix"), FELIX)))).isZero();

      assertFromAllCaches("person1", Map.of("oihana", OIHANA, "koldo", KOLDO, "felix", FELIX));

      assertThat(await(multimap.get("person1", toBytes("oihana")))).isEqualTo(OIHANA);
      assertThat(await(multimap.get("person1", toBytes("unknown")))).isNull();
      assertThat(await(multimap.get("unknown", toBytes("unknown")))).isNull();
   }

   public void testSizeOperation() {
      EmbeddedMultimapPairCache<String, byte[], Person> multimap = getMultimapMember();
      CompletionStage<Integer> cs = multimap.set("size-test", Map.entry(toBytes("oihana"), OIHANA), Map.entry(toBytes("ramon"), RAMON))
            .thenCompose(ignore -> multimap.size("size-test"));
      assertThat(await(cs)).isEqualTo(2);
   }

   public void testKeySetOperation() {
      EmbeddedMultimapPairCache<String, byte[], Person> multimap = getMultimapMember();
      CompletionStage<Set<String>> cs = multimap.set("keyset-test", Map.entry(toBytes("oihana"), OIHANA), Map.entry(toBytes("koldo"), KOLDO))
            .thenCompose(ignore -> multimap.keySet("keyset-test"))
            .thenApply(s -> s.stream().map(String::new).collect(Collectors.toSet()));
      assertThat(await(cs)).contains("oihana", "koldo");
   }

   public void testSetAndRemove() {
      EmbeddedMultimapPairCache<String, byte[], Person> multimap = getMultimapMember();
      await(multimap.set("set-and-remove", Map.entry(toBytes("oihana"), OIHANA), Map.entry(toBytes("koldo"), KOLDO))
            .thenCompose(ignore -> multimap.remove("set-and-remove", toBytes("oihana")))
            .thenCompose(v -> {
               assertThat(v).isEqualTo(1);
               return multimap.remove("set-and-remove", toBytes("koldo"), toBytes("ramon"));
            })
            .thenAccept(v -> assertThat(v).isEqualTo(1)));
      assertThatThrownBy(() -> await(multimap.remove("set-and-remove", null)))
            .isInstanceOf(NullPointerException.class);
      assertThatThrownBy(() -> await(multimap.remove(null, new byte[] { 1 })))
            .isInstanceOf(NullPointerException.class);
   }

   protected void assertFromAllCaches(String key, Map<String, Person> expected) {
      for (EmbeddedMultimapPairCache<String, byte[], Person> multimap : pairCacheCluster.values()) {
         assertThat(await(multimap.get(key)
               .thenApply(DistributedMultimapPairCacheTest::convertKeys)))
               .containsAllEntriesOf(expected);
      }
   }

   protected void assertFromAllCaches(Function<EmbeddedMultimapPairCache<String, byte[], Person>, CompletionStage<?>> f) {
      for (EmbeddedMultimapPairCache<String, byte[], Person> multimap : pairCacheCluster.values()) {
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
      EmbeddedMultimapPairCache<String, byte[], Person> multimap = getMultimapMember();
      CompletionStage<Collection<Person>> cs = multimap.set("values-test", Map.entry(toBytes("oihana"), OIHANA), Map.entry(toBytes("koldo"), KOLDO))
                  .thenCompose(ignore -> multimap.values("values-test"));
      assertThat(await(cs)).contains(OIHANA, KOLDO);
   }

   public void testContainsProperty() {
      EmbeddedMultimapPairCache<String, byte[], Person> multimap = getMultimapMember();
      await(multimap.set("contains-test", Map.entry(toBytes("oihana"), OIHANA))
            .thenAccept(ignore -> assertFromAllCaches("contains-test", Map.of("oihana", OIHANA)))
            .thenCompose(ignore -> multimap.contains("contains-test", toBytes("oihana"))
            .thenCompose(b -> {
               assertThat(b).isTrue();
               return multimap.contains("contains-test", toBytes("unknown"));
            })
            .thenCompose(b -> {
               assertThat(b).isFalse();
               return multimap.contains("unknown", toBytes("unknown"));
            })
            .thenAccept(b -> assertThat(b).isFalse())));
   }

   public void testCompute() {
      EmbeddedMultimapPairCache<String, byte[], Person> multimap = getMultimapMember();
      byte[] oihana = toBytes("oihana");
      await(multimap.set("compute-test", Map.entry(oihana, OIHANA))
            .thenAccept(ignore -> assertFromAllCaches("compute-test", Map.of("oihana", OIHANA)))
            .thenCompose(ignore -> multimap.compute("compute-test", oihana, (k, v) -> {
               assertThat(k).isEqualTo(oihana);
               assertThat(v).isEqualTo(OIHANA);
               return FELIX;
            }))
            .thenAccept(ignore -> assertFromAllCaches("compute-test", Map.of("oihana", FELIX))));
   }

   public void testSubSelect() {
      // Not existent key.
      assertFromAllCaches(m -> m.subSelect("something-not-existent", 10)
            .thenAccept(v -> assertThat(v).isNull()));
      assertFromAllCaches(multimap -> multimap.set("sub-select-test", Map.entry(toBytes("oihana"), OIHANA), Map.entry(toBytes("koldo"), KOLDO))
            .thenCompose(ignore -> multimap.subSelect("sub-select-test", 1))
            .thenAccept(m ->
                  assertThat(convertKeys(m))
                        .hasSize(1)
                        .containsAnyOf(Map.entry("oihana", OIHANA), Map.entry("koldo", KOLDO))));
   }

   public void testGetMultiple() {
      assertFromAllCaches(multimap -> multimap.set("get-multiple-test", Map.entry(toBytes("oihana"), OIHANA), Map.entry(toBytes("koldo"), KOLDO))
            .thenCompose(ignore -> multimap.get("get-multiple-test", toBytes("oihana"), toBytes("koldo")))
            .thenAccept(m -> assertThat(convertKeys(m))
                  .hasSize(2)
                  .containsEntry("oihana", OIHANA)
                  .containsEntry("koldo", KOLDO)));
   }
}
