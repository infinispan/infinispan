package org.infinispan.multimap.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.multimap.impl.MultimapTestUtils.FELIX;
import static org.infinispan.multimap.impl.MultimapTestUtils.KOLDO;
import static org.infinispan.multimap.impl.MultimapTestUtils.OIHANA;
import static org.infinispan.multimap.impl.MultimapTestUtils.RAMON;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
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

   protected void assertFromAllCaches(String key, Map<String, Person> expected) {
      for (EmbeddedMultimapPairCache<String, byte[], Person> multimap : pairCacheCluster.values()) {
         assertThat(await(multimap.get(key)
               .thenApply(DistributedMultimapPairCacheTest::convertKeys)))
               .containsAllEntriesOf(expected);
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
}
