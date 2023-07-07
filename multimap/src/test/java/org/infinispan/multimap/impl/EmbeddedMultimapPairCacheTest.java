package org.infinispan.multimap.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.functional.FunctionalTestUtils.await;

import java.util.Map;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "multimap.impl.EmbeddedMultimapPairCacheTest")
public class EmbeddedMultimapPairCacheTest extends SingleCacheManagerTest {

   EmbeddedMultimapPairCache<String, String, String> embeddedPairCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(MultimapSCI.INSTANCE);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      cm.createCache("test", builder.build());
      embeddedPairCache = new EmbeddedMultimapPairCache<>(cm.getCache("test"));
      return cm;
   }

   public void testSetGetOperations() {
      assertThat(await(embeddedPairCache.set("person1", Map.entry("name", "Oihana"))))
            .isEqualTo(1);
      assertThat(await(embeddedPairCache.set("person1", Map.entry("age", "1"), Map.entry("birthday", "2023-05-26"))))
            .isEqualTo(2);

      assertThat(await(embeddedPairCache.set("person1", Map.entry("name", "Ramon"))))
            .isEqualTo(0);

      Map<String, String> person1 = await(embeddedPairCache.get("person1"));
      assertThat(person1)
            .containsEntry("name", "Ramon")
            .containsEntry("age", "1")
            .containsEntry("birthday", "2023-05-26")
            .hasSize(3);

      assertThat(await(embeddedPairCache.get("person1", "name"))).isEqualTo("Ramon");
      assertThat(await(embeddedPairCache.get("person1", "unknown"))).isNull();
      assertThat(await(embeddedPairCache.get("unknown", "unknown"))).isNull();

      assertThatThrownBy(() -> await(embeddedPairCache.get(null, "property")))
            .isInstanceOf(NullPointerException.class);
      assertThatThrownBy(() -> await(embeddedPairCache.get("person1", (String) null)))
            .isInstanceOf(NullPointerException.class);
   }

   public void testSizeOperation() {
      assertThat(await(embeddedPairCache.set("size-test", Map.entry("name", "Oihana"), Map.entry("age", "1"))))
            .isEqualTo(2);

      assertThat(await(embeddedPairCache.size("size-test"))).isEqualTo(2);
      assertThat(await(embeddedPairCache.size("unknown"))).isEqualTo(0);

      assertThatThrownBy(() -> await(embeddedPairCache.size(null)))
            .isInstanceOf(NullPointerException.class);
   }

   public void testKeySetOperation() {
      assertThat(await(embeddedPairCache.set("keyset-test", Map.entry("name", "Oihana"), Map.entry("age", "1"))))
            .isEqualTo(2);

      assertThat(await(embeddedPairCache.keySet("keyset-test"))).contains("name", "age");
      assertThat(await(embeddedPairCache.keySet("unknown-entry"))).isEmpty();

      assertThatThrownBy(() -> await(embeddedPairCache.keySet(null)))
            .isInstanceOf(NullPointerException.class);
   }

   public void testValuesOperation() {
      assertThat(await(embeddedPairCache.set("values-test", Map.entry("name", "Oihana"), Map.entry("age", "1"))))
            .isEqualTo(2);

      assertThat(await(embeddedPairCache.values("values-test"))).contains("Oihana", "1");
      assertThat(await(embeddedPairCache.values("unknown-entry"))).isEmpty();

      assertThatThrownBy(() -> await(embeddedPairCache.values(null)))
            .isInstanceOf(NullPointerException.class);
   }

   public void testContainsProperty() {
      assertThat(await(embeddedPairCache.set("contains-test", Map.entry("name", "Oihana"))))
            .isEqualTo(1);

      assertThat(await(embeddedPairCache.contains("contains-test", "name")))
            .isTrue();
      assertThat(await(embeddedPairCache.contains("contains-test", "age")))
            .isFalse();
      assertThat(await(embeddedPairCache.contains("unknown-entry", "name")))
            .isFalse();

      assertThatThrownBy(() -> await(embeddedPairCache.contains(null, "name")))
            .isInstanceOf(NullPointerException.class);
      assertThatThrownBy(() -> await(embeddedPairCache.contains("contains-test", null)))
            .isInstanceOf(NullPointerException.class);
   }

   public void testSetAndRemove() {
      // Does not exist
      assertThat(await(embeddedPairCache.remove("some-unknown-key", "name"))).isZero();

      assertThat(await(embeddedPairCache.set("set-and-remove", Map.entry("k1", "v1"), Map.entry("k2", "v2"), Map.entry("k3", "v3"))))
            .isEqualTo(3);

      assertThat(await(embeddedPairCache.remove("set-and-remove", "k1"))).isEqualTo(1);
      assertThat(await(embeddedPairCache.remove("set-and-remove", "k2", "k3", "k4"))).isEqualTo(2);

      assertThat(await(embeddedPairCache.get("set-and-remove"))).isEmpty();

      assertThatThrownBy(() -> await(embeddedPairCache.remove("set-and-remove", null)))
            .isInstanceOf(NullPointerException.class);
      assertThatThrownBy(() -> await(embeddedPairCache.remove(null, "k1")))
            .isInstanceOf(NullPointerException.class);
   }

   public void testCompute() {
      assertThat(await(embeddedPairCache.compute("compute-test", "name", (k, v) -> {
         assertThat(k).isEqualTo("name");
         assertThat(v).isNull();
         return "Oihana";
      }))).isEqualTo("Oihana");

      assertThat(await(embeddedPairCache.compute("compute-test", "name", (k, v) -> {
         assertThat(k).isEqualTo("name");
         assertThat(v).isEqualTo("Oihana");
         return "Ramon";
      }))).isEqualTo("Ramon");

      assertThat(await(embeddedPairCache.get("compute-test", "name"))).isEqualTo("Ramon");
   }

   public void testSubSelect() {
      assertThat(await(embeddedPairCache.subSelect("unknown-subselect", 10))).isNull();

      Map<String, String> map = Map.of("name", "Oihana", "age", "1", "birthday", "2023-05-26");
      assertThat(await(embeddedPairCache.set("subselect-test", map.entrySet().toArray(new Map.Entry[0]))))
            .isEqualTo(3);

      assertThat(await(embeddedPairCache.subSelect("subselect-test", 2)))
            .hasSize(2)
            .satisfies(m -> assertThat(map).containsAllEntriesOf(m));

      assertThat(await(embeddedPairCache.subSelect("subselect-test", 30)))
            .containsAllEntriesOf(map)
            .hasSize(map.size());

      assertThatThrownBy(() -> await(embeddedPairCache.subSelect(null, 10)))
            .isInstanceOf(NullPointerException.class);
      assertThatThrownBy(() -> await(embeddedPairCache.subSelect("subselect-test", -1)))
            .isInstanceOf(IllegalArgumentException.class);
   }

   public void testGetMultipleProperties() {
      // ENTRY not exist, so an empty map.
      assertThat(await(embeddedPairCache.get("unknown-multiple", "k1", "k2"))).isEmpty();

      Map<String, String> map = Map.of("name", "Oihana", "age", "1", "birthday", "2023-05-26");
      assertThat(await(embeddedPairCache.set("multiple-test", map.entrySet().toArray(new Map.Entry[0]))))
            .isEqualTo(3);

      assertThat(await(embeddedPairCache.get("multiple-test", "name", "age")))
            .containsEntry("name", "Oihana")
            .containsEntry("age", "1")
            .hasSize(2);

      // An existing entry but with an unknown property.
      assertThat(await(embeddedPairCache.get("multiple-test", "name", "something-not-there", "age")))
            .containsEntry("name", "Oihana")
            .containsEntry("age", "1")
            .containsEntry("something-not-there", null)
            .hasSize(3);

      assertThatThrownBy(() -> await(embeddedPairCache.get(null, "k1", "k2")))
            .isInstanceOf(NullPointerException.class);
      assertThatThrownBy(() -> await(embeddedPairCache.get(null, "k1", null)))
            .isInstanceOf(NullPointerException.class);
      assertThatThrownBy(() -> await(embeddedPairCache.get("multiple-test", (String[]) null)))
            .isInstanceOf(NullPointerException.class);
      assertThatThrownBy(() -> await(embeddedPairCache.get("multiple-test", new String[0])))
            .isInstanceOf(IllegalArgumentException.class);
   }
}
