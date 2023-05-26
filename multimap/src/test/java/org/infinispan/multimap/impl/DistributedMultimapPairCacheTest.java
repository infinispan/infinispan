package org.infinispan.multimap.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.functional.FunctionalTestUtils.await;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.multimap.api.embedded.EmbeddedMultimapCacheManagerFactory;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DistributedMultimapPairCacheTest")
public class DistributedMultimapPairCacheTest extends BaseDistFunctionalTest<String, Map<String, String>> {

   protected Map<Address, EmbeddedMultimapPairCache<String, String, String>> pairCacheCluster = new HashMap<>();

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

   private EmbeddedMultimapPairCache<String, String, String> getMultimapMember() {
      return pairCacheCluster.values()
            .stream().findFirst().orElseThrow(() -> new IllegalStateException("No multimap cluster found!"));
   }

   public void testSetGetOperations() {
      EmbeddedMultimapPairCache<String, String, String> multimap = getMultimapMember();
      await(multimap.set("person1", Map.entry("name", "Oihana")));
      await(multimap.set("person1", Map.entry("age", "1"), Map.entry("birthday", "2023-05-26")));

      Map<String, String> person1 = await(multimap.get("person1"));
      assertThat(person1)
            .containsEntry("name", "Oihana")
            .containsEntry("age", "1")
            .containsEntry("birthday", "2023-05-26")
            .hasSize(3);
   }
}
