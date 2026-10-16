package org.infinispan.server.functional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.rest.RestResponseInfo.NO_CONTENT;
import static org.infinispan.client.rest.RestResponseInfo.OK;
import static org.infinispan.server.test.core.Common.assertStatus;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.jupiter.InfinispanServerExtension;
import org.infinispan.server.test.jupiter.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class HeterogeneousClientConditionalOperationIT {

   @RegisterExtension
   public static final InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerTest.xml")
               .numServers(1)
               .runMode(ServerRunMode.CONTAINER)
               .build();

   private static final String CACHE_CONFIGURATION = """
         <local-cache name="%s">
            <encoding>
               <key media-type="text/plain"/>
               <value media-type="text/plain"/>
            </encoding>
         </local-cache>
         """;

   @Test
   public void testReplaceWithVersionMatchingSucceeds() {
      String key = "my-key";

      RestCacheClient restClient = createRestClient();
      assertStatus(NO_CONTENT, restClient.put(key, "initial"));

      RemoteCache<String, String> hotRodClient = createHotRodClient();
      MetadataValue<String> mv = hotRodClient.getWithMetadata(key);
      assertThat(mv).isNotNull();
      assertThat(mv.getVersion()).isNotZero();

      assertThat(hotRodClient.replaceWithVersion(key, "v2", mv.getVersion())).isTrue();
      assertThat(hotRodClient.get(key)).isEqualTo("v2");
   }

   @Test
   public void testReplaceWithVersionMismatchFails() {
      String key = "my-key";

      RestCacheClient restClient = createRestClient();
      assertStatus(NO_CONTENT, restClient.put(key, "initial"));

      RemoteCache<String, String> hotRodClient = createHotRodClient();
      MetadataValue<String> mv = hotRodClient.getWithMetadata(key);
      assertThat(mv).isNotNull();

      // Stale version, the replace must fail and leave the entry unchanged.
      long staleVersion = mv.getVersion() - 1;
      assertThat(hotRodClient.replaceWithVersion(key, "v2", staleVersion)).isFalse();
      assertThat(hotRodClient.get(key)).isEqualTo("initial");
   }

   @Test
   public void testRemoveWithVersionMatchingSucceeds() {
      String key = "my-key";

      RestCacheClient restClient = createRestClient();
      assertStatus(NO_CONTENT, restClient.put(key, "initial"));

      RemoteCache<String, String> hotRodClient = createHotRodClient();
      MetadataValue<String> mv = hotRodClient.getWithMetadata(key);
      assertThat(mv).isNotNull();

      assertThat(hotRodClient.removeWithVersion(key, mv.getVersion())).isTrue();
      assertThat(hotRodClient.get(key)).isNull();
   }

   @Test
   public void testRemoveWithVersionMismatchFails() {
      String key = "my-key";

      RestCacheClient restClient = createRestClient();
      assertStatus(NO_CONTENT, restClient.put(key, "initial"));

      RemoteCache<String, String> hotRodClient = createHotRodClient();
      MetadataValue<String> mv = hotRodClient.getWithMetadata(key);
      assertThat(mv).isNotNull();

      long staleVersion = mv.getVersion() - 1;
      assertThat(hotRodClient.removeWithVersion(key, staleVersion)).isFalse();
      assertThat(hotRodClient.get(key)).isEqualTo("initial");
   }

   @Test
   public void testComputeIfPresentOnRestCreatedEntry() {
      String key = "my-key";

      RestCacheClient restClient = createRestClient();
      assertStatus(NO_CONTENT, restClient.put(key, "initial"));

      RemoteCache<String, String> hotRodClient = createHotRodClient();
      String result = hotRodClient.computeIfPresent(key, (k, v) -> v + "-computed");
      assertThat(result).isEqualTo("initial-computed");
      assertThat(hotRodClient.get(key)).isEqualTo("initial-computed");
   }

   @Test
   public void testComputeOnRestCreatedEntry() {
      String key = "my-key";

      RestCacheClient restClient = createRestClient();
      assertStatus(NO_CONTENT, restClient.put(key, "initial"));

      // compute must complete without spinning — entry exists, function is applied.
      RemoteCache<String, String> hotRodClient = createHotRodClient();
      String result = hotRodClient.compute(key, (k, v) -> v + "-computed");
      assertThat(result).isEqualTo("initial-computed");
      assertThat(hotRodClient.get(key)).isEqualTo("initial-computed");
   }

   @Test
   public void testRemoveIfValueMatchesOnRestCreatedEntry() {
      String key = "my-key";

      RestCacheClient restClient = createRestClient();
      assertStatus(NO_CONTENT, restClient.put(key, "initial"));

      RemoteCache<String, String> hotRodClient = createHotRodClient();

      // Wrong value must not remove.
      assertThat(hotRodClient.remove(key, "wrong")).isFalse();
      assertThat(hotRodClient.get(key)).isEqualTo("initial");

      // Correct value must remove.
      assertThat(hotRodClient.remove(key, "initial")).isTrue();
      assertThat(hotRodClient.get(key)).isNull();
   }

   @Test
   public void testReplaceOldNewOnRestCreatedEntry() {
      String key = "my-key";

      RestCacheClient restClient = createRestClient();
      assertStatus(NO_CONTENT, restClient.put(key, "initial"));

      RemoteCache<String, String> hotRodClient = createHotRodClient();
      assertThat(hotRodClient.replace(key, "initial", "replaced")).isTrue();
      assertThat(hotRodClient.get(key)).isEqualTo("replaced");
   }

   @Test
   public void testInterleavedRestAndHotRodReplace() {
      String key = "my-key";

      RestCacheClient restClient = createRestClient();
      RemoteCache<String, String> hotRodClient = createHotRodClient();

      // Round 1: REST put -> Hot Rod replace succeeds.
      assertStatus(NO_CONTENT, restClient.put(key, "v1"));
      MetadataValue<String> mv1 = hotRodClient.getWithMetadata(key);
      assertThat(mv1).isNotNull();
      assertThat(hotRodClient.replaceWithVersion(key, "v2", mv1.getVersion())).isTrue();

      // Round 2: REST put again -> stale Hot Rod version now fails, fresh version succeeds.
      assertStatus(NO_CONTENT, restClient.put(key, "v3"));
      // mv1.getVersion() is now stale, it must fail.
      assertThat(hotRodClient.replaceWithVersion(key, "v4", mv1.getVersion())).isFalse();
      // Fetch new version and retry, must succeed.
      MetadataValue<String> mv2 = hotRodClient.getWithMetadata(key);
      assertThat(mv2).isNotNull();
      assertThat(mv2.getVersion()).isNotEqualTo(mv1.getVersion());
      assertThat(hotRodClient.replaceWithVersion(key, "v4", mv2.getVersion())).isTrue();
      assertThat(hotRodClient.get(key)).isEqualTo("v4");
   }

   @Test
   public void testConditionalOperationWithHeterogeneousClients() {
      String key = "my-key";

      // Creates the entry using the REST API.
      RestCacheClient restClient = createRestClient();
      assertStatus(NO_CONTENT, restClient.put(key, "initial"));
      assertStatus(OK, restClient.get(key));

      // Perform the conditional operations using Hot Rod now.
      RemoteCache<String, String> hotRodClient = createHotRodClient();
      MetadataValue<String> mv = hotRodClient.getWithMetadata(key);
      assertThat(mv).isNotNull();
      assertThat(mv.getValue()).isEqualTo("initial");

      // No concurrent access, replace should succeed.
      assertThat(hotRodClient.replaceWithVersion(key, "v2", mv.getVersion()))
            .isTrue();
   }

   private RestCacheClient createRestClient() {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      String xml = CACHE_CONFIGURATION.formatted(SERVERS.getMethodName());
      RestClient client = SERVERS.rest()
            .withServerConfiguration(new StringConfiguration(xml))
            .withClientConfiguration(builder)
            .create();
      return client.cache(SERVERS.getMethodName());
   }

   private RemoteCache<String, String> createHotRodClient() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      String xml = CACHE_CONFIGURATION.formatted(SERVERS.getMethodName());
      return SERVERS.hotrod()
            .withServerConfiguration(new StringConfiguration(xml))
            .withClientConfiguration(builder)
            .create();
   }
}
