package org.infinispan.rest.resources;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;
import static org.infinispan.configuration.cache.CacheMode.DIST_SYNC;
import static org.infinispan.configuration.cache.CacheMode.LOCAL;
import static org.infinispan.partitionhandling.PartitionHandling.DENY_READ_WRITES;
import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.infinispan.client.rest.RestCacheManagerClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.configuration.JsonWriter;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.health.HealthStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

@Test(groups = "functional", testName = "rest.CacheManagerResourceTest")
public class CacheManagerResourceTest extends AbstractRestResourceTest {

   private Configuration cache1Config;
   private Configuration cache2Config;
   private ObjectMapper mapper = new ObjectMapper();
   private JsonWriter jsonWriter = new JsonWriter();
   private Configuration templateConfig;
   private RestCacheManagerClient cacheManagerClient;

   @Override
   public Object[] factory() {
      return new Object[]{
            new CacheManagerResourceTest().withSecurity(true),
            new CacheManagerResourceTest().withSecurity(false)
      };
   }

   @Override
   protected void createCacheManagers() throws Exception {
      super.createCacheManagers();
      cacheManagerClient = client.cacheManager("default");
   }

   @Override
   protected void defineCaches(EmbeddedCacheManager cm) {
      cache1Config = getCache1Config();
      cache2Config = getCache2Config();
      ConfigurationBuilder templateConfigBuilder = new ConfigurationBuilder();
      templateConfigBuilder.template(true).clustering().cacheMode(LOCAL).encoding().key().mediaType(TEXT_PLAIN_TYPE);
      templateConfig = templateConfigBuilder.build();
      cm.defineConfiguration("cache1", cache1Config);
      cm.defineConfiguration("cache2", cache2Config);
      cm.defineConfiguration("template", templateConfig);
   }

   private Configuration getCache1Config() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.statistics().enable().clustering().cacheMode(DIST_SYNC).partitionHandling().whenSplit(DENY_READ_WRITES);
      return builder.build();
   }

   private Configuration getCache2Config() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.statistics().enable().clustering().cacheMode(LOCAL).encoding().key().mediaType(TEXT_PLAIN_TYPE);
      return builder.build();
   }

   @Test
   public void testHealth() throws Exception {
      RestResponse response = join(cacheManagerClient.health());
      ResponseAssertion.assertThat(response).isOk();

      JsonNode jsonNode = mapper.readTree(response.getBody());
      JsonNode clusterHealth = jsonNode.get("cluster_health");
      // One of the caches is in FAILED state
      assertEquals(clusterHealth.get("health_status").asText(), HealthStatus.FAILED.toString());
      assertEquals(clusterHealth.get("number_of_nodes").asInt(), 2);
      assertEquals(clusterHealth.get("node_names").size(), 2);

      ArrayNode cacheHealth = (ArrayNode) jsonNode.get("cache_health");
      List<String> cacheNames = extractCacheNames(cacheHealth);
      assertTrue(cacheNames.contains("cache1"));
      assertTrue(cacheNames.contains("cache2"));

      response = join(cacheManagerClient.health(true));
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasNoContent();
   }

   @Test
   public void testCacheConfigs() throws Exception {
      String accept = "text/plain; q=0.9, application/json; q=0.6";

      RestResponse response = join(cacheManagerClient.cacheConfigurations(accept));

      ResponseAssertion.assertThat(response).isOk();

      String json = response.getBody();
      ArrayNode jsonNode = (ArrayNode) mapper.readTree(json);
      Map<String, String> cachesAndConfig = cacheAndConfig(jsonNode);

      assertEquals(cachesAndConfig.get("template"), jsonWriter.toJSON(templateConfig));
      assertEquals(cachesAndConfig.get("cache2"), jsonWriter.toJSON(cache2Config));
      assertEquals(cachesAndConfig.get("cache2"), jsonWriter.toJSON(cache2Config));
   }

   @Test
   public void testCacheConfigsTemplates() throws Exception {
      String accept = "text/plain; q=0.9, application/json; q=0.6";

      RestResponse response = join(cacheManagerClient.templates(accept));

      ResponseAssertion.assertThat(response).isOk();

      String json = response.getBody();
      ArrayNode jsonNode = (ArrayNode) mapper.readTree(json);
      Map<String, String> cachesAndConfig = cacheAndConfig(jsonNode);

      assertEquals(cachesAndConfig.get("template"), jsonWriter.toJSON(templateConfig));
      assertFalse(cachesAndConfig.containsKey("cache1"));
      assertFalse(cachesAndConfig.containsKey("cache2"));
   }

   @Test
   public void testCaches() throws Exception {
      RestResponse response = join(cacheManagerClient.caches());
      ResponseAssertion.assertThat(response).isOk();

      String json = response.getBody();
      JsonNode jsonNode = mapper.readTree(json);
      List<String> names = asText(jsonNode.findValues("name"));
      Set<String> expectedNames = Util.asSet("defaultcache", "cache1", "cache2", "invalid");

      assertEquals(expectedNames, new HashSet<>(names));

      List<String> status = asText(jsonNode.findValues("status"));
      assertTrue(status.contains("RUNNING"));

      List<String> types = asText(jsonNode.findValues("type"));
      assertTrue(types.contains("local-cache"));
      assertTrue(types.contains("distributed-cache"));

      List<String> simpleCaches = asText(jsonNode.findValues("simple_cache"));
      assertTrue(simpleCaches.contains("false"));

      List<String> transactional = asText(jsonNode.findValues("transactional"));
      assertTrue(transactional.contains("false"));

      List<String> persistent = asText(jsonNode.findValues("persistent"));
      assertTrue(persistent.contains("false"));

      List<String> bounded = asText(jsonNode.findValues("bounded"));
      assertTrue(bounded.contains("false"));

      List<String> secured = asText(jsonNode.findValues("secured"));
      assertTrue(secured.contains("false"));

      List<String> indexed = asText(jsonNode.findValues("indexed"));
      assertTrue(indexed.contains("false"));

      List<String> hasRemoteBackup = asText(jsonNode.findValues("has_remote_backup"));
      assertTrue(hasRemoteBackup.contains("false"));

      List<String> health = asText(jsonNode.findValues("health"));

      assertTrue(health.contains("HEALTHY"));
   }

   @Test
   public void testCachesWithIgnoreCache() throws JsonProcessingException {
      if (security) {
         Security.doAs(TestingUtil.makeSubject(AuthorizationPermission.ADMIN.name()), (PrivilegedAction<CompletableFuture<Void>>) () -> ignoreManager.ignoreCache("cache1"));
      } else {
         ignoreManager.ignoreCache("cache1");
      }

      RestResponse response = join(cacheManagerClient.caches());
      ResponseAssertion.assertThat(response).isOk();

      String json = response.getBody();
      JsonNode jsonNode = mapper.readTree(json);
      List<String> names = asText(jsonNode.findValues("name"));
      Set<String> expectedNames = Util.asSet("defaultcache", "cache1", "cache2", "invalid");

      assertEquals(expectedNames, new HashSet<>(names));

      List<String> status = asText(jsonNode.findValues("status"));
      assertTrue(status.contains("RUNNING"));
      assertTrue(status.contains("IGNORED"));
   }

   private List<String> asText(List<JsonNode> values) {
      return values.stream().map(JsonNode::asText).collect(Collectors.toList());
   }

   @Test
   public void testGetGlobalConfig() {
      RestResponse response = join(cacheManagerClient.globalConfiguration());

      ResponseAssertion.assertThat(response).isOk();

      String json = response.getBody();
      EmbeddedCacheManager embeddedCacheManager = cacheManagers.get(0);
      GlobalConfiguration globalConfiguration = embeddedCacheManager.withSubject(ADMIN_USER).getCacheManagerConfiguration();
      String globalConfigJSON = jsonWriter.toJSON(globalConfiguration);
      assertEquals(globalConfigJSON, json);
   }

   @Test
   public void testGetGlobalConfigXML() {
      RestResponse response = join(cacheManagerClient.globalConfiguration(APPLICATION_XML_TYPE));

      ResponseAssertion.assertThat(response).isOk();

      String xml = response.getBody();
      ParserRegistry parserRegistry = new ParserRegistry();
      ConfigurationBuilderHolder builderHolder = parserRegistry.parse(xml);

      GlobalConfigurationBuilder globalConfigurationBuilder = builderHolder.getGlobalConfigurationBuilder();
      assertNotNull(globalConfigurationBuilder.build());
   }

   @Test
   public void testInfo() throws Exception {
      RestResponse response = join(cacheManagerClient.info());

      ResponseAssertion.assertThat(response).isOk();

      String json = response.getBody();
      JsonNode cmInfo = mapper.readTree(json);

      assertFalse(cmInfo.get("version").asText().isEmpty());
      assertEquals(2, cmInfo.get("cluster_members").size());
      assertEquals(2, cmInfo.get("cluster_members_physical_addresses").size());
      assertEquals("LON-1", cmInfo.get("local_site").asText());
   }

   @Test
   public void testStats() throws Exception {
      RestResponse response = join(cacheManagerClient.stats());

      ResponseAssertion.assertThat(response).isOk();

      String json = response.getBody();
      JsonNode cmStats = mapper.readTree(json);

      assertTrue(cmStats.get("statistics_enabled").asBoolean());
      assertEquals(0, cmStats.get("stores").asInt());
      assertEquals(0, cmStats.get("number_of_entries").asInt());

      cacheManagers.iterator().next().getCache("cache1").put("key", "value");
      cmStats = mapper.readTree(join(cacheManagerClient.stats()).getBody());
      assertEquals(1, cmStats.get("stores").asInt());
      assertEquals(1, cmStats.get("number_of_entries").asInt());
   }

   private Map<String, String> cacheAndConfig(ArrayNode list) {
      Map<String, String> result = new HashMap<>();
      list.elements().forEachRemaining(node -> result.put(node.get("name").asText(), node.get("configuration").toString()));
      return result;
   }

   private List<String> extractCacheNames(ArrayNode cacheStatuses) {
      List<String> names = new ArrayList<>();
      cacheStatuses.elements().forEachRemaining(n -> names.add(n.get("cache_name").asText()));
      return names;
   }
}
