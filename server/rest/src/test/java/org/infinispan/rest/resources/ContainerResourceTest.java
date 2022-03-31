package org.infinispan.rest.resources;

import static org.infinispan.client.rest.configuration.Protocol.HTTP_11;
import static org.infinispan.client.rest.configuration.Protocol.HTTP_20;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;
import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.infinispan.configuration.cache.CacheMode.DIST_SYNC;
import static org.infinispan.configuration.cache.CacheMode.LOCAL;
import static org.infinispan.partitionhandling.PartitionHandling.DENY_READ_WRITES;
import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;
import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.infinispan.client.rest.RestCacheManagerClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.io.StringBuilderWriter;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.health.HealthStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.ControlledTimeService;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.ContainerResourceTest")
public class ContainerResourceTest extends AbstractRestResourceTest {
   private static final String PERSISTENT_LOCATION = tmpDirectory(ContainerResourceTest.class.getName());
   private static final String CACHE_1 = "cache1";
   private static final String CACHE_2 = "cache2";
   private static final String DEFAULT_CACHE = "defaultcache";
   private static final String INVALID_CACHE = "invalid";
   private static final String CACHE_MANAGER_NAME = "default";
   public static final String TEMPLATE_CONFIG = "template";

   private Configuration cache2Config;
   private Configuration templateConfig;
   private RestCacheManagerClient cacheManagerClient, adminCacheManagerClient;
   private ControlledTimeService timeService;

   @Override
   public Object[] factory() {
      return new Object[]{
            new ContainerResourceTest().withSecurity(true).protocol(HTTP_11),
            new ContainerResourceTest().withSecurity(false).protocol(HTTP_11),
            new ContainerResourceTest().withSecurity(true).protocol(HTTP_20),
            new ContainerResourceTest().withSecurity(false).protocol(HTTP_20)
      };
   }

   @Override
   protected GlobalConfigurationBuilder getGlobalConfigForNode(int id) {
      GlobalConfigurationBuilder config = super.getGlobalConfigForNode(id);
      config.globalState().enable()
            .configurationStorage(ConfigurationStorage.OVERLAY)
            .persistentLocation(Paths.get(PERSISTENT_LOCATION, Integer.toString(id)).toString())
            .metrics().accurateSize(true);
      return config;
   }

   @Override
   protected void createCacheManagers() throws Exception {
      Util.recursiveFileRemove(PERSISTENT_LOCATION);
      super.createCacheManagers();
      cacheManagerClient = client.cacheManager(CACHE_MANAGER_NAME);
      adminCacheManagerClient = adminClient.cacheManager(CACHE_MANAGER_NAME);
      timeService = new ControlledTimeService();
      cacheManagers.forEach(cm -> TestingUtil.replaceComponent(cm, TimeService.class, timeService, true));
   }

   @Override
   protected void defineCaches(EmbeddedCacheManager cm) {
      Configuration cache1Config = getCache1Config();
      cache2Config = getCache2Config();
      ConfigurationBuilder templateConfigBuilder = new ConfigurationBuilder();
      templateConfigBuilder.template(true).clustering().cacheMode(LOCAL).encoding().key().mediaType(TEXT_PLAIN_TYPE);
      templateConfig = templateConfigBuilder.build();
      cm.defineConfiguration(CACHE_1, cache1Config);
      cm.defineConfiguration(CACHE_2, cache2Config);
      cm.defineConfiguration(TEMPLATE_CONFIG, templateConfig);
   }

   private Configuration getCache1Config() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.statistics().enable().clustering().cacheMode(DIST_SYNC).partitionHandling().whenSplit(DENY_READ_WRITES);
      return builder.build();
   }

   private Configuration getCache2Config() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.statistics().enable().clustering().cacheMode(LOCAL).encoding().key().mediaType(TEXT_PLAIN_TYPE);
      builder.memory().maxCount(1000).storage(StorageType.HEAP).whenFull(EvictionStrategy.REMOVE);
      return builder.build();
   }

   @Test
   public void testHealth() {
      RestResponse response = join(cacheManagerClient.health());
      ResponseAssertion.assertThat(response).isOk();

      Json jsonNode = Json.read(response.getBody());
      Json clusterHealth = jsonNode.at("cluster_health");
      // One of the caches is in FAILED state
      assertEquals(clusterHealth.at("health_status").asString(), HealthStatus.FAILED.toString());
      assertEquals(clusterHealth.at("number_of_nodes").asInteger(), 2);
      assertEquals(clusterHealth.at("node_names").asJsonList().size(), 2);

      Json cacheHealth = jsonNode.at("cache_health");
      List<String> cacheNames = extractCacheNames(cacheHealth);
      assertTrue(cacheNames.contains(CACHE_1));
      assertTrue(cacheNames.contains(CACHE_2));

      response = join(cacheManagerClient.health(true));
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasNoContent();
   }

   @Test
   public void testCacheConfigs() {
      String accept = "text/plain; q=0.9, application/json; q=0.6";

      RestResponse response = join(cacheManagerClient.cacheConfigurations(accept));

      ResponseAssertion.assertThat(response).isOk();

      String json = response.getBody();
      Json jsonNode = Json.read(json);
      Map<String, String> cachesAndConfig = cacheAndConfig(jsonNode);

      assertEquals(cachesAndConfig.get(TEMPLATE_CONFIG), cacheConfigToJson(TEMPLATE_CONFIG, templateConfig));
      assertEquals(cachesAndConfig.get(CACHE_2), cacheConfigToJson(CACHE_2, cache2Config));
   }

   @Test
   public void testCacheConfigsTemplates() {
      String accept = "text/plain; q=0.9, application/json; q=0.6";

      RestResponse response = join(cacheManagerClient.templates(accept));

      ResponseAssertion.assertThat(response).isOk();

      String json = response.getBody();
      Json jsonNode = Json.read(json);
      Map<String, String> cachesAndConfig = cacheAndConfig(jsonNode);

      assertEquals(cachesAndConfig.get(TEMPLATE_CONFIG), cacheConfigToJson(TEMPLATE_CONFIG, templateConfig));
      assertFalse(cachesAndConfig.containsKey(CACHE_1));
      assertFalse(cachesAndConfig.containsKey(CACHE_2));
   }

   @Test
   public void testCaches() {
      RestResponse response = join(cacheManagerClient.caches());
      ResponseAssertion.assertThat(response).isOk();

      String json = response.getBody();
      Json jsonNode = Json.read(json);
      List<String> names = find(jsonNode, "name");
      Set<String> expectedNames = Util.asSet(DEFAULT_CACHE, CACHE_1, CACHE_2, INVALID_CACHE);

      assertEquals(expectedNames, new HashSet<>(names));

      List<String> status = find(jsonNode, "status");
      assertTrue(status.contains("RUNNING"));

      List<String> types = find(jsonNode, "type");
      assertTrue(types.contains("local-cache"));
      assertTrue(types.contains("distributed-cache"));

      List<String> simpleCaches = find(jsonNode, "simple_cache");
      assertTrue(simpleCaches.contains("false"));

      List<String> transactional = find(jsonNode, "transactional");
      assertTrue(transactional.contains("false"));

      List<String> persistent = find(jsonNode, "persistent");
      assertTrue(persistent.contains("false"));

      List<String> bounded = find(jsonNode, "bounded");
      List<String> notBoundedCaches = bounded.stream().filter(b -> "false".equals(b)).collect(Collectors.toList());
      List<String> boundedCaches = bounded.stream().filter(b -> "true".equals(b)).collect(Collectors.toList());
      assertEquals(1, boundedCaches.size());
      assertEquals(3, notBoundedCaches.size());

      List<String> secured = find(jsonNode, "secured");
      assertTrue(secured.contains("false"));

      List<String> indexed = find(jsonNode, "indexed");
      assertTrue(indexed.contains("false"));

      List<String> hasRemoteBackup = find(jsonNode, "has_remote_backup");
      assertTrue(hasRemoteBackup.contains("false"));

      List<String> health = find(jsonNode, "health");

      assertTrue(health.contains("HEALTHY"));

      List<String> isRebalancingEnabled = find(jsonNode, "rebalancing_enabled");
      assertTrue(isRebalancingEnabled.contains("true"));
   }

   @Test
   public void testCachesWithIgnoreCache() {
      if (security) {
         Security.doAs(TestingUtil.makeSubject(AuthorizationPermission.ADMIN.name()), (PrivilegedAction<CompletableFuture<Void>>) () -> serverStateManager.ignoreCache(
               CACHE_1));
      } else {
         serverStateManager.ignoreCache(CACHE_1);
      }

      RestResponse response = join(cacheManagerClient.caches());
      ResponseAssertion.assertThat(response).isOk();

      String json = response.getBody();
      Json jsonNode = Json.read(json);
      List<String> names = find(jsonNode, "name");
      Set<String> expectedNames = Util.asSet(DEFAULT_CACHE, CACHE_1, CACHE_2, INVALID_CACHE);

      assertEquals(expectedNames, new HashSet<>(names));

      List<String> status = find(jsonNode, "status");
      assertTrue(status.contains("RUNNING"));
      assertTrue(status.contains("IGNORED"));
   }

   private List<String> find(Json array, String name) {
      return array.asJsonList().stream().map(j -> j.at(name).getValue().toString()).collect(Collectors.toList());
   }

   @Test
   public void testGetGlobalConfig() {
      RestResponse response = join(adminCacheManagerClient.globalConfiguration());

      ResponseAssertion.assertThat(response).isOk();

      String json = response.getBody();
      EmbeddedCacheManager embeddedCacheManager = cacheManagers.get(0);
      GlobalConfiguration globalConfiguration = embeddedCacheManager.withSubject(ADMIN).getCacheManagerConfiguration();
      StringBuilderWriter sw = new StringBuilderWriter();
      try (ConfigurationWriter w = ConfigurationWriter.to(sw).withType(APPLICATION_JSON).build()) {
         new ParserRegistry().serialize(w, globalConfiguration, Collections.emptyMap());
      }
      assertEquals(sw.toString(), json);
   }

   @Test
   public void testGetGlobalConfigXML() {
      RestResponse response = join(adminCacheManagerClient.globalConfiguration(APPLICATION_XML_TYPE));

      ResponseAssertion.assertThat(response).isOk();

      String xml = response.getBody();
      ParserRegistry parserRegistry = new ParserRegistry();
      ConfigurationBuilderHolder builderHolder = parserRegistry.parse(xml);

      GlobalConfigurationBuilder globalConfigurationBuilder = builderHolder.getGlobalConfigurationBuilder();
      assertNotNull(globalConfigurationBuilder.build());
   }

   @Test
   public void testInfo() {
      RestResponse response = join(cacheManagerClient.info());

      ResponseAssertion.assertThat(response).isOk();

      String json = response.getBody();
      Json cmInfo = Json.read(json);

      assertFalse(cmInfo.at("version").asString().isEmpty());
      assertEquals(2, cmInfo.at("cluster_members").asList().size());
      assertEquals(2, cmInfo.at("cluster_members_physical_addresses").asList().size());
      assertEquals("LON-1", cmInfo.at("local_site").asString());
      assertTrue(cmInfo.at("relay_node").asBoolean());
      assertEquals(1, cmInfo.at("relay_nodes_address").asList().size());
      assertEquals(1, cmInfo.at("sites_view").asList().size());
      assertEquals("LON-1", cmInfo.at("sites_view").asList().get(0));
      assertTrue(cmInfo.at("rebalancing_enabled").asBoolean());
   }

   @Test
   public void testStats() {
      RestResponse response = join(adminCacheManagerClient.stats());

      ResponseAssertion.assertThat(response).isOk();

      String json = response.getBody();
      Json cmStats = Json.read(json);

      assertTrue(cmStats.at("statistics_enabled").asBoolean());
      assertEquals(0, cmStats.at("stores").asInteger());
      assertEquals(0, cmStats.at("number_of_entries").asInteger());

      // Advance 1 second for the cached stats to expire
      timeService.advance(1000);

      cacheManagers.iterator().next().getCache(CACHE_1).put("key", "value");
      cmStats = Json.read(join(adminCacheManagerClient.stats()).getBody());
      assertEquals(1, cmStats.at("stores").asInteger());
      assertEquals(1, cmStats.at("number_of_entries").asInteger());
   }

   @Test
   public void testConfigListener() throws InterruptedException, IOException {
      SSEListener sseListener = new SSEListener();
      try (Closeable ignored = adminClient.raw().listen("/rest/v2/container/config?action=listen&includeCurrentState=true", Collections.emptyMap(), sseListener)) {
         AssertJUnit.assertTrue(sseListener.openLatch.await(10, TimeUnit.SECONDS));

         // Assert that all of the existing caches and templates have a corresponding event
         sseListener.expectEvent("create-template", TEMPLATE_CONFIG);
         sseListener.expectEvent("create-cache", "___protobuf_metadata");
         sseListener.expectEvent("create-cache", CACHE_2);
         sseListener.expectEvent("create-cache", INVALID_CACHE);
         sseListener.expectEvent("create-cache", CACHE_1);
         sseListener.expectEvent("create-cache", DEFAULT_CACHE);
         sseListener.expectEvent("create-cache", "___script_cache");

         // Assert that new cache creations create an event
         createCache("{\"local-cache\":{\"encoding\":{\"media-type\":\"text/plain\"}}}", "listen1");
         sseListener.expectEvent("create-cache", "text/plain");
         createCache("{\"local-cache\":{\"encoding\":{\"media-type\":\"application/octet-stream\"}}}", "listen2");
         sseListener.expectEvent("create-cache", "application/octet-stream");

         // Assert that deletions create an event
         assertThat(client.cache("listen1").delete()).isOk();
         sseListener.expectEvent("remove-cache", "listen1");
      }
   }

   private void createCache(String json, String name) {
      RestEntity jsonEntity = RestEntity.create(APPLICATION_JSON, json);
      CompletionStage<RestResponse> response = client.cache(name).createWithConfiguration(jsonEntity);
      assertThat(response).isOk();
   }

   @Test
   public void testRebalancingActions() {
      assertRebalancingStatus(true);

      RestResponse response = join(adminCacheManagerClient.disableRebalancing());
      ResponseAssertion.assertThat(response).isOk();
      assertRebalancingStatus(false);

      response = join(adminCacheManagerClient.enableRebalancing());
      ResponseAssertion.assertThat(response).isOk();
      assertRebalancingStatus(true);
   }

   private void assertRebalancingStatus(boolean enabled) {
      for (EmbeddedCacheManager cm : cacheManagers) {
         eventuallyEquals(enabled, () -> {
            try {
               return TestingUtil.extractGlobalComponent(cm, LocalTopologyManager.class).isRebalancingEnabled();
            } catch (Exception e) {
               fail("Unexpected exception", e);
               return !enabled;
            }
         });
      }
   }

   private Map<String, String> cacheAndConfig(Json list) {
      Map<String, String> result = new HashMap<>();
      list.asJsonList().forEach(node -> result.put(node.at("name").asString(), node.at("configuration").toString()));
      return result;
   }

   private List<String> extractCacheNames(Json cacheStatuses) {
      return cacheStatuses.asJsonList().stream().map(j -> j.at("cache_name").asString()).collect(Collectors.toList());
   }
}
