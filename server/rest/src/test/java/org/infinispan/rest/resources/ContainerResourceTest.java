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
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
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

   private Configuration cache2Config;
   private Configuration templateConfig;
   private RestCacheManagerClient cacheManagerClient;
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
            .persistentLocation(Paths.get(PERSISTENT_LOCATION, Integer.toString(id)).toString());
      return config;
   }

   @Override
   protected void createCacheManagers() throws Exception {
      Util.recursiveFileRemove(PERSISTENT_LOCATION);
      super.createCacheManagers();
      cacheManagerClient = client.cacheManager("default");
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
      assertTrue(cacheNames.contains("cache1"));
      assertTrue(cacheNames.contains("cache2"));

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

      assertEquals(cachesAndConfig.get("template"), cacheConfigToJson("template", templateConfig));
      assertEquals(cachesAndConfig.get("cache2"), cacheConfigToJson("cache2", cache2Config));
   }

   @Test
   public void testCacheConfigsTemplates() {
      String accept = "text/plain; q=0.9, application/json; q=0.6";

      RestResponse response = join(cacheManagerClient.templates(accept));

      ResponseAssertion.assertThat(response).isOk();

      String json = response.getBody();
      Json jsonNode = Json.read(json);
      Map<String, String> cachesAndConfig = cacheAndConfig(jsonNode);

      assertEquals(cachesAndConfig.get("template"), cacheConfigToJson("template", templateConfig));
      assertFalse(cachesAndConfig.containsKey("cache1"));
      assertFalse(cachesAndConfig.containsKey("cache2"));
   }

   @Test
   public void testCaches() {
      RestResponse response = join(cacheManagerClient.caches());
      ResponseAssertion.assertThat(response).isOk();

      String json = response.getBody();
      Json jsonNode = Json.read(json);
      List<String> names = find(jsonNode, "name");
      Set<String> expectedNames = Util.asSet("defaultcache", "cache1", "cache2", "invalid");

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
      assertTrue(bounded.contains("false"));

      List<String> secured = find(jsonNode, "secured");
      assertTrue(secured.contains("false"));

      List<String> indexed = find(jsonNode, "indexed");
      assertTrue(indexed.contains("false"));

      List<String> hasRemoteBackup = find(jsonNode, "has_remote_backup");
      assertTrue(hasRemoteBackup.contains("false"));

      List<String> health = find(jsonNode, "health");

      assertTrue(health.contains("HEALTHY"));
   }

   @Test
   public void testCachesWithIgnoreCache() {
      if (security) {
         Security.doAs(TestingUtil.makeSubject(AuthorizationPermission.ADMIN.name()), (PrivilegedAction<CompletableFuture<Void>>) () -> serverStateManager.ignoreCache("cache1"));
      } else {
         serverStateManager.ignoreCache("cache1");
      }

      RestResponse response = join(cacheManagerClient.caches());
      ResponseAssertion.assertThat(response).isOk();

      String json = response.getBody();
      Json jsonNode = Json.read(json);
      List<String> names = find(jsonNode, "name");
      Set<String> expectedNames = Util.asSet("defaultcache", "cache1", "cache2", "invalid");

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
      RestResponse response = join(cacheManagerClient.globalConfiguration());

      ResponseAssertion.assertThat(response).isOk();

      String json = response.getBody();
      EmbeddedCacheManager embeddedCacheManager = cacheManagers.get(0);
      GlobalConfiguration globalConfiguration = embeddedCacheManager.withSubject(ADMIN_USER).getCacheManagerConfiguration();
      StringBuilderWriter sw = new StringBuilderWriter();
      try (ConfigurationWriter w = ConfigurationWriter.to(sw).withType(APPLICATION_JSON).build()) {
         new ParserRegistry().serialize(w, globalConfiguration, Collections.emptyMap());
      }
      assertEquals(sw.toString(), json);
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
   public void testInfo() {
      RestResponse response = join(cacheManagerClient.info());

      ResponseAssertion.assertThat(response).isOk();

      String json = response.getBody();
      Json cmInfo = Json.read(json);

      assertFalse(cmInfo.at("version").asString().isEmpty());
      assertEquals(2, cmInfo.at("cluster_members").asList().size());
      assertEquals(2, cmInfo.at("cluster_members_physical_addresses").asList().size());
      assertEquals("LON-1", cmInfo.at("local_site").asString());
      assertTrue(cmInfo.at("site_coordinator").asBoolean());
      assertEquals(1, cmInfo.at("site_coordinators_address").asList().size());
      assertEquals(1, cmInfo.at("sites_view").asList().size());
      assertEquals("LON-1", cmInfo.at("sites_view").asList().get(0));
   }

   @Test
   public void testStats() {
      RestResponse response = join(cacheManagerClient.stats());

      ResponseAssertion.assertThat(response).isOk();

      String json = response.getBody();
      Json cmStats = Json.read(json);

      assertTrue(cmStats.at("statistics_enabled").asBoolean());
      assertEquals(0, cmStats.at("stores").asInteger());
      assertEquals(0, cmStats.at("number_of_entries").asInteger());

      // Advance 1 second for the cached stats to expire
      timeService.advance(1000);

      cacheManagers.iterator().next().getCache("cache1").put("key", "value");
      cmStats = Json.read(join(cacheManagerClient.stats()).getBody());
      assertEquals(1, cmStats.at("stores").asInteger());
      assertEquals(1, cmStats.at("number_of_entries").asInteger());
   }

   @Test
   public void testConfigListener() throws InterruptedException, IOException {
      SSEListener sseListener = new SSEListener();
      Closeable listen = client.raw().listen("/rest/v2/container/config?action=listen", Collections.emptyMap(), sseListener);
      AssertJUnit.assertTrue(sseListener.openLatch.await(10, TimeUnit.SECONDS));
      createCache("{\"local-cache\":{\"encoding\":{\"media-type\":\"text/plain\"}}}", "listen1");
      assertEquals("create-cache", sseListener.events.poll(10, TimeUnit.SECONDS));
      AssertJUnit.assertTrue(sseListener.data.removeFirst().contains("text/plain"));
      createCache("{\"local-cache\":{\"encoding\":{\"media-type\":\"application/octet-stream\"}}}", "listen2");
      assertEquals("create-cache", sseListener.events.poll(10, TimeUnit.SECONDS));
      AssertJUnit.assertTrue(sseListener.data.removeFirst().contains("application/octet-stream"));
      assertThat(client.cache("listen1").delete()).isOk();
      assertEquals("remove-cache", sseListener.events.poll(10, TimeUnit.SECONDS));
      assertEquals("listen1", sseListener.data.removeFirst());
      listen.close();
   }

   private void createCache(String json, String name) {
      RestEntity jsonEntity = RestEntity.create(APPLICATION_JSON, json);
      CompletionStage<RestResponse> response = client.cache(name).createWithConfiguration(jsonEntity);
      assertThat(response).isOk();
   }

   @Test
   public void testRebalancingActions() {
      assertRebalancingStatus(true);

      RestResponse response = join(cacheManagerClient.disableRebalancing());
      ResponseAssertion.assertThat(response).isOk();
      assertRebalancingStatus(false);

      response = join(cacheManagerClient.enableRebalancing());
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
