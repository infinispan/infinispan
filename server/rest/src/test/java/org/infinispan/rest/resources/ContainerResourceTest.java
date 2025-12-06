package org.infinispan.rest.resources;

import static org.infinispan.client.rest.configuration.Protocol.HTTP_11;
import static org.infinispan.client.rest.configuration.Protocol.HTTP_20;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;
import static org.infinispan.commons.internal.InternalCacheNames.PROTOBUF_METADATA_CACHE_NAME;
import static org.infinispan.commons.internal.InternalCacheNames.SCRIPT_CACHE_NAME;
import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.infinispan.configuration.cache.CacheMode.DIST_SYNC;
import static org.infinispan.configuration.cache.CacheMode.LOCAL;
import static org.infinispan.partitionhandling.PartitionHandling.DENY_READ_WRITES;
import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.io.StringBuilderWriter;
import org.infinispan.commons.time.ControlledTimeService;
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
import org.infinispan.security.Security;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.KeyValuePair;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.ContainerResourceTest")
public class ContainerResourceTest extends AbstractRestResourceTest {
   private static final String PERSISTENT_LOCATION = tmpDirectory(ContainerResourceTest.class.getName());
   private static final String CACHE_1 = "cache1";
   private static final String CACHE_2 = "cache2";
   private static final String CACHE_3= "cache3";
   private static final String DEFAULT_CACHE = "defaultcache";
   private static final String INVALID_CACHE = "invalid";
   public static final String TEMPLATE_CONFIG = "template";

   private Configuration cache2Config;
   private Configuration templateConfig;
   private ControlledTimeService timeService;

   @Override
   public Object[] factory() {
      return new Object[]{
            new ContainerResourceTest().withSecurity(true).protocol(HTTP_11).browser(false),
            new ContainerResourceTest().withSecurity(true).protocol(HTTP_11).browser(true),
            new ContainerResourceTest().withSecurity(false).protocol(HTTP_11).browser(false),
            new ContainerResourceTest().withSecurity(false).protocol(HTTP_11).browser(true),
            new ContainerResourceTest().withSecurity(true).protocol(HTTP_20).browser(false),
            new ContainerResourceTest().withSecurity(true).protocol(HTTP_20).browser(true),
            new ContainerResourceTest().withSecurity(false).protocol(HTTP_20).browser(false),
            new ContainerResourceTest().withSecurity(false).protocol(HTTP_20).browser(true),
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
      cm.defineConfiguration(CACHE_3, getCache3Config());
      cm.defineConfiguration(TEMPLATE_CONFIG, templateConfig);
   }

   private Configuration getCache1Config() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.statistics().enable().clustering().cacheMode(DIST_SYNC).partitionHandling().whenSplit(DENY_READ_WRITES).aliases("alias");
      return builder.build();
   }

   private Configuration getCache2Config() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.statistics().enable().clustering().cacheMode(LOCAL).encoding().key().mediaType(TEXT_PLAIN_TYPE);
      builder.memory().maxCount(1000).storage(StorageType.HEAP).whenFull(EvictionStrategy.REMOVE);
      return builder.build();
   }

   private Configuration getCache3Config() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      if (security) {
         builder.security().authorization().enable().roles("ADMIN", "USER");
      }
      return builder.build();
   }

   @Test
   public void testHealth() {
      RestResponse response = join(client.container().health());
      ResponseAssertion.assertThat(response).isOk();
      Json jsonNode = Json.read(response.body());
      Json clusterHealth = jsonNode.at("cluster_health");
      assertEquals(HealthStatus.HEALTHY.toString(), clusterHealth.at("health_status").asString());
      assertEquals(2, clusterHealth.at("number_of_nodes").asInteger());
      assertEquals(2, clusterHealth.at("node_names").asJsonList().size());

      Json cacheHealth = jsonNode.at("cache_health");
      List<String> cacheNames = extractCacheNames(cacheHealth);
      assertTrue(cacheNames.contains(CACHE_1));
      assertTrue(cacheNames.contains(CACHE_2));

      response = join(client.container().health(true));
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasNoContent();
   }

   @Test
   public void testUnhealthy() {
      // Create a cache that will report a failed health status
      Security.doAs(ADMIN, () -> {
         cacheManagers.forEach(cm -> cm.defineConfiguration(INVALID_CACHE,
               getDefaultCacheBuilder().encoding().mediaType(APPLICATION_OBJECT_TYPE)
                     .indexing().enabled(true).addIndexedEntities("invalid").build()));
         try {
            cacheManagers.get(0).getCache(INVALID_CACHE);
         } catch (CacheConfigurationException ignored) {
         }
      });
      RestResponse response = join(client.container().health());
      ResponseAssertion.assertThat(response).isOk();

      Json jsonNode = Json.read(response.body());
      Json clusterHealth = jsonNode.at("cluster_health");
      assertEquals(HealthStatus.FAILED.toString(), clusterHealth.at("health_status").asString());
   }

   @Test
   public void testCacheConfigs() {
      String accept = "text/plain; q=0.9, application/json; q=0.6";

      RestResponse response = join(client.container().cacheConfigurations(accept));

      ResponseAssertion.assertThat(response).isOk();

      String json = response.body();
      Json jsonNode = Json.read(json);
      Map<String, String> cachesAndConfig = cacheAndConfig(jsonNode);

      assertEquals(cachesAndConfig.get(TEMPLATE_CONFIG), cacheConfigToJson(TEMPLATE_CONFIG, templateConfig));
      assertEquals(cachesAndConfig.get(CACHE_2), cacheConfigToJson(CACHE_2, cache2Config));
   }

   @Test
   public void testCacheConfigsTemplates() {
      String accept = "text/plain; q=0.9, application/json; q=0.6";

      RestResponse response = join(client.container().templates(accept));

      ResponseAssertion.assertThat(response).isOk();

      String json = response.body();
      Json jsonNode = Json.read(json);
      Map<String, String> cachesAndConfig = cacheAndConfig(jsonNode);

      assertEquals(cachesAndConfig.get(TEMPLATE_CONFIG), cacheConfigToJson(TEMPLATE_CONFIG, templateConfig));
      assertFalse(cachesAndConfig.containsKey(CACHE_1));
      assertFalse(cachesAndConfig.containsKey(CACHE_2));
   }

   private List<String> find(Json array, String name) {
      return array.asJsonList().stream().map(j -> j.at(name).getValue().toString()).collect(Collectors.toList());
   }

   @Test
   public void testGetGlobalConfig() {
      RestResponse response = join(adminClient.container().globalConfiguration());

      ResponseAssertion.assertThat(response).isOk();

      String json = response.body();
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
      RestResponse response = join(adminClient.container().globalConfiguration(APPLICATION_XML_TYPE));

      ResponseAssertion.assertThat(response).isOk();

      String xml = response.body();
      ParserRegistry parserRegistry = new ParserRegistry();
      ConfigurationBuilderHolder builderHolder = parserRegistry.parse(xml);

      GlobalConfigurationBuilder globalConfigurationBuilder = builderHolder.getGlobalConfigurationBuilder();
      assertNotNull(globalConfigurationBuilder.build());
   }

   @Test
   public void testInfo() {
      RestResponse response = join(client.container().info());

      ResponseAssertion.assertThat(response).isOk();

      String json = response.body();
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
      assertFalse(cmInfo.at("tracing_enabled").asBoolean());
   }

   @Test
   public void testStats() {
      RestResponse response = join(adminClient.container().stats());

      ResponseAssertion.assertThat(response).isOk();

      String json = response.body();
      Json cmStats = Json.read(json);

      assertTrue(cmStats.at("statistics_enabled").asBoolean());
   }

   @Test
   public void testConfigListener() throws InterruptedException, IOException {
      SSEListener sseListener = new SSEListener();
      try (Closeable ignored = adminClient.raw().listen("/rest/v2/container/config?action=listen&includeCurrentState=true", Collections.emptyMap(), sseListener)) {
         assertTrue(sseListener.await(10, TimeUnit.SECONDS));

         // Assert that all the existing caches and templates have a corresponding event
         List<String> elements = List.of(TEMPLATE_CONFIG, CACHE_1, CACHE_2, CACHE_3, DEFAULT_CACHE,
               PROTOBUF_METADATA_CACHE_NAME, SCRIPT_CACHE_NAME);
         List<KeyValuePair<String, String>> events = sseListener.poll(elements.size());
         Assertions.assertThat(events).extracting(KeyValuePair::getKey)
               .containsAnyOf("create-cache", "create-template");
         elements.forEach(element -> {
            Iterator<KeyValuePair<String, String>> iterator = events.iterator();
            boolean found = false;
            while (iterator.hasNext() && !found) {
               KeyValuePair<String, String> next = iterator.next();
               if (next.getValue().contains(element)) {
                  found = true;
               }
            }
            assertTrue(found);
         });

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

      try (RestResponse response = join(adminClient.container().disableRebalancing())) {
         ResponseAssertion.assertThat(response).isOk();
         assertRebalancingStatus(false);
      }

      try (RestResponse response = join(adminClient.container().enableRebalancing())) {
         ResponseAssertion.assertThat(response).isOk();
         assertRebalancingStatus(true);
      }
   }

   private void assertRebalancingStatus(boolean enabled) {
      for (EmbeddedCacheManager cm : cacheManagers) {
         eventuallyEquals(enabled, () -> {
            try {
               return TestingUtil.extractGlobalComponent(cm, LocalTopologyManager.class).isRebalancingEnabled();
            } catch (Exception e) {
               fail("Unexpected exception " + e);
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
