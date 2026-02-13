package org.infinispan.rest.resources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.rest.configuration.Protocol.HTTP_11;
import static org.infinispan.client.rest.configuration.Protocol.HTTP_20;
import static org.infinispan.commons.api.CacheContainerAdmin.AdminFlag.VOLATILE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_YAML;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_YAML_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;
import static org.infinispan.commons.internal.InternalCacheNames.CONFIG_STATE_CACHE_NAME;
import static org.infinispan.commons.internal.InternalCacheNames.PROTOBUF_METADATA_CACHE_NAME;
import static org.infinispan.commons.internal.InternalCacheNames.SCRIPT_CACHE_NAME;
import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.infinispan.commons.util.EnumUtil.EMPTY_BIT_SET;
import static org.infinispan.commons.util.Util.getResourceAsString;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.context.Flag.SKIP_CACHE_LOAD;
import static org.infinispan.context.Flag.SKIP_INDEXING;
import static org.infinispan.rest.RequestHeader.ACCEPT_HEADER;
import static org.infinispan.rest.RequestHeader.KEY_CONTENT_TYPE_HEADER;
import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.xml.parsers.DocumentBuilderFactory;

import org.assertj.core.api.Assertions;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.client.rest.MultiPartRestEntity;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestRawClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.configuration.io.NamingStrategy;
import org.infinispan.commons.configuration.io.PropertyReplacer;
import org.infinispan.commons.configuration.io.URLConfigurationResourceResolver;
import org.infinispan.commons.configuration.io.yaml.YamlConfigurationReader;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.test.annotation.TestForIssue;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.globalstate.ScopedState;
import org.infinispan.globalstate.impl.CacheState;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.reactive.publisher.impl.ClusterPublisherManager;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.reactive.publisher.impl.SegmentPublisherSupplier;
import org.infinispan.rest.ResponseHeader;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.security.Security;
import org.infinispan.test.TestException;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.LocalTopologyManager;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.reporters.Files;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import io.reactivex.rxjava3.core.Flowable;

@Test(groups = "functional", testName = "rest.CacheResourceV2Test")
public class CacheResourceV2Test extends AbstractRestResourceTest {
   // Wild guess: a non-empty index (populated with addData) should be more than this many bytes
   private static final long MIN_NON_EMPTY_INDEX_SIZE = 1000L;
   private static final String PERSISTENT_LOCATION = tmpDirectory(CacheResourceV2Test.class.getName());

   private static final String PROTO_SCHEMA =
         """
                /* @Indexed */
                message Entity {
                   /* @Basic */
                   required int32 value=1;
                   optional string description=2;
                }
                /* @Indexed */
                message Another {
                   /* @Basic */
                   required int32 value=1;
                   optional string description=2;
                }
               """;
   public static final String ACCEPT = "Accept";

   protected CacheMode cacheMode;

   @Override
   protected String parameters() {
      return "[security=" + security + ", protocol=" + protocol.toString() + ", ssl=" + ssl + ", cacheMode=" + cacheMode + ", browser=" + browser + "]";
   }

   protected CacheResourceV2Test withCacheMode(CacheMode cacheMode) {
      this.cacheMode = cacheMode;
      return this;
   }

   @Override
   protected void defineCaches(EmbeddedCacheManager cm) {
      ConfigurationBuilder configurationBuilder = getDefaultCacheBuilder();
      if (cacheMode != null) {
         // We force num owners to 1 so that some operations have to go to a remote node
         configurationBuilder.clustering().cacheMode(cacheMode).hash().numOwners(1);
      }
      cm.defineConfiguration("default", configurationBuilder.build());
      cm.defineConfiguration("proto", getProtoCacheBuilder().build());
      cm.defineConfiguration("simple-text", getTextCacheBuilder().build());

      if (security) {
         cm.defineConfiguration("secured-simple-text", getTextCacheBuilder()
               .security().authorization().enable().roles("ADMIN").build());
      }

      cm.getCache(PROTOBUF_METADATA_CACHE_NAME).put("sample.proto", PROTO_SCHEMA);

      cm.defineConfiguration("indexedCache", getIndexedPersistedCache().build());
      cm.defineConfiguration("denyReadWritesCache", getDefaultCacheBuilder().clustering().partitionHandling().whenSplit(PartitionHandling.DENY_READ_WRITES).build());
   }

   public ConfigurationBuilder getProtoCacheBuilder() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);
      return builder;
   }

   public ConfigurationBuilder getTextCacheBuilder() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.encoding().mediaType(TEXT_PLAIN_TYPE);
      return builder;
   }

   @Override
   public Object[] factory() {
      return new Object[]{
            new CacheResourceV2Test().withSecurity(false).protocol(HTTP_11).ssl(false).browser(false),
            new CacheResourceV2Test().withSecurity(false).protocol(HTTP_11).ssl(false).browser(true),
            new CacheResourceV2Test().withSecurity(true).protocol(HTTP_20).ssl(false).browser(false),
            new CacheResourceV2Test().withSecurity(true).protocol(HTTP_20).ssl(false).browser(true),
            new CacheResourceV2Test().withSecurity(true).protocol(HTTP_11).ssl(true).browser(false),
            new CacheResourceV2Test().withSecurity(true).protocol(HTTP_11).ssl(true).browser(true),
            new CacheResourceV2Test().withSecurity(true).protocol(HTTP_20).ssl(true).browser(false),
            new CacheResourceV2Test().withSecurity(true).protocol(HTTP_20).ssl(true).browser(true),
            new CacheResourceV2Test().withCacheMode(CacheMode.DIST_SYNC).withSecurity(false).protocol(HTTP_11).ssl(false).browser(false),
            new CacheResourceV2Test().withCacheMode(CacheMode.DIST_SYNC).withSecurity(false).protocol(HTTP_11).ssl(false).browser(true),
            new CacheResourceV2Test().withCacheMode(CacheMode.DIST_SYNC).withSecurity(true).protocol(HTTP_20).ssl(false).browser(false),
            new CacheResourceV2Test().withCacheMode(CacheMode.DIST_SYNC).withSecurity(true).protocol(HTTP_20).ssl(false).browser(true),
            new CacheResourceV2Test().withCacheMode(CacheMode.DIST_SYNC).withSecurity(true).protocol(HTTP_11).ssl(true).browser(false),
            new CacheResourceV2Test().withCacheMode(CacheMode.DIST_SYNC).withSecurity(true).protocol(HTTP_11).ssl(true).browser(true),
            new CacheResourceV2Test().withCacheMode(CacheMode.DIST_SYNC).withSecurity(true).protocol(HTTP_20).ssl(true).browser(false),
            new CacheResourceV2Test().withCacheMode(CacheMode.DIST_SYNC).withSecurity(true).protocol(HTTP_20).ssl(true).browser(true),
      };
   }

   private ConfigurationBuilder getIndexedPersistedCache() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.statistics().enable();
      builder.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("Entity")
            .addIndexedEntity("Another")
            .statistics().enable()
            .persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).shared(true).storeName("store");
      return builder;
   }

   @Override
   protected void createCacheManagers() throws Exception {
      Util.recursiveFileRemove(PERSISTENT_LOCATION);
      super.createCacheManagers();
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

   @Test
   public void testNonExistingCacheConfig() {
      RestCacheClient cacheClient = adminClient.cache("non-existing");
      try (RestResponse response = join(cacheClient.configuration())) {
         assertThat(response).isNotFound();
      }
   }

   @Test
   public void testCacheV2KeyOps() {
      RestCacheClient cacheClient = client.cache("default");

      RestResponse response = join(cacheClient.post("key", "value"));
      assertThat(response).isOk();

      response = join(cacheClient.post("key", "value"));
      assertThat(response).isConflicted().hasReturnedText("An entry already exists");

      response = join(cacheClient.put("key", "value-new"));
      assertThat(response).isOk();

      response = join(cacheClient.get("key"));
      assertThat(response).hasReturnedText("value-new");

      response = join(cacheClient.head("key"));
      assertThat(response).isOk();
      assertThat(response).hasNoContent();

      response = join(cacheClient.remove("key"));
      assertThat(response).isOk();

      response = join(cacheClient.get("key"));
      assertThat(response).isNotFound();
   }

   @Test
   public void testCreateCacheEncodedName() {
      testCreateAndUseCache("a/");
      testCreateAndUseCache("a/b/c");
      testCreateAndUseCache("a-b-c");
      testCreateAndUseCache("áb\\ćé/+-$");
      testCreateAndUseCache("org.infinispan.cache");
      testCreateAndUseCache("a%25bc");
   }

   @Test
   public void testCreateCacheEncoding() {
      String cacheName = "encoding-test";
      String json = "{\"local-cache\":{\"encoding\":{\"media-type\":\"text/plain\"}}}";

      createCache(json, cacheName);
      String cacheConfig = getCacheConfig(APPLICATION_JSON_TYPE, cacheName);

      Json encoding = Json.read(cacheConfig).at(cacheName).at("local-cache").at("encoding");
      Json mediaType = encoding.at("media-type");

      assertEquals(TEXT_PLAIN_TYPE, mediaType.asString());
   }

   private void testCreateAndUseCache(String name) {
      String cacheConfig = "{\"distributed-cache\":{\"mode\":\"SYNC\"}}";

      RestCacheClient cacheClient = client.cache(name);
      RestEntity config = RestEntity.create(APPLICATION_JSON, cacheConfig);
      CompletionStage<RestResponse> response = cacheClient.createWithConfiguration(config);

      assertThat(response).isOk();

      CompletionStage<RestResponse> sizeResponse = cacheClient.size();
      assertThat(sizeResponse).isOk();
      assertThat(sizeResponse).containsReturnedText("0");


      RestResponse namesResponse = join(client.caches());
      assertThat(namesResponse).isOk();
      List<String> names = Json.read(namesResponse.body()).asJsonList().stream().map(Json::asString).toList();
      assertTrue(names.contains(name));

      CompletionStage<RestResponse> putResponse = cacheClient.post("key", "value");
      assertThat(putResponse).isOk();

      CompletionStage<RestResponse> getResponse = cacheClient.get("key");
      assertThat(getResponse).isOk();
      assertThat(getResponse).containsReturnedText("value");
   }

   @Test
   public void testCreateAndAlterCache() {
      String cacheConfig = """
            {
              "distributed-cache" : {
                "mode" : "SYNC",
                "statistics" : true,
                "encoding" : {
                  "key" : {
                    "media-type" : "application/x-protostream"
                  },
                  "value" : {
                    "media-type" : "application/x-protostream"
                  }
                },
                "expiration" : {
                  "lifespan" : "60000"
                },
                "memory" : {
                  "max-count" : "1000",
                  "when-full" : "REMOVE"
                }
              }
            }
            """;
      String cacheConfigAlter = """
            {
              "distributed-cache" : {
                "mode" : "SYNC",
                "statistics" : true,
                "encoding" : {
                  "key" : {
                    "media-type" : "application/x-protostream"
                  },
                  "value" : {
                    "media-type" : "application/x-protostream"
                  }
                },
                "expiration" : {
                  "lifespan" : "30000"
                },
                "memory" : {
                  "max-count" : "2000",
                  "when-full" : "REMOVE"
                }
              }
            }
            """;
      String cacheConfigConflict = """
            {
              "distributed-cache" : {
                "mode" : "ASYNC"
              }
            }
            """;

      RestCacheClient cacheClient = client.cache("mutable");
      CompletionStage<RestResponse> response = cacheClient.createWithConfiguration(RestEntity.create(APPLICATION_JSON, cacheConfig));
      assertThat(response).isOk();
      response = cacheClient.updateWithConfiguration(RestEntity.create(APPLICATION_JSON, cacheConfigAlter));
      assertThat(response).isOk();

      response = adminClient.cache("mutable").configuration();
      assertThat(response).isOk();
      String configFromServer = join(response).body();

      assertTrue(configFromServer.contains("\"expiration\":{\"lifespan\":\"30000\"}"));
      assertTrue(configFromServer.contains("\"memory\":{\"max-count\":\"2000\""));

      response = cacheClient.updateWithConfiguration(RestEntity.create(APPLICATION_JSON, cacheConfigConflict));
      assertThat(response).isBadRequest();
   }

   @Test
   public void testUpdateFailure() {
      String cacheConfig = "localCache:\n  encoding:\n    mediaType: \"application/x-protostream\"\n";
      String cacheConfigAlter = "localCache:\n  encoding:\n    mediaType: \"application/x-java-serialized-object\"\n";

      RestCacheClient cacheClient = client.cache("updateFailure");
      CompletionStage<RestResponse> response = cacheClient.createWithConfiguration(RestEntity.create(APPLICATION_YAML, cacheConfig));
      assertThat(response).isOk();
      response = cacheClient.updateWithConfiguration(RestEntity.create(APPLICATION_YAML, cacheConfigAlter));
      assertThat(response).isBadRequest();
      String body = join(response).body();
      assertThat(body).contains("ISPN000961: Incompatible attribute");
   }

   @Test
   public void testMutableAttributes() {
      String cacheName = "mutable-attributes";
      String json = "{\"local-cache\":{\"encoding\":{\"media-type\":\"text/plain\"}}}";
      RestCacheClient cacheClient = createCache(adminClient, json, cacheName);
      CompletionStage<RestResponse> response = cacheClient.configurationAttributes(true);
      assertThat(response).isOk();
      Json attributes = Json.read(join(response).body());
      assertEquals(18, attributes.asJsonMap().size());
      assertEquals("timequantity", attributes.at("clustering.remote-timeout").at("type").asString());
      assertEquals("15s", attributes.at("clustering.remote-timeout").at("value").asString());
      assertThat(attributes.at("indexing.indexed-entities").at("type").asString()).isEqualTo("set");
      assertThat(attributes.at("indexing.indexed-entities").at("value").asList()).isEmpty();
   }

   @Test
   public void testUpdateConfigurationAttribute() {
      String protoSchema = """
            package org.infinispan;
            /**
             * @Indexed
             */
            message Developer {
               /**
                * @Basic
                */
               optional string nick = 1;
               /**
                * @Basic(sortable=true)
                */
               optional int32 contributions = 2;
            }
            /**
             * @Indexed
             */
            message Engineer {
               /**
                * @Basic
                */
               optional string nick = 1;
               /**
                * @Basic(sortable=true)
                */
               optional int32 contributions = 2;
            }
            """;

      // Register schema
      RestResponse restResponse = join(client.schemas().put("dev.proto", protoSchema));
      assertThat(restResponse).isOk();

      // Create the indexed cache
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable().storage(LOCAL_HEAP).addIndexedEntities("org.infinispan.Developer");
      if (isSecurityEnabled()) {
         builder.security().authorization().roles("ADMIN");
      }
      String cacheConfig = cacheConfigToJson("developers", builder.build());

      RestCacheClient cacheClient = adminClient.cache("developers");

      RestEntity config = RestEntity.create(APPLICATION_JSON, cacheConfig);
      CompletionStage<RestResponse> response = cacheClient.createWithConfiguration(config);
      assertThat(response).isOk();

      response = cacheClient.updateConfigurationAttribute("indexing.indexed-entities",
            "org.infinispan.Developer org.infinispan.Engineer");
      assertThat(response).isOk();

      response = cacheClient.configuration();
      assertThat(response).isOk();
      String configFromServer = join(response).body();

      assertThat(configFromServer)
            .contains("\"indexed-entities\":[\"org.infinispan.Engineer\",\"org.infinispan.Developer\"]");
   }

   @Test
   public void testCacheV2LifeCycle() throws Exception {
      String xml = getResourceAsString("cache.xml", getClass().getClassLoader());
      String json = getResourceAsString("cache.json", getClass().getClassLoader());

      RestEntity xmlEntity = RestEntity.create(APPLICATION_XML, xml);
      RestEntity jsonEntity = RestEntity.create(APPLICATION_JSON, json);

      CompletionStage<RestResponse> response = client.cache("cache1").createWithConfiguration(xmlEntity, VOLATILE);
      assertThat(response).isOk();
      assertPersistence("cache1", false);

      response = client.cache("cache2").createWithConfiguration(jsonEntity);
      assertThat(response).isOk();
      assertPersistence("cache2", true);

      String mediaList = "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";
      response = adminClient.cache("cache1").configuration(mediaList);
      assertThat(response).isOk();
      String cache1Cfg = join(response).body();

      response = adminClient.cache("cache2").configuration();
      assertThat(response).isOk();
      String cache2Cfg = join(response).body();

      assertEquals(cache1Cfg, cache2Cfg.replace("cache2", "cache1"));

      response = adminClient.cache("cache1").configuration("application/xml");
      assertThat(response).isOk();
      String cache1Xml = join(response).body();

      ParserRegistry registry = new ParserRegistry();
      Configuration xmlConfig = registry.parse(cache1Xml).getCurrentConfigurationBuilder().build();
      assertEquals(1200000, xmlConfig.clustering().l1().lifespan());
      assertEquals(60500, xmlConfig.clustering().stateTransfer().timeout());

      response = adminClient.cache("cache1").configuration("application/xml; q=0.9");
      assertThat(response).isOk();
   }

   @Test
   public void testCreateDeleteCache() throws Exception {
      createDeleteCache(getResourceAsString("cache.xml", getClass().getClassLoader()));
   }

   @Test
   public void testCreateDeleteCacheFromFragment() throws Exception {
      createDeleteCache(getResourceAsString("cache-fragment.xml", getClass().getClassLoader()));
   }

   private void createDeleteCache(String xml) {
      RestEntity xmlEntity = RestEntity.create(APPLICATION_XML, xml);

      RestCacheClient cacheClient = client.cache("cacheCRUD");

      CompletionStage<RestResponse> response = cacheClient.createWithConfiguration(xmlEntity, VOLATILE);
      assertThat(response).isOk();

      response = cacheClient.stats();
      assertThat(response).isOk().hasJson().hasProperty("current_number_of_entries").is(-1);

      response = cacheClient.delete();
      assertThat(response).isOk();

      response = cacheClient.stats();
      assertThat(response).isNotFound().hasReturnedText("ISPN012010: Cache with name 'cacheCRUD' not found amongst the configured caches");
   }

   private void assertPersistence(String name, boolean persisted) {
      EmbeddedCacheManager cm = cacheManagers.iterator().next();
      Cache<ScopedState, CacheState> configCache = cm.getCache(CONFIG_STATE_CACHE_NAME);
      assertEquals(persisted, configCache.entrySet()
            .stream().anyMatch(e -> e.getKey().getName().equals(name) && !e.getValue().getFlags().contains(VOLATILE)));
   }

   @Test
   public void testCacheV2Stats() {
      String cacheJson = "{ \"distributed-cache\" : { \"statistics\":true } }";
      RestCacheClient cacheClient = client.cache("statCache");

      RestEntity jsonEntity = RestEntity.create(APPLICATION_JSON, cacheJson);
      CompletionStage<RestResponse> response = cacheClient.createWithConfiguration(jsonEntity, VOLATILE);
      assertThat(response).isOk();

      putStringValueInCache("statCache", "key1", "data");
      putStringValueInCache("statCache", "key2", "data");

      response = cacheClient.stats();
      assertThat(response).isOk();

      Json jsonNode = Json.read(join(response).body());
      assertEquals(jsonNode.at("current_number_of_entries").asInteger(), 2);
      assertEquals(jsonNode.at("stores").asInteger(), 2);

      response = cacheClient.clear();
      assertThat(response).isOk();
      response = cacheClient.stats();
      assertThat(response).isOk().hasJson().hasProperty("current_number_of_entries").is(0);
   }

   @Test
   @TestForIssue(jiraKey = "ISPN-14957")
   public void getCacheInfoInternalCache() {
      RestCacheClient scriptCache = client.cache(SCRIPT_CACHE_NAME);
      CompletionStage<RestResponse> details = scriptCache.details();
      assertThat(details).isOk();

      RestResponse response = join(details);
      String body = response.body();
      assertThat(body).isNotBlank();
   }

   @Test
   public void testCacheV2Distribution() {
      String cacheJson = "{ \"distributed-cache\" : { \"statistics\":true, \"memory\" : {"
            + "\"storage\": \"OFF_HEAP\", \"max-size\": \"1MB\" } } }";
      RestCacheClient cacheClient = client.cache("distributionCache");

      RestEntity jsonEntity = RestEntity.create(APPLICATION_JSON, cacheJson);
      CompletionStage<RestResponse> response = cacheClient.createWithConfiguration(jsonEntity, VOLATILE);
      assertThat(response).isOk();

      putStringValueInCache("distributionCache", "key1", "data");
      putStringValueInCache("distributionCache", "key2", "data");

      response = cacheClient.distribution();
      assertThat(response).isOk();

      Json jsonNode = Json.read(join(response).body());
      assertTrue(jsonNode.isArray());
      List<Json> jsons = jsonNode.asJsonList();

      assertEquals(NUM_SERVERS, jsons.size());
      Pattern pattern = Pattern.compile(this.getClass().getSimpleName() + "-Node[a-zA-Z]$");
      Map<String, Long> previousSizes = new HashMap<>();
      for (Json node : jsons) {
         assertEquals(node.at("memory_entries").asInteger(), 2);
         assertEquals(node.at("total_entries").asInteger(), 2);
         assertEquals(node.at("node_addresses").asJsonList().size(), 1);
         assertTrue(pattern.matcher(node.at("node_name").asString()).matches());
         assertTrue(node.at("memory_used").asLong() > 0);

         previousSizes.put(node.at("node_name").asString(), node.at("memory_used").asLong());
      }

      response = cacheClient.clear();
      assertThat(response).isOk();

      response = cacheClient.distribution();
      assertThat(response).isOk();
      jsonNode = Json.read(join(response).body());

      assertTrue(jsonNode.isArray());
      jsons = jsonNode.asJsonList();
      assertEquals(NUM_SERVERS, jsons.size());
      for (Json node : jsons) {
         assertEquals(node.at("memory_entries").asInteger(), 0);
         assertEquals(node.at("total_entries").asInteger(), 0);

         // Even though the cache was cleared, it still occupies some space.
         assertTrue(node.at("memory_used").asLong() > 0);

         // But less space than before.
         assertTrue(node.at("memory_used").asLong() < previousSizes.get(node.at("node_name").asString()));
      }
   }

   @Test
   public void testCacheV2KeyDistribution() {
      final String cacheName = "keyDistribution";
      String cacheJson = "{ \"distributed-cache\" : { \"statistics\":true } }";
      createCache(cacheJson, cacheName);

      RestCacheClient cacheClient = client.cache(cacheName);

      putStringValueInCache(cacheName, "key1", "data");
      putStringValueInCache(cacheName, "key2", "data");
      Map<String, Boolean> sample = Map.of("key1", true, "key2", true, "unknown", false);

      for (Map.Entry<String, Boolean> entry : sample.entrySet()) {
         CompletionStage<RestResponse> response = cacheClient.distribution(entry.getKey());
         assertThat(response).isOk();

         try (RestResponse restResponse = join(response)) {
            Json jsonNode = Json.read(restResponse.body());
            assertEquals((boolean) entry.getValue(), jsonNode.at("contains_key").asBoolean());
            assertTrue(jsonNode.at("owners").isArray());

            List<Json> distribution = jsonNode.at("owners").asJsonList();
            assertEquals(NUM_SERVERS, distribution.size());

            Pattern pattern = Pattern.compile(this.getClass().getSimpleName() + "-Node[a-zA-Z]$");
            for (Json node : distribution) {
               assertEquals(node.at("node_addresses").asJsonList().size(), 1);
               assertTrue(pattern.matcher(node.at("node_name").asString()).matches());
               assertTrue(node.has("primary"));
            }

            if (entry.getValue()) {
               assertTrue(distribution.stream().anyMatch(n -> n.at("primary").asBoolean()));
            } else {
               assertTrue(distribution.stream().noneMatch(n -> n.at("primary").asBoolean()));
            }
         } catch (Exception e) {
            throw new TestException(e);
         }
      }
   }

   @Test
   public void testCacheSize() {
      for (int i = 0; i < 100; i++) {
         putInCache("default", i, "" + i, TEXT_PLAIN_TYPE);
      }

      CompletionStage<RestResponse> response = client.cache("default").size();

      assertThat(response).isOk();
      assertThat(response).containsReturnedText("100");
   }

   @Test
   public void testCaches() {
      RestResponse response = join(adminClient.detailedCacheList());
      ResponseAssertion.assertThat(response).isOk();

      String json = response.body();
      Json jsonNode = Json.read(json);
      List<String> names = find(jsonNode, "name");
      Set<String> expectedNames = new HashSet<>(cacheManagers.get(0).getCacheNames());
      expectedNames.remove(SCRIPT_CACHE_NAME);
      expectedNames.remove(PROTOBUF_METADATA_CACHE_NAME);

      assertEquals(expectedNames, new HashSet<>(names));

      List<String> status = find(jsonNode, "status");
      Assert.assertTrue(status.contains("RUNNING"));

      List<String> types = find(jsonNode, "type");
      Assert.assertTrue(types.contains("distributed-cache"));

      List<String> simpleCaches = find(jsonNode, "simple_cache");
      Assert.assertTrue(simpleCaches.contains("false"));

      List<String> transactional = find(jsonNode, "transactional");
      Assert.assertTrue(transactional.contains("false"));

      List<String> persistent = find(jsonNode, "persistent");
      Assert.assertTrue(persistent.contains("false"));

      List<String> bounded = find(jsonNode, "bounded");
      Assert.assertTrue(bounded.contains("false"));

      List<String> secured = find(jsonNode, "secured");
      Assert.assertTrue(secured.contains("false"));

      List<String> indexed = find(jsonNode, "indexed");
      Assert.assertTrue(indexed.contains("false"));

      List<String> hasRemoteBackup = find(jsonNode, "has_remote_backup");
      Assert.assertTrue(hasRemoteBackup.contains("false"));

      List<String> health = find(jsonNode, "health");

      Assert.assertTrue(health.contains("HEALTHY"));

      List<String> isRebalancingEnabled = find(jsonNode, "rebalancing_enabled");
      Assert.assertTrue(isRebalancingEnabled.contains("true"));

      List<String> tracing = find(jsonNode, "tracing");
      Assert.assertFalse(tracing.isEmpty());

      List<String> aliases = find(jsonNode, "aliases");
      Assert.assertFalse(aliases.isEmpty());
   }

   private List<String> find(Json array, String name) {
      return array.asJsonList().stream().map(j -> j.at(name).getValue().toString()).collect(Collectors.toList());
   }

   @Test
   public void testCacheFullDetail() {
      RestResponse response = join(adminClient.cache("proto").details());
      Json document = Json.read(response.body());
      assertThat(response).isOk();
      assertThat(document.at("name").asString()).isEqualTo("proto");
      assertThat(document.at("status").asString()).isEqualTo("RUNNING");
      assertThat(document.at("type").asString()).isEqualTo("distributed-cache");
      assertThat(document.at("stats")).isNotNull();
      assertThat(document.at("size")).isNotNull();
      assertThat(document.at("configuration")).isNotNull();
      assertThat(document.at("rehash_in_progress").asBoolean()).isFalse();
      assertThat(document.at("persistent").asBoolean()).isFalse();
      assertThat(document.at("bounded").asBoolean()).isFalse();
      assertThat(document.at("indexed").asBoolean()).isFalse();
      assertThat(document.at("has_remote_backup").asBoolean()).isFalse();
      assertThat(document.at("secured").asBoolean()).isFalse();
      assertThat(document.at("tracing").asBoolean()).isFalse();
      assertThat(document.at("indexing_in_progress").asBoolean()).isFalse();
      assertThat(document.at("aliases")).isNotNull();
      assertThat(document.at("queryable")).isNotNull();
      assertThat(document.at("rebalancing_enabled")).isNotNull();
      assertThat(document.at("rebalancing_requested")).isNotNull();
      assertThat(document.at("rebalancing_inflight")).isNotNull();
      assertThat(document.at("key_storage").asString()).isEqualTo("application/x-protostream");
      assertThat(document.at("value_storage").asString()).isEqualTo("application/x-protostream");
      assertThat(document.at("mode").asString()).isEqualTo("DIST_SYNC");
      assertThat(document.at("storage_type").asString()).isEqualTo("HEAP");
      assertThat(document.at("max_size").asString()).isEmpty();
      assertThat(document.at("max_size_bytes").asLong()).isEqualTo(-1);


      // non admins should have an empty config
      if (security) {
         response = join(client.cache("default").details());
         document = Json.read(response.body());
         assertThat(response).isOk();
         assertThat(document.at("configuration")).isNull();
         assertThat(document.at("storage_type")).isNull();
         assertThat(document.at("max_size")).isNull();
         assertThat(document.at("max_size_bytes")).isNull();
         assertThat(document.at("name").asString()).isEqualTo("default");
         assertThat(document.at("status").asString()).isEqualTo("RUNNING");
         assertThat(document.at("type").asString()).isEqualTo("distributed-cache");
      }

      response = join(client.cache("proto").details());
      document = Json.read(response.body());
      assertThat(document.at("key_storage").asString()).isEqualTo("application/x-protostream");
      assertThat(document.at("value_storage").asString()).isEqualTo("application/x-protostream");
   }

   public void testCacheQueryable() {
      // Default config
      createCache(new ConfigurationBuilder(), "cacheNotQueryable");
      Json details = getCacheDetail("cacheNotQueryable");
      assertFalse(details.at("queryable").asBoolean());

      // Indexed
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable().storage(LOCAL_HEAP);
      builder.indexing().enable().addIndexedEntity("Entity");
      createCache(builder, "cacheIndexed");
      details = getCacheDetail("cacheIndexed");
      assertTrue(details.at("queryable").asBoolean());

      // NonIndexed
      ConfigurationBuilder proto = new ConfigurationBuilder();
      proto.encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);
      createCache(proto, "cacheQueryable");
      details = getCacheDetail("cacheQueryable");
      assertTrue(details.at("queryable").asBoolean());
   }

   @Test
   public void testCreateInvalidCache() {
      String invalidConfig = """
            <infinispan>
             <cache-container>
               <replicated-cache name="books">
                 <encoding media-type="application/x-java-object"/>
                 <indexing>
                   <indexed-entities>
                     <indexed-entity>Dummy</indexed-entity>
                    </indexed-entities>
                 </indexing>
               </replicated-cache>
             </cache-container>
            </infinispan>""";

      CompletionStage<RestResponse> response = client.cache("CACHE").createWithConfiguration(RestEntity.create(APPLICATION_XML, invalidConfig));
      assertThat(response).isBadRequest().containsReturnedText("Cannot instantiate class 'Dummy'");

      response = client.cache("CACHE").exists();
      assertThat(response).isOk();

      CompletionStage<RestResponse> healthResponse = client.container().health();
      assertThat(healthResponse).isOk().containsReturnedText("{\"status\":\"FAILED\",\"cache_name\":\"CACHE\"}");

      CompletionStage<RestResponse> cacheHealthResponse = client.cache("CACHE").health();
      assertThat(cacheHealthResponse).isOk();

      // we need admin for this one
      CompletionStage<RestResponse> configResponse = adminClient.cache("CACHE").configuration();
      assertThat(configResponse).isOk();

      // The only way to recover from a broken cache is to delete it
      response = client.cache("CACHE").delete();
      assertThat(response).isOk();

      response = client.cache("CACHE").exists();
      assertThat(response).isNotFound();

      invalidConfig = """
            <distributed-cache>
               <encoding>
                  <key media-type="application/x-protostream"/>
                  <value media-type="application/x-protostream"/>
               </encoding
            </distributed-cache>
            """;
      response = client.cache("CACHE").createWithConfiguration(RestEntity.create(APPLICATION_XML, invalidConfig));
      assertThat(response).isBadRequest().hasReturnedText("expected > to finish end tag not < from line 2 (position: TEXT seen ...<value media-type=\"application/x-protostream\"/>\\n   </encoding\\n<... @6:2) ");

      response = client.cache("CACHE").exists();
      assertThat(response).isNotFound();

      invalidConfig = """
            <distributed-cache>
               <encoding>
                  <key media-type="application/x-protostream"/>
                  <value media-type="application/x-protostrea"/>
               </encoding>
            </distributed-cache>""";
      response = client.cache("CACHE").createWithConfiguration(RestEntity.create(APPLICATION_XML, invalidConfig));
      assertThat(response).isBadRequest().hasReturnedText("ISPN000492: Cannot find transcoder between 'application/x-java-object' to 'application/x-protostrea'");

      response = client.cache("CACHE").delete();
      assertThat(response).isOk();
   }

   private RestCacheClient createCache(ConfigurationBuilder builder, String name) {
      return createCache(cacheConfigToJson(name, builder.build()), name);
   }

   private RestCacheClient createCache(RestClient c, String json, String name) {
      RestEntity jsonEntity = RestEntity.create(APPLICATION_JSON, json);

      RestCacheClient cache = c.cache(name);
      CompletionStage<RestResponse> response = cache.createWithConfiguration(jsonEntity);
      assertThat(response).isOk();
      return cache;
   }

   private RestCacheClient createCache(String json, String name) {
      return createCache(client, json, name);
   }

   private Json getCacheDetail(String name) {
      RestResponse response = join(client.cache(name).details());

      assertThat(response).isOk();

      return Json.read(response.body());
   }

   @Test
   public void testCacheNames() {
      CompletionStage<RestResponse> response = adminClient.caches();

      assertThat(response).isOk();

      List responseCacheNames = Json.read(join(response).body()).asList();
      assertThat(responseCacheNames).containsExactlyElementsOf(cacheManagers.get(0).getCacheNames());

      assertThat(client.caches()).isOk();
   }

   @Test
   public void testCacheHealth() {
      String cacheName = "default";
      CompletionStage<RestResponse> response = client.cache(cacheName).health();
      assertThat(response).isOk().hasReturnedText("HEALTHY");

      Cache<?, ?> cache = cacheManagers.get(0).getCache(cacheName);
      ComponentRegistry cr = TestingUtil.extractComponentRegistry(cache);
      PartitionHandlingManager phm = cr.getComponent(PartitionHandlingManager.class);
      PartitionHandlingManager spyPhm = Mockito.spy(phm);
      Mockito.when(spyPhm.getAvailabilityMode()).thenReturn(AvailabilityMode.DEGRADED_MODE);
      TestingUtil.replaceComponent(cache, PartitionHandlingManager.class, spyPhm, true);
      response = client.cache(cacheName).health();
      assertThat(response).isOk().hasReturnedText("DEGRADED");

      response = client.cache("UnknownCache").health();
      assertThat(response).isNotFound();
   }

   @Test
   public void testFlags() {
      RestResponse response = insertEntity(1, 1000);
      assertThat(response).isOk();
      assertIndexed(1000);

      response = insertEntity(2, 1200, SKIP_INDEXING.toString(), SKIP_CACHE_LOAD.toString());
      assertThat(response).isOk();
      assertNotIndexed(1200);

      response = insertEntity(3, 1200, "Invalid");
      assertThat(response).isBadRequest().containsReturnedText("No enum constant org.infinispan.context.Flag.Invalid");
   }

   @Test
   public void testValidateCacheQueryable() {
      registerSchema("simple.proto", "message Simple { required int32 value=1;}");
      correctReportNotQueryableCache("jsonCache", new ConfigurationBuilder().encoding().mediaType(APPLICATION_JSON_TYPE).build());
   }

   private void correctReportNotQueryableCache(String name, Configuration configuration) {
      createAndWriteToCache(name, configuration);

      RestResponse response = queryCache(name);
      assertThat(response).isBadRequest();

      Json json = Json.read(response.body());
      assertTrue(json.at("error").at("cause").toString().matches(".*ISPN028015.*"));
   }

   private RestResponse queryCache(String name) {
      return join(client.cache(name).query("FROM Simple"));
   }

   private void createAndWriteToCache(String name, Configuration configuration) {
      String jsonConfig = cacheConfigToJson(name, configuration);
      RestEntity configEntity = RestEntity.create(APPLICATION_JSON, jsonConfig);

      CompletionStage<RestResponse> response = client.cache(name).createWithConfiguration(configEntity);
      assertThat(response).isOk();

      RestEntity valueEntity = RestEntity.create(APPLICATION_JSON, "{\"_type\":\"Simple\",\"value\":1}");

      response = client.cache(name).post("1", valueEntity);
      assertThat(response).isOk();
   }

   @Test
   public void testKeyStreamWithFailure() {
      String exceptionMessage = "Expected failure";
      Cache<?, ?> c = cacheManagers.get(0).getCache("default");
      ComponentRegistry ccr = TestingUtil.extractComponentRegistry(c);
      ClusterPublisherManager<?, ?> cpm = ccr.getClusterPublisherManager().running();
      ClusterPublisherManager<?, ?> spyCpm = Mockito.spy(cpm);
      Mockito.doAnswer(ivk -> {
         SegmentPublisherSupplier<Object> sps = (SegmentPublisherSupplier<Object>) ivk.callRealMethod();
         SegmentPublisherSupplier<Object> spySps = Mockito.spy(sps);
         Mockito.doAnswer(ignore -> Flowable.error(new RuntimeException(exceptionMessage)))
               .when(spySps).publisherWithoutSegments();
         return spySps;
      }).when(spyCpm).keyPublisher(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.eq(EMPTY_BIT_SET),
            Mockito.eq(DeliveryGuarantee.EXACTLY_ONCE), Mockito.eq(1000), Mockito.any());
      TestingUtil.replaceComponent(c, ClusterPublisherManager.class, spyCpm, true);

      if (protocol == HTTP_11) {
         Exceptions.expectCompletionException(IOException.class, "HTTP/1.1 header parser received no bytes", client.cache("default").keys());
      } else {
         try (RestResponse res = join(client.cache("default").keys())) {
            Assertions.assertThatThrownBy(res::body)
                  .rootCause()
                  .isInstanceOf(IOException.class)
                  .hasMessage("Received RST_STREAM: Stream cancelled");
         }
      }
      TestingUtil.replaceComponent(c, ClusterPublisherManager.class, cpm, true);
   }

   @Test
   public void testEntryStreamWithFailure() {
      String exceptionMessage = "Expected failure";
      Cache<?, ?> c = cacheManagers.get(0).getCache("default");
      ComponentRegistry ccr = TestingUtil.extractComponentRegistry(c);
      ClusterPublisherManager<?, ?> cpm = ccr.getClusterPublisherManager().running();
      ClusterPublisherManager<?, ?> spyCpm = Mockito.spy(cpm);
      Mockito.doAnswer(ivk -> {
         SegmentPublisherSupplier<Object> sps = (SegmentPublisherSupplier<Object>) ivk.callRealMethod();
         SegmentPublisherSupplier<Object> spySps = Mockito.spy(sps);
         Mockito.doAnswer(ignore -> Flowable.error(new RuntimeException(exceptionMessage)))
               .when(spySps).publisherWithoutSegments();
         return spySps;
      }).when(spyCpm).entryPublisher(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.eq(EMPTY_BIT_SET),
            Mockito.eq(DeliveryGuarantee.EXACTLY_ONCE), Mockito.eq(1000), Mockito.any());
      TestingUtil.replaceComponent(c, ClusterPublisherManager.class, spyCpm, true);

      if (protocol == HTTP_11) {
         Exceptions.expectCompletionException(IOException.class, "HTTP/1.1 header parser received no bytes", client.cache("default").entries());
      } else {
         try (RestResponse res = join(client.cache("default").entries())) {
            Assertions.assertThatThrownBy(res::body)
                  .rootCause()
                  .isInstanceOf(IOException.class)
                  .hasMessage("Received RST_STREAM: Stream cancelled");
         }
      }
      TestingUtil.replaceComponent(c, ClusterPublisherManager.class, cpm, true);
   }

   @Test
   public void testMultiByte() {
      putTextEntryInCache("default", "José", "Uberlândia");
      RestResponse response = join(client.cache("default").keys());
      String body = response.body();
      Collection<Json> singleSet = Json.read(body).asJsonList();
      assertEquals(1, singleSet.size());
      assertTrue(singleSet.contains(Json.factory().string("José")));

      response = join(client.cache("default").entries());
      body = response.body();
      singleSet = Json.read(body).asJsonList();
      assertEquals(1, singleSet.size());
      Json entity = singleSet.stream().findFirst().orElseThrow();
      assertTrue(entity.has("key"));
      assertTrue(entity.has("value"));
      assertEquals("José", entity.at("key").asString());
      assertEquals("Uberlândia", entity.at("value").asString());
   }

   @Test
   public void testGetAllKeys() {
      RestResponse response = join(client.cache("default").keys());
      Collection<?> emptyKeys = Json.read(response.body()).asJsonList();
      assertEquals(0, emptyKeys.size());

      putTextEntryInCache("default", "1", "value");
      response = join(client.cache("default").keys());
      Collection<?> singleSet = Json.read(response.body()).asJsonList();
      assertEquals(1, singleSet.size());

      int entries = 10;
      for (int i = 0; i < entries; i++) {
         putTextEntryInCache("default", String.valueOf(i), "value");
      }
      response = join(client.cache("default").keys());
      Set<?> keys = Json.read(response.body()).asJsonList().stream().map(Json::asInteger).collect(Collectors.toSet());
      assertEquals(entries, keys.size());
      assertTrue(IntStream.range(0, entries).allMatch(keys::contains));

      response = join(client.cache("default").keys(5));
      Set<?> keysLimited = Json.read(response.body()).asJsonList().stream().map(Json::asInteger).collect(Collectors.toSet());
      assertEquals(5, keysLimited.size());
   }

   @Test
   public void testGetAllKeysWithDifferentType() {
      String cacheJson = "{    \"distributed-cache\": { \"mode\": \"SYNC\","
            + " \"encoding\": {"
            + " \"key\": {\"media-type\": \"application/json\"},"
            + " \"value\": {\"media-type\": \"application/xml\"}}}}";
      String value = "<?xml version=\"1.0\"?>\n"
            + "<log category=\"CLUSTER\">\n"
            + "    <content level=\"INFO\" message=\"hello\" detail=\"testing\"/>\n"
            + "    <meta instant=\"42\" context=\"testing\" scope=\"\" who=\"\"/>\n"
            + "</log>\n";
      String cacheName = "xmlCaching";
      RestCacheClient cacheClient = client.cache(cacheName);

      RestEntity jsonEntity = RestEntity.create(APPLICATION_JSON, cacheJson);
      CompletionStage<RestResponse> r = cacheClient.createWithConfiguration(jsonEntity, VOLATILE);
      assertThat(r).isOk();

      RestResponse response = join(client.cache(cacheName).keys());
      Collection<?> emptyKeys = Json.read(response.body()).asJsonList();
      assertEquals(0, emptyKeys.size());

      // Test key with escape.
      putInCache(cacheName, "{\"text\": \"I'm right \\\\\"here\\\\\".\"}", APPLICATION_JSON_TYPE, value, APPLICATION_XML);
      response = join(client.cache(cacheName).keys());
      Collection<?> singleSet = Json.read(response.body()).asJsonList();
      assertEquals(1, singleSet.size());
      join(client.cache(cacheName).clear());

      int entries = 10;
      for (int i = 0; i < entries; i++) {
         putInCache(cacheName, String.format("{\"v\": %d}", i), APPLICATION_JSON_TYPE, value, APPLICATION_XML);
      }
      response = join(client.cache(cacheName).keys());
      List<Json> keys = Json.read(response.body()).asJsonList();
      assertEquals(entries, keys.size());

      response = join(client.cache(cacheName).keys(5));
      List<?> keysLimited = Json.read(response.body()).asJsonList();
      assertEquals(5, keysLimited.size());
   }

   private void putInCache(String cacheName, String key, String keyType, String value, MediaType valueType) {
      CompletionStage<RestResponse> r = client.cache(cacheName).put(key, keyType, RestEntity.create(valueType, value));
      ResponseAssertion.assertThat(r).isOk();
   }

   @Test
   public void testStreamEntries() {
      RestResponse response = join(client.cache("default").entries());
      Collection<?> emptyEntries = Json.read(response.body()).asJsonList();
      assertEquals(0, emptyEntries.size());
      putTextEntryInCache("default", "key_0", "value_0");
      response = join(client.cache("default").entries());
      Collection<?> singleSet = Json.read(response.body()).asJsonList();
      assertEquals(1, singleSet.size());
      for (int i = 0; i < 20; i++) {
         putTextEntryInCache("default", "key_" + i, "value_" + i);
      }
      response = join(client.cache("default").entries());
      List<Json> jsons = Json.read(response.body()).asJsonList();
      assertEquals(20, jsons.size());

      response = join(client.cache("default").entries(3));
      jsons = Json.read(response.body()).asJsonList();
      assertEquals(3, jsons.size());
      Json first = jsons.get(0);
      String entry = first.toPrettyString();
      assertThat(entry).contains("\"key\" : \"key_");
      assertThat(entry).contains("\"value\" : \"value_");
      assertThat(entry).doesNotContain("timeToLiveSeconds");
      assertThat(entry).doesNotContain("maxIdleTimeSeconds");
      assertThat(entry).doesNotContain("created");
      assertThat(entry).doesNotContain("lastUsed");
      assertThat(entry).doesNotContain("expireTime");
   }

   @Test
   public void testStreamComplexProtobufEntries() {
      RestResponse response = join(client.cache("indexedCache").entries(-1, false));
      Collection<?> emptyEntries = Json.read(response.body()).asJsonList();
      assertEquals(0, emptyEntries.size());
      insertEntity(3, "Another", 3, "Three");
      response = join(client.cache("indexedCache").entries(-1, true));

      if (response.status() != 200) {
         Assertions.fail(response.body());
      }
      List<Json> jsons = Json.read(response.body()).asJsonList();
      assertThat(jsons).hasSize(1);

      response = join(client.cache("indexedCache").keys());
      if (response.status() != 200) {
         Assertions.fail(response.body());
      }
      jsons = Json.read(response.body()).asJsonList();
      assertThat(jsons).hasSize(1);
      assertThat(jsons).contains(Json.factory().string("3"));
   }

   private String asString(Json json) {
      return json.isObject() ? json.toString() : json.asString();
   }

   private void testStreamEntriesFromCache(String cacheName, MediaType cacheMediaType, MediaType writeMediaType, Map<String, String> data) {
      // Create the cache with the supplied encoding
      createCache(cacheName, cacheMediaType);

      // Write entries with provided data and writeMediaType
      data.forEach((key, value) -> writeEntry(key, value, cacheName, writeMediaType));

      // Get entries
      RestCacheClient cacheClient = client.cache(cacheName);
      RestResponse response = join(cacheClient.entries(true));
      Map<String, String> entries = entriesAsMap(response);

      // Obtain the negotiated media type of the entries
      String contentTypeHeader = response.header(ResponseHeader.VALUE_CONTENT_TYPE_HEADER.getValue());

      // Check entries are in the required format
      assertEquals(data.size(), entries.size());
      entries.forEach((key, value) -> assertEquals(value, data.get(key)));

      // Change an entry using the content type returned from the previous call to getEntries
      String aKey = data.keySet().iterator().next();
      String aValue = data.get(aKey);
      String changedValue = aValue.replace("value", "value-changed");
      writeEntry(aKey, changedValue, cacheName, MediaType.fromString(contentTypeHeader));

      // Check changed entry
      entries = entriesAsMap(join(cacheClient.entries(true)));

      assertEquals(changedValue, entries.get(aKey));
   }

   public void testStreamFromXMLCache() {
      Map<String, String> data = new HashMap<>();
      data.put("<id>1</id>", "<value>value1</value>");
      data.put("<id>2</id>", "<value>value2</value>");

      testStreamEntriesFromCache("xml", APPLICATION_XML, APPLICATION_XML, data);
   }

   public void testStreamFromTextPlainCache() {
      Map<String, String> data = new HashMap<>();
      data.put("key-1", "value-1");
      data.put("key-2", "value-2");

      testStreamEntriesFromCache("text", TEXT_PLAIN, TEXT_PLAIN, data);
   }

   public void testStreamFromJSONCache() {
      Map<String, String> data = new HashMap<>();
      data.put("1", "{\"value\":1}");
      data.put("2", "{\"value\":2}");

      testStreamEntriesFromCache("json", APPLICATION_JSON, APPLICATION_JSON, data);
   }

   public void testStreamFromDefaultCache() {
      Map<String, String> data = new HashMap<>();
      data.put("0x01", "0x010203");
      data.put("0x02", "0x020406");

      testStreamEntriesFromCache("noEncoding", null, APPLICATION_OCTET_STREAM.withEncoding("hex"), data);
   }

   private void createCache(String cacheName, MediaType mediaType) {
      RestCacheClient cacheClient = client.cache(cacheName);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      if (mediaType != null) builder.encoding().mediaType(mediaType.toString());
      String jsonConfig = cacheConfigToJson(cacheName, builder.build());
      RestEntity cacheConfig = RestEntity.create(APPLICATION_JSON, jsonConfig);
      RestResponse response = join(cacheClient.createWithConfiguration(cacheConfig));
      assertThat(response).isOk();
   }

   private void writeEntry(String key, String value, String cacheName, MediaType mediaType) {
      RestCacheClient cacheClient = client.cache(cacheName);
      RestResponse response;
      if (mediaType == null) {
         response = join(cacheClient.put(key, value));
      } else {
         response = join(cacheClient.put(key, mediaType.toString(), RestEntity.create(mediaType, value)));
      }
      assertThat(response).isOk();
   }

   private Map<String, String> entriesAsMap(RestResponse response) {
      assertThat(response).isOk();
      List<Json> entries = Json.read(response.body()).asJsonList();
      return entries.stream().collect(Collectors.toMap(j -> asString(j.at("key")), j -> asString(j.at("value"))));
   }

   @Test
   public void testStreamEntriesWithMetadata() {
      RestResponse response = join(client.cache("default").entries(-1, true));
      Collection<?> emptyEntries = Json.read(response.body()).asJsonList();
      assertEquals(0, emptyEntries.size());
      putTextEntryInCache("default", "key_0", "value_0");
      response = join(client.cache("default").entries(-1, true));
      Collection<?> singleSet = Json.read(response.body()).asJsonList();
      assertEquals(1, singleSet.size());
      for (int i = 0; i < 20; i++) {
         putTextEntryInCache("default", "key_" + i, "value_" + i);
      }
      response = join(client.cache("default").entries(-1, true));
      List<Json> jsons = Json.read(response.body()).asJsonList();
      assertEquals(20, jsons.size());

      response = join(client.cache("default").entries(3, true));
      jsons = Json.read(response.body()).asJsonList();
      assertEquals(3, jsons.size());
      Json first = jsons.get(0);
      String entry = first.toPrettyString();
      assertThat(entry).contains("\"key\" : \"key_");
      assertThat(entry).contains("\"value\" : \"value_");
      assertThat(entry).contains("\"timeToLiveSeconds\" : -1");
      assertThat(entry).contains("\"maxIdleTimeSeconds\" : -1");
      assertThat(entry).contains("\"created\" : -1");
      assertThat(entry).contains("\"lastUsed\" : -1");
      assertThat(entry).contains("\"expireTime\" : -1");
   }

   @Test
   public void testStreamEntriesWithMetadataAndExpirationTimesConvertedToSeconds() {
      RestEntity textValue = RestEntity.create(TEXT_PLAIN, "value1");
      join(client.cache("default").put("key1", TEXT_PLAIN_TYPE, textValue, 5000, 1000));
      RestResponse response = join(client.cache("default").entries(1, true));
      List<Json> jsons = Json.read(response.body()).asJsonList();

      assertEquals(1, jsons.size());
      Json first = jsons.get(0);
      String entry = first.toPrettyString();
      assertThat(entry).contains("\"key\" : \"key1");
      assertThat(entry).contains("\"value\" : \"value1");
      assertThat(entry).contains("\"timeToLiveSeconds\" : 5000");
      assertThat(entry).contains("\"maxIdleTimeSeconds\" : 1000");
   }

   @Test
   public void testProtobufMetadataManipulation() {
      String cache = PROTOBUF_METADATA_CACHE_NAME;
      putStringValueInCache(cache, "file1.proto", "message A{}");
      putStringValueInCache(cache, "file2.proto", "message B{}");
      putStringValueInCache(cache, "sample.proto", PROTO_SCHEMA);

      RestResponse response = join(client.schemas().names());
      String contentAsString = response.body();
      Collection<?> keys = Json.read(contentAsString).asJsonList();
      assertEquals(3, keys.size());
   }

   @Test
   public void testVersionMetadata() {
      Metadata metadata1 = new EmbeddedMetadata.Builder()
            .version(new NumericVersion(7)).build();
      Metadata metadata2 = new EmbeddedMetadata.Builder()
            .version(new SimpleClusteredVersion(3, 9)).build();

      Cache<String, String> embeddedCache = cacheManagers.get(0).getCache("simple-text");
      AdvancedCache<String, String> advancedCache = embeddedCache.getAdvancedCache();

      advancedCache.put("key-1", "value-1", metadata1);
      advancedCache.put("key-2", "value-2", metadata2);
      advancedCache.put("key-3", "value-3");

      RestCacheClient cacheClient = client.cache("simple-text");

      RestResponse response = join(cacheClient.entries(100, true));
      assertThat(response).isOk();

      String body = response.body();
      Assert.assertTrue(body.contains("key-1"));
      Assert.assertTrue(body.contains("key-2"));
      Assert.assertTrue(body.contains("key-3"));

      List<Json> returnedEntries = Json.read(body).asJsonList();
      Assert.assertEquals(3, returnedEntries.size());

      for (int i = 0; i < 3; i++) {
         Json entry = returnedEntries.get(0);
         String key = entry.at("key").asString();
         Json version = entry.at("version");
         Json topologyId = entry.at("topologyId");

         switch (key) {
            case "key-1":
               assertEquals(7L, version.asLong());
               assertNull(topologyId);
               break;
            case "key-2":
               assertEquals(3L, version.asLong());
               assertEquals(7, topologyId.asInteger());
               break;
            case "key-3":
               assertNull(version);
               assertNull(topologyId);
               break;
            default:
               fail("unexpected key: " + key);
         }
      }
   }

   @Test
   public void testRebalancingActions() {
      String cacheName = "default";
      assertRebalancingStatus(cacheName, true);

      RestCacheClient cacheClient = adminClient.cache(cacheName);
      RestResponse response = join(cacheClient.disableRebalancing());
      ResponseAssertion.assertThat(response).isOk();
      assertRebalancingStatus(cacheName, false);

      response = join(cacheClient.enableRebalancing());
      ResponseAssertion.assertThat(response).isOk();
      assertRebalancingStatus(cacheName, true);
   }

   private void assertRebalancingStatus(String cacheName, boolean enabled) {
      for (EmbeddedCacheManager cm : cacheManagers) {
         eventuallyEquals(enabled, () -> {
            try {
               return TestingUtil.extractGlobalComponent(cm, LocalTopologyManager.class).isCacheRebalancingEnabled(cacheName);
            } catch (Exception e) {
               fail("Unexpected exception", e);
               return !enabled;
            }
         });
      }
   }

   private String getCacheConfig(String accept, String name) {
      RestResponse response = join(adminClient.cache(name).configuration(accept));
      assertThat(response).isOk();
      return response.body();
   }

   @Test
   public void testConversionFromXML() {
      testConversionFromXML0("distributed-cache");
      testConversionFromXML0("distributed-cache-configuration");
   }

   private void testConversionFromXML0(String root) {
      RestRawClient rawClient = client.raw();

      String xml = String.format(
            """
                  <%s name="cacheName" mode="SYNC" configuration="parent">
                     <memory storage="HEAP" max-count="20"/>
                     <persistence>
                        <file-store/>
                     </persistence>
                  </%s>""", root, root
      );

      CompletionStage<RestResponse> response = rawClient.post("/rest/v2/caches?action=convert", Map.of(ACCEPT, APPLICATION_JSON_TYPE), RestEntity.create(APPLICATION_XML, xml));
      assertThat(response).isOk();
      checkJSON(response, "cacheName", root);

      response = rawClient.post("/rest/v2/caches?action=convert", Collections.singletonMap(ACCEPT, APPLICATION_YAML_TYPE), RestEntity.create(APPLICATION_XML, xml));
      assertThat(response).isOk();
      checkYaml(response, "cacheName", root);
   }

   @Test
   public void testConversionFromJSON() throws Exception {
      testConversionFromJSON0("distributed-cache");
      testConversionFromJSON0("distributed-cache-configuration");
   }

   private void testConversionFromJSON0(String root) throws Exception {
      RestRawClient rawClient = client.raw();

      String json = String.format("""
            {
               "%s": {
                  "configuration":"parent",
                  "mode":"SYNC",
                  "memory":{
                     "storage":"HEAP","max-count":"20"
                  },
                  "persistence": {
                     "file-store": {}
                  }
               }
            }
            """, root);

      CompletionStage<RestResponse> response = rawClient.post("/rest/v2/caches?action=convert", Collections.singletonMap(ACCEPT, APPLICATION_XML_TYPE), RestEntity.create(APPLICATION_JSON, json));
      assertThat(response).isOk();
      checkXML(response, root);

      response = rawClient.post("/rest/v2/caches?action=convert", Collections.singletonMap(ACCEPT, APPLICATION_YAML_TYPE), RestEntity.create(APPLICATION_JSON, json));
      assertThat(response).isOk();
      checkYaml(response, "", root);
   }

   @Test
   public void testConversionFromYAML() throws Exception {
      testConversionFromYAML0("distributedCache");
      testConversionFromYAML0("distributedCacheConfiguration");
   }

   private void testConversionFromYAML0(String root) throws Exception {
      RestRawClient rawClient = client.raw();

      String yaml = String.format("""
            %s:
              mode: 'SYNC'
              configuration: 'parent'
              memory:
                storage: 'HEAP'
                maxCount: 20
              persistence:
                fileStore: ~
            """, root);

      CompletionStage<RestResponse> response = rawClient.post("/rest/v2/caches?action=convert", Collections.singletonMap(ACCEPT, APPLICATION_XML_TYPE), RestEntity.create(APPLICATION_YAML, yaml));
      assertThat(response).isOk();
      checkXML(response, root);

      response = rawClient.post("/rest/v2/caches?action=convert", Collections.singletonMap(ACCEPT, APPLICATION_JSON_TYPE), RestEntity.create(APPLICATION_YAML, yaml));
      assertThat(response).isOk();
      checkJSON(response, "", root);
   }

   @Test
   public void testBrokenConfiguration() throws IOException {
      for (String name : Arrays.asList("broken.xml", "broken.yaml", "broken.json")) {
         CompletionStage<RestResponse> response = createCacheFromResource(name);
         String body = join(response).body();
         assertThat(body).contains("ISPN000327: Cannot find a parser for element 'error' in namespace '' at [");
      }
   }

   private CompletionStage<RestResponse> createCacheFromResource(String name) throws IOException {
      String cfg;
      try (InputStream is = CacheResourceV2Test.class.getResourceAsStream("/" + name)) {
         cfg = Files.readFile(is);
      }
      RestEntity entity = RestEntity.create(MediaType.fromExtension(name), cfg);
      RestCacheClient cache = client.cache(name);
      return cache.createWithConfiguration(entity);
   }

   private void checkJSON(CompletionStage<RestResponse> response, String name, String rootElement) {
      Json jsonNode = Json.read(join(response).body());
      if (!name.isBlank()) {
         jsonNode = jsonNode.at(name);
      }
      Json distCache = jsonNode.at(NamingStrategy.KEBAB_CASE.convert(rootElement));
      Json memory = distCache.at("memory");
      assertEquals("SYNC", distCache.at("mode").asString());
      assertEquals("parent", distCache.at("configuration").asString());
      assertEquals(20, memory.at("max-count").asInteger());
   }

   private void checkXML(CompletionStage<RestResponse> response, String rootElement) throws Exception {
      String xml = join(response).body();
      Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
      Element root = doc.getDocumentElement();
      assertEquals(NamingStrategy.KEBAB_CASE.convert(rootElement), root.getTagName());
      assertEquals("parent", root.getAttribute("configuration"));
      assertEquals("SYNC", root.getAttribute("mode"));
      NodeList children = root.getElementsByTagName("memory");
      assertEquals(1, children.getLength());
      Element memory = (Element) children.item(0);
      assertEquals("HEAP", memory.getAttribute("storage"));
      assertEquals("20", memory.getAttribute("max-count"));
   }

   private void checkYaml(CompletionStage<RestResponse> response, String name, String root) {
      String rootElement = NamingStrategy.CAMEL_CASE.convert(root);
      try (YamlConfigurationReader yaml = new YamlConfigurationReader(new StringReader(join(response).body()), new URLConfigurationResourceResolver(null), new Properties(), PropertyReplacer.DEFAULT, NamingStrategy.KEBAB_CASE)) {
         Map<String, Object> config = yaml.asMap();
         assertEquals("parent", getYamlProperty(config, name, rootElement, "configuration"));
         assertEquals("SYNC", getYamlProperty(config, name, rootElement, "mode"));
         assertEquals("HEAP", getYamlProperty(config, name, rootElement, "memory", "storage"));
         assertEquals("20", getYamlProperty(config, name, rootElement, "memory", "maxCount"));
      }
   }

   public static <T> T getYamlProperty(Map<String, Object> yaml, String... names) {
      for (int i = 0; i < names.length - 1; i++) {
         if (!names[i].isBlank()) {
            yaml = (Map<String, Object>) yaml.get(names[i]);
            if (yaml == null) {
               return null;
            }
         }
      }
      return (T) yaml.get(names[names.length - 1]);
   }

   @Test
   public void testCacheExists() {
      assertEquals(404, checkCache("nonexistent"));
      assertEquals(204, checkCache("default"));
      assertEquals(204, checkCache("indexedCache"));
   }

   @Test
   public void testCRUDWithProtobufPrimitives() throws Exception {
      RestCacheClient client = this.client.cache("proto");
      MediaType integerType = MediaType.APPLICATION_OBJECT.withClassType(Integer.class);

      // Insert a pair of Integers
      RestEntity value = RestEntity.create(integerType, "1");
      CompletionStage<RestResponse> response = client.put("1", integerType.toString(), value);
      assertThat(response).isOk();

      // Change the value to another Integer
      RestEntity anotherValue = RestEntity.create(integerType, "2");
      response = client.put("1", integerType.toString(), anotherValue);
      assertThat(response).isOk();

      // Read the changed value as an integer
      Map<String, String> headers = new HashMap<>();
      headers.put(KEY_CONTENT_TYPE_HEADER.getValue(), integerType.toString());
      headers.put(ACCEPT_HEADER.getValue(), integerType.toString());
      response = client.get("1", headers);
      assertThat(response).isOk();
      assertThat(response).hasReturnedText("2");

      // Read the changed value as protobuf
      headers = new HashMap<>();
      headers.put(KEY_CONTENT_TYPE_HEADER.getValue(), integerType.toString());
      headers.put(ACCEPT_HEADER.getValue(), MediaType.APPLICATION_PROTOSTREAM_TYPE);
      response = client.get("1", headers);
      assertThat(response).isOk();
      assertThat(response).hasReturnedBytes(new ProtoStreamMarshaller().objectToByteBuffer(2));
   }

   @Test
   public void indexMetamodel() {
      RestCacheClient cacheClient = adminClient.cache("indexedCache");
      join(cacheClient.clear());

      RestResponse response = join(cacheClient.indexMetamodel());
      Json indexMetamodel = Json.read(response.body());

      List<Json> indexes = indexMetamodel.asJsonList();
      assertThat(indexes).hasSize(2);

      Json entity = indexes.get(0);
      assertThat(entity.at("entity-name").asString()).isEqualTo("Entity");
      assertThat(entity.at("java-class").asString()).isEqualTo("[B");
      assertThat(entity.at("index-name").asString()).isEqualTo("Entity");

      Map<String, Json> valueFields = entity.at("value-fields").asJsonMap();
      assertThat(valueFields).containsKey("value");
      Json valueField = valueFields.get("value");
      assertThat(valueField.at("multi-valued").asBoolean()).isFalse();
      assertThat(valueField.at("multi-valued-in-root").asBoolean()).isFalse();
      assertThat(valueField.at("type").asString()).isEqualTo(Integer.class.getName());
      assertThat(valueField.at("projection-type").asString()).isEqualTo(Integer.class.getName());
      assertThat(valueField.at("argument-type").asString()).isEqualTo(Integer.class.getName());
      assertThat(valueField.at("searchable").asBoolean()).isTrue();
      assertThat(valueField.at("sortable").asBoolean()).isFalse();
      assertThat(valueField.at("projectable").asBoolean()).isFalse();
      assertThat(valueField.at("aggregable").asBoolean()).isFalse();
      assertThat(valueField.at("analyzer")).isNull();
      assertThat(valueField.at("normalizer")).isNull();

      Json another = indexes.get(1);
      assertThat(another.at("entity-name").asString()).isEqualTo("Another");
      assertThat(another.at("java-class").asString()).isEqualTo("[B");
      assertThat(another.at("index-name").asString()).isEqualTo("Another");
   }

   @Test
   public void testDeleteByQuery() {
      RestCacheClient cacheClient = adminClient.cache("indexedCache");
      join(cacheClient.clear());

      insertEntity(1, "Another", 11, "Eleven 1");
      insertEntity(2, "Another", 11, "Eleven 2");
      insertEntity(3, "Another", 11, "Eleven 3");
      insertEntity(4, "Another", 9, "Nine 1");
      insertEntity(5, "Another", 9, "Nine 2");

      RestResponse response = join(cacheClient.size());
      assertThat(response).hasReturnedText("5");

      response = join(cacheClient.deleteByQuery("DELETE FROM Another WHERE value > 10", true));
      assertThat(response).isOk();

      response = join(cacheClient.size());
      assertThat(response).hasReturnedText("2");

      // Test with POST
      RestRawClient rawClient = adminClient.raw();
      RestEntity restEntity = RestEntity.create("{\"query\": \"DELETE FROM Another WHERE value = 9\"}");
      response = join(rawClient.post("/rest/v2/caches/indexedCache?action=deleteByQuery", restEntity));
      assertThat(response).isOk();
      response = join(cacheClient.size());
      assertThat(response).hasReturnedText("0");

      response = join(cacheClient.deleteByQuery("FROM Another WHERE value = 9", true));
      assertThat(response).isBadRequest();
   }

   @Test
   public void testSearchStatistics() {
      RestCacheClient cacheClient = adminClient.cache("indexedCache");
      join(cacheClient.clear());

      // Clear all stats
      RestResponse response = join(cacheClient.clearSearchStats());
      assertThat(response).isOk();
      response = join(cacheClient.searchStats());
      Json statJson = Json.read(response.body());
      assertIndexStatsEmpty(statJson.at("index"));
      assertAllQueryStatsEmpty(statJson.at("query"));

      // Insert some data
      insertEntity(1, "Entity", 1, "One");
      insertEntity(11, "Entity", 11, "Eleven");
      insertEntity(21, "Entity", 21, "Twenty One");
      insertEntity(3, "Another", 3, "Three");
      insertEntity(33, "Another", 33, "Thirty Three");

      response = join(cacheClient.size());
      assertThat(response).hasReturnedText("5");

      response = join(cacheClient.searchStats());
      assertThat(response).isOk();

      // All stats should be zero in the absence of query
      statJson = Json.read(response.body());
      assertAllQueryStatsEmpty(statJson.at("query"));

      // Execute some indexed queries
      String indexedQuery = "FROM Entity WHERE value > 5";
      IntStream.range(0, 3).forEach(i -> {
         RestResponse response1 = join(cacheClient.query(indexedQuery));
         assertThat(response1).isOk();
         Json queryJson = Json.read(response1.body());
         assertEquals(2, queryJson.at("hit_count").asInteger());
         assertEquals(true, queryJson.at("hit_count_exact").asBoolean());
      });
      response = join(cacheClient.searchStats());
      statJson = Json.read(response.body());

      // Hybrid and non-indexed queries stats should be empty
      assertEquals(0, statJson.at("query").at("hybrid").at("count").asLong());
      assertEquals(0, statJson.at("query").at("non_indexed").at("count").asLong());

      Json queryStats = statJson.at("query");
      assertQueryStatEmpty(queryStats.at("hybrid"));
      assertQueryStatEmpty(queryStats.at("non_indexed"));

      // Indexed queries should be recorded
      assertEquals(3, statJson.at("query").at("indexed_local").at("count").asLong());
      assertTrue(statJson.at("query").at("indexed_local").at("average").asLong() > 0);
      assertTrue(statJson.at("query").at("indexed_local").at("max").asLong() > 0);

      assertEquals(3, statJson.at("query").at("indexed_distributed").at("count").asLong());
      assertTrue(statJson.at("query").at("indexed_distributed").at("average").asLong() > 0);
      assertTrue(statJson.at("query").at("indexed_distributed").at("max").asLong() > 0);

      // Execute a hybrid query
      String hybrid = "FROM Entity WHERE value > 5 AND description = 'One'";
      response = join(cacheClient.query(hybrid));
      Json queryJson = Json.read(response.body());
      assertEquals(2, queryJson.at("hit_count").asInteger());
      assertTrue(queryJson.at("hit_count_exact").asBoolean());
      response = join(cacheClient.searchStats());
      statJson = Json.read(response.body());

      // Hybrid queries should be recorded
      assertEquals(1, statJson.at("query").at("hybrid").at("count").asLong());
      assertTrue(statJson.at("query").at("hybrid").at("average").asLong() > 0);
      assertTrue(statJson.at("query").at("hybrid").at("max").asLong() > 0);

      // Check index stats
      response = join(cacheClient.searchStats());
      statJson = Json.read(response.body());
      assertEquals(3, statJson.at("index").at("types").at("Entity").at("count").asInteger());
      assertEquals(2, statJson.at("index").at("types").at("Another").at("count").asInteger());
      assertThat(statJson.at("index").at("types").at("Entity").at("size").asLong())
            .isGreaterThan(MIN_NON_EMPTY_INDEX_SIZE);
      assertThat(statJson.at("index").at("types").at("Another").at("size").asLong())
            .isGreaterThan(MIN_NON_EMPTY_INDEX_SIZE);
      assertFalse(statJson.at("index").at("reindexing").asBoolean());
   }

   @Test
   public void testIndexDataSyncInvalidSchema() {
      String notQuiteIndexed = """
            package schemas;
             /* @Indexed */
             message Entity {
                optional string name=1;
             }""";

      // Register schema
      RestResponse restResponse = join(client.schemas().put("schemas.proto", notQuiteIndexed));
      assertThat(restResponse).isOk();

      // Create the indexed cache
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable().storage(LOCAL_HEAP).addIndexedEntities("schemas.Entity");
      String cacheConfig = cacheConfigToJson("sync-data-index", builder.build());
      RestCacheClient cacheClient = client.cache("sync-data-index");
      RestEntity config = RestEntity.create(APPLICATION_JSON, cacheConfig);
      CompletionStage<RestResponse> response = cacheClient.createWithConfiguration(config);
      assertThat(response).isOk();

      // Write an entry, it should error
      String value = Json.object().set("_type", "schemas.Entity").set("name", "Jun").toString();
      RestEntity restEntity = RestEntity.create(APPLICATION_JSON, value);
      restResponse = join(cacheClient.put("key", restEntity));
      assertThat(restResponse).containsReturnedText("make sure at least one field has some indexing annotation");

      // Cache should not have any data
      response = cacheClient.size();
      assertThat(response).containsReturnedText("0");
   }

   @Test
   public void testLazySearchMapping() {
      String proto = """
             package future;
             /* @Indexed */
             message Entity {
                /* @Basic */
                optional string name=1;
             }
            """;

      String value = Json.object().set("_type", "future.Entity").set("name", "Kim").toString();
      RestEntity restEntity = RestEntity.create(APPLICATION_JSON, value);

      // Create a cache with a declared, not yet registered protobuf entity
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable().storage(LOCAL_HEAP).addIndexedEntities("future.Entity");
      String cacheConfig = cacheConfigToJson("index-lazy", builder.build());

      RestCacheClient cacheClient = client.cache("index-lazy");
      RestEntity config = RestEntity.create(APPLICATION_JSON, cacheConfig);

      CompletionStage<RestResponse> response = cacheClient.createWithConfiguration(config);
      assertThat(response).isOk();

      // Queries should return error
      RestResponse restResponse = join(cacheClient.query("From future.Entity"));
      assertThat(restResponse).containsReturnedText("Unknown type name : future.Entity");

      // Writes too
      restResponse = join(cacheClient.put("key", restEntity));
      assertThat(restResponse).containsReturnedText("Unknown type name : future.Entity");

      // Register the protobuf
      restResponse = join(client.schemas().put("future.proto", proto));
      assertThat(restResponse).isOk();

      // All operations should work
      restResponse = join(cacheClient.put("key", restEntity));
      assertThat(restResponse).isOk();

      restResponse = join(cacheClient.query("From future.Entity"));
      assertThat(restResponse).isOk();
      assertThat(restResponse).containsReturnedText("Kim");
   }

   @Test
   public void testCacheListener() throws InterruptedException, IOException {
      SSEListener sseListener = new SSEListener();
      Closeable listen = client.raw().listen("/rest/v2/caches/default?action=listen", Collections.singletonMap(ACCEPT, TEXT_PLAIN_TYPE), sseListener);
      assertTrue(sseListener.await(10, TimeUnit.SECONDS));
      putTextEntryInCache("default", "AKey", "AValue");
      sseListener.expectEvent("cache-entry-created", "AKey");
      removeTextEntryFromCache("default", "AKey");
      sseListener.expectEvent("cache-entry-removed", "AKey");
      listen.close();
   }

   @Test
   public void testConnectStoreValidation() {
      RestCacheClient cacheClient = client.cache("default");

      assertBadResponse(cacheClient, "true");
      assertBadResponse(cacheClient, "2");
      assertBadResponse(cacheClient, "[1,2,3]");
      assertBadResponse(cacheClient, "\"random text\"");

      assertBadResponse(cacheClient, "{\"jdbc-store\":{\"shared\":true}}");
      assertBadResponse(cacheClient, "{\"jdbc-store\":{\"shared\":true},\"remote-store\":{\"shared\":true}}");
   }

   @Test
   public void testSourceConnected() {
      RestCacheClient cacheClient = client.cache("default");
      RestResponse restResponse = join(cacheClient.sourceConnected());
      ResponseAssertion.assertThat(restResponse).isNotFound();
   }

   @Test
   public void testCacheAvailability() {
      RestCacheClient cacheClient = adminClient.cache("denyReadWritesCache");
      RestResponse restResponse = join(cacheClient.getAvailability());
      ResponseAssertion.assertThat(restResponse).isOk().containsReturnedText("AVAILABLE");

      restResponse = join(cacheClient.setAvailability("DEGRADED_MODE"));
      ResponseAssertion.assertThat(restResponse).isOk();

      eventuallyEquals("Availability status not updated!", "DEGRADED_MODE", () -> {
         RestResponse r = join(cacheClient.getAvailability());
         ResponseAssertion.assertThat(r).isOk();
         return r.body();
      });

      // Ensure that the endpoints can be utilised with internal caches
      RestCacheClient adminCacheClient = adminClient.cache(CONFIG_STATE_CACHE_NAME);
      restResponse = join(adminCacheClient.getAvailability());
      ResponseAssertion.assertThat(restResponse).isOk().containsReturnedText("AVAILABLE");

      // No-op in core as the cache uses the PreferAvailabilityStategy
      // Call to ensure that accessing internal cache doesn't throw an exception
      restResponse = join(adminCacheClient.setAvailability("DEGRADED_MODE"));
      ResponseAssertion.assertThat(restResponse).isOk();

      // The availability will always be AVAILABLE
      restResponse = join(adminCacheClient.getAvailability());
      ResponseAssertion.assertThat(restResponse).isOk().containsReturnedText("AVAILABLE");
   }

   @Test
   public void testComparison() {
      RestRawClient rawClient = client.raw();

      String xml = """
            <distributed-cache name="cacheName" mode="SYNC">
            <memory storage="HEAP" max-count="20"/>
            </distributed-cache>""";
      String json20 = "{\"distributed-cache\":{\"memory\":{\"storage\":\"HEAP\",\"max-count\":\"20\"}}}";
      String json30 = "{\"distributed-cache\":{\"memory\":{\"storage\":\"HEAP\",\"max-count\":\"30\"}}}";
      String jsonrepl = "{\"replicated-cache\":{\"memory\":{\"storage\":\"HEAP\",\"max-count\":\"30\"}}}";

      MultiPartRestEntity multiPart = RestEntity.multiPart();
      multiPart.addPart("one", xml);
      multiPart.addPart("two", json20);

      CompletionStage<RestResponse> response = rawClient.post("/rest/v2/caches?action=compare", multiPart);
      assertThat(response).isOk();

      multiPart = RestEntity.multiPart();
      multiPart.addPart("one", xml);
      multiPart.addPart("two", json30);

      response = rawClient.post("/rest/v2/caches?action=compare", multiPart);
      assertThat(response).isConflicted();

      response = rawClient.post("/rest/v2/caches?action=compare&ignoreMutable=true", multiPart);
      assertThat(response).isOk();

      multiPart = RestEntity.multiPart();
      multiPart.addPart("one", xml);
      multiPart.addPart("two", jsonrepl);

      response = rawClient.post("/rest/v2/caches?action=compare&ignoreMutable=true", Collections.emptyMap(), multiPart);
      assertThat(response).isConflicted();

      multiPart = RestEntity.multiPart();
      multiPart.addPart("one", "{\"local-cache\":{\"statistics\":true,\"encoding\":{\"key\": {\"media-type\":\"text/plain\"} ,\"value\":{\"media-type\":\"text/plain\"}},\"memory\":{\"max-count\":\"50\"}}}");
      multiPart.addPart("two", "{\"local-cache\":{\"statistics\":true,\"encoding\":{\"key\":{\"media-type\":\"application/x-protostream\"},\"value\":{\"media-type\":\"application/x-protostream\"}},\"memory\":{\"max-count\":\"50\"}}}");
      response = rawClient.post("/rest/v2/caches?action=compare&ignoreMutable=true", Collections.emptyMap(), multiPart);
      assertThat(response).isConflicted();
      assertEquals("ISPN000963: Invalid configuration in 'local-cache'\n" +
                  "    ISPN000961: Incompatible attribute 'local-cache.encoding.key.media-type' existing value='text/plain', new value='application/x-protostream'\n" +
                  "    ISPN000961: Incompatible attribute 'local-cache.encoding.value.media-type' existing value='text/plain', new value='application/x-protostream'",
            join(response).body());
   }

   @Test
   public void testForbiddenMethod() {
      RestRawClient rawClient = client.raw();

      CompletionStage<RestResponse> response = rawClient.execute("NOT-EXIST", "/rest/v2/caches");
      assertThat(response).isForbidden();
   }

   @Test
   public void testAccessibleCaches() {
      if (security) {
         RestResponse response = join(adminClient.cachesByRole("ADMIN"));
         ResponseAssertion.assertThat(response).isOk();
         String json = response.body();
         Json jsonNode = Json.read(json);
         assertThat(jsonNode.at("secured").asList()).containsExactlyInAnyOrder("secured-simple-text");
         assertThat(jsonNode.at("non-secured").asList()).containsExactlyInAnyOrder("default",
               "simple-text",
               "indexedCache",
               "proto",
               "denyReadWritesCache",
               "defaultcache");
         response = join(adminClient.cachesByRole("USER"));
         json = response.body();
         jsonNode = Json.read(json);
         assertThat(jsonNode.at("secured").asList()).isEmpty();
         assertThat(jsonNode.at("non-secured").asList()).containsExactlyInAnyOrder("default",
               "simple-text",
               "indexedCache",
               "proto",
               "denyReadWritesCache",
               "defaultcache");
         ResponseAssertion.assertThat(join(client.cachesByRole("ADMIN"))).isForbidden();
      } else {
         ResponseAssertion.assertThat(join(adminClient.cachesByRole("ADMIN"))).isNotFound();
         ResponseAssertion.assertThat(join(client.cachesByRole("ADMIN"))).isNotFound();
      }
   }

   @Test
   public void testCacheAliases() {
      String cacheJson = """
            { "distributed-cache" : { "statistics":true, "aliases": ["butch-cassidy"] } }
            """;
      RestCacheClient cacheClient = client.cache("robert-parker");

      RestEntity jsonEntity = RestEntity.create(APPLICATION_JSON, cacheJson);
      CompletionStage<RestResponse> response = cacheClient.createWithConfiguration(jsonEntity, VOLATILE);
      assertThat(response).isOk();
      assertThat(client.cache("butch-cassidy").exists()).isOk();
      response = client.cache("impostor").createWithConfiguration(jsonEntity, VOLATILE);
      assertThat(response).isBadRequest();
      assertThat(response).containsReturnedText("The alias 'butch-cassidy' is already being used by cache 'robert-parker'");
   }

   @Test
   public void testCacheAliasesSwitch() {
      RestCacheClient wuMing1 = client.cache("wu-ming-1");
      assertThat(wuMing1.createWithConfiguration(RestEntity.create(APPLICATION_JSON, """
            { "distributed-cache" : { "statistics":true, "aliases": ["wu-ming"] } }
            """), VOLATILE)).isOk();
      RestCacheClient wuMing2 = client.cache("wu-ming-2");
      assertThat(wuMing2.createWithConfiguration(RestEntity.create(APPLICATION_JSON, """
            { "distributed-cache" : { "statistics":true } }
            """), VOLATILE)).isOk();

      // Write different data in the two backing caches
      assertThat(wuMing1.put("key", "v1")).isOk();
      assertThat(wuMing2.put("key", "v2")).isOk();

      RestCacheClient wuMing = client.cache("wu-ming");
      assertThat(wuMing.get("key")).isOk().hasReturnedText("v1");
      // Flip the alias
      assertThat(adminClient.cache("wu-ming-2").assignAlias("wu-ming")).isOk();
      assertThat(wuMing.get("key")).isOk().hasReturnedText("v2");
   }

   @Test
   public void reinitializeNotExistentCache() {
      RestCacheClient restClient = adminClient.cache("it-does-not-exist");
      assertThat(restClient.markTopologyStable(false))
            .isNotFound()
            .hasReturnedText("\"Cache 'it-does-not-exist' does not exist\"");
   }

   public void testNonInitializedCacheNotListed() {
      String cacheName = "non-initialized-cache";
      for (EmbeddedCacheManager cm : cacheManagers) {
         if (security) {
            Security.doAs(ADMIN, () -> cm.defineConfiguration(cacheName, new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_SYNC).build()));
         } else {
            cm.defineConfiguration(cacheName, new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_SYNC).build());
         }
      }

      List<String> names = new ArrayList<>();
      try (RestResponse response = join(adminClient.caches())) {
         assertThat(response).isOk();
         List<String> caches = Json.read(response.body()).asJsonList()
               .stream().map(Json::asString).toList();
         assertThat(caches).doesNotContain(cacheName);
         names.addAll(caches);
      }

      try (RestResponse response = join(adminClient.detailedCacheList())) {
         assertThat(response).isOk();
         List<String> caches = Json.read(response.body()).asJsonList().stream()
               .map(j -> j.at("name").asString())
               .toList();
         assertThat(caches).doesNotContain(cacheName);
         assertThat(names).containsAll(caches);
      }
   }

   private void assertBadResponse(RestCacheClient client, String config) {
      RestResponse response = join(client.connectSource(RestEntity.create(APPLICATION_JSON, config)));
      ResponseAssertion.assertThat(response).isBadRequest();
      ResponseAssertion.assertThat(response).containsReturnedText("Invalid remote-store JSON description");
   }

   private void assertQueryStatEmpty(Json queryTypeStats) {
      assertEquals(0, queryTypeStats.at("count").asInteger());
      assertEquals(0, queryTypeStats.at("max").asInteger());
      assertEquals(0.0, queryTypeStats.at("average").asDouble());
      assertNull(queryTypeStats.at("slowest"));
   }

   private void assertAllQueryStatsEmpty(Json queryStats) {
      queryStats.asJsonMap().forEach((name, s) -> assertQueryStatEmpty(s));
   }

   private void assertIndexStatsEmpty(Json indexStats) {
      indexStats.at("types").asJsonMap().forEach((name, json) -> {
         assertEquals(0, json.at("count").asInteger());
         // TODO Restore this assertion when Infinispan forces a merge after clearing a cache.
         //   Currently the index size remains high after the cache was cleared,
         //   because Infinispan doesn't force a merge. We're left with segments
         //   where all documents have been marked for deletion.
         //   In real-world usage that's no big deal, since Lucene will automatically
         //   trigger a merge some time later, but for tests it means we can't require
         //   that indexes are small *just* after a clear.
//         assertThat(json.at("size").asLong()).isLessThan(MAX_EMPTY_INDEX_SIZE);
         assertThat(json.at("size").asLong()).isGreaterThanOrEqualTo(0L);
      });
   }

   private int checkCache(String name) {
      CompletionStage<RestResponse> response = client.cache(name).exists();
      return join(response).status();
   }

   private void registerSchema(String name, String schema) {
      CompletionStage<RestResponse> response = client.schemas().put(name, schema);
      assertThat(response).isOk().hasNoErrors();
   }

   private RestResponse insertEntity(int key, int value, String... flags) {
      String json = String.format("{\"_type\": \"Entity\",\"value\": %d}", value);
      RestEntity restEntity = RestEntity.create(APPLICATION_JSON, json);
      RestCacheClient cacheClient = client.cache("indexedCache");

      return join(cacheClient.put(String.valueOf(key), restEntity, flags));
   }


   private void insertEntity(int cacheKey, String type, int intValue, String stringValue) {
      Json json = Json.object().set("_type", type).set("value", intValue).set("description", stringValue);
      RestEntity restEntity = RestEntity.create(APPLICATION_JSON, json.toString());
      RestCacheClient cacheClient = client.cache("indexedCache");
      CompletionStage<RestResponse> response = cacheClient.put(String.valueOf(cacheKey), restEntity);
      assertThat(response).isOk();
   }

   private void assertIndexed(int value) {
      assertIndex(value, true);
   }

   private void assertNotIndexed(int value) {
      assertIndex(value, false);
   }

   private void assertIndex(int value, boolean present) {
      String query = "FROM Entity WHERE value = " + value;
      RestResponse response = join(client.cache("indexedCache").query(query));
      assertThat(response).isOk();
      assertEquals(present, response.body().contains(String.valueOf(value)));
   }
}
