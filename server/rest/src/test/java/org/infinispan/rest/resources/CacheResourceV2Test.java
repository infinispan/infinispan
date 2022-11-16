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
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_YAML_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;
import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.infinispan.commons.util.Util.getResourceAsString;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.context.Flag.SKIP_CACHE_LOAD;
import static org.infinispan.context.Flag.SKIP_INDEXING;
import static org.infinispan.globalstate.GlobalConfigurationManager.CONFIG_STATE_CACHE_NAME;
import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;
import static org.infinispan.rest.RequestHeader.ACCEPT_HEADER;
import static org.infinispan.rest.RequestHeader.KEY_CONTENT_TYPE_HEADER;
import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;
import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
import org.infinispan.Cache;
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
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.globalstate.GlobalConfigurationManager;
import org.infinispan.globalstate.ScopedState;
import org.infinispan.globalstate.impl.CacheState;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.rest.ResponseHeader;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.test.TestException;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.LocalTopologyManager;
import org.testng.annotations.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

@Test(groups = "functional", testName = "rest.CacheResourceV2Test")
public class CacheResourceV2Test extends AbstractRestResourceTest {

   // Wild guess: an empty index shouldn't be more than this many bytes
   private static final long MAX_EMPTY_INDEX_SIZE = 300L;
   // Wild guess: a non-empty index (populated with addData) should be more than this many bytes
   private static final long MIN_NON_EMPTY_INDEX_SIZE = 1000L;

   private static final String PERSISTENT_LOCATION = tmpDirectory(CacheResourceV2Test.class.getName());

   private static final String PROTO_SCHEMA =
         " /* @Indexed */                     \n" +
               " message Entity {                   \n" +
               "    /* @Field */                    \n" +
               "    required int32 value=1;         \n" +
               "    optional string description=2;  \n" +
               " }                                  \n" +
               " /* @Indexed */                     \n" +
               " message Another {                  \n" +
               "    /* @Field */                    \n" +
               "    required int32 value=1;         \n" +
               "    optional string description=2;  \n" +
               " }";

   @Override
   protected void defineCaches(EmbeddedCacheManager cm) {
      cm.defineConfiguration("default", getDefaultCacheBuilder().build());
      cm.defineConfiguration("proto", getProtoCacheBuilder().build());

      Cache<String, String> metadataCache = cm.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.putIfAbsent("sample.proto", PROTO_SCHEMA);
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      cm.defineConfiguration("indexedCache", getIndexedPersistedCache().build());
      cm.defineConfiguration("denyReadWritesCache", getDefaultCacheBuilder().clustering().partitionHandling().whenSplit(PartitionHandling.DENY_READ_WRITES).build());
   }

   public ConfigurationBuilder getProtoCacheBuilder() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);
      return builder;
   }

   @Override
   public Object[] factory() {
      return new Object[]{
            new CacheResourceV2Test().withSecurity(false).protocol(HTTP_11).ssl(false),
            new CacheResourceV2Test().withSecurity(true).protocol(HTTP_20).ssl(false),
            new CacheResourceV2Test().withSecurity(true).protocol(HTTP_11).ssl(true),
            new CacheResourceV2Test().withSecurity(true).protocol(HTTP_20).ssl(true),
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
      Json keyMediaType = encoding.at("key").at("media-type");
      Json valueMediaType = encoding.at("value").at("media-type");

      assertEquals(TEXT_PLAIN_TYPE, keyMediaType.asString());
      assertEquals(TEXT_PLAIN_TYPE, valueMediaType.asString());
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
      List<String> names = Json.read(namesResponse.getBody()).asJsonList().stream().map(Json::asString).collect(Collectors.toList());
      assertTrue(names.contains(name));

      CompletionStage<RestResponse> putResponse = cacheClient.post("key", "value");
      assertThat(putResponse).isOk();

      CompletionStage<RestResponse> getResponse = cacheClient.get("key");
      assertThat(getResponse).isOk();
      assertThat(getResponse).containsReturnedText("value");
   }

   @Test
   public void testCreateAndAlterCache() {
      String cacheConfig = "{\n" +
            "  \"distributed-cache\" : {\n" +
            "    \"mode\" : \"SYNC\",\n" +
            "    \"statistics\" : true,\n" +
            "    \"encoding\" : {\n" +
            "      \"key\" : {\n" +
            "        \"media-type\" : \"application/x-protostream\"\n" +
            "      },\n" +
            "      \"value\" : {\n" +
            "        \"media-type\" : \"application/x-protostream\"\n" +
            "      }\n" +
            "    },\n" +
            "    \"expiration\" : {\n" +
            "      \"lifespan\" : \"60000\"\n" +
            "    },\n" +
            "    \"memory\" : {\n" +
            "      \"max-count\" : \"1000\",\n" +
            "      \"when-full\" : \"REMOVE\"\n" +
            "    }\n" +
            "  }\n" +
            "}\n";
      String cacheConfigAlter = "{\n" +
            "  \"distributed-cache\" : {\n" +
            "    \"mode\" : \"SYNC\",\n" +
            "    \"statistics\" : true,\n" +
            "    \"encoding\" : {\n" +
            "      \"key\" : {\n" +
            "        \"media-type\" : \"application/x-protostream\"\n" +
            "      },\n" +
            "      \"value\" : {\n" +
            "        \"media-type\" : \"application/x-protostream\"\n" +
            "      }\n" +
            "    },\n" +
            "    \"expiration\" : {\n" +
            "      \"lifespan\" : \"30000\"\n" +
            "    },\n" +
            "    \"memory\" : {\n" +
            "      \"max-count\" : \"2000\",\n" +
            "      \"when-full\" : \"REMOVE\"\n" +
            "    }\n" +
            "  }\n" +
            "}\n";
      String cacheConfigConflict = "{\n" +
            "  \"distributed-cache\" : {\n" +
            "    \"mode\" : \"ASYNC\"\n" +
            "  }\n" +
            "}\n";

      RestCacheClient cacheClient = client.cache("mutable");
      CompletionStage<RestResponse> response = cacheClient.createWithConfiguration(RestEntity.create(APPLICATION_JSON, cacheConfig));
      assertThat(response).isOk();
      response = cacheClient.updateWithConfiguration(RestEntity.create(APPLICATION_JSON, cacheConfigAlter));
      assertThat(response).isOk();

      response = cacheClient.configuration();
      assertThat(response).isOk();
      String configFromServer = join(response).getBody();

      assertTrue(configFromServer.contains("\"expiration\":{\"lifespan\":\"30000\"}"));
      assertTrue(configFromServer.contains("\"memory\":{\"max-count\":\"2000\""));

      response = cacheClient.updateWithConfiguration(RestEntity.create(APPLICATION_JSON, cacheConfigConflict));
      assertThat(response).isBadRequest();
   }

   @Test
   public void testMutableAttributes() {
      String cacheName = "mutable-attributes";
      String json = "{\"local-cache\":{\"encoding\":{\"media-type\":\"text/plain\"}}}";
      RestCacheClient cacheClient = createCache(adminClient, json, cacheName);
      CompletionStage<RestResponse> response = cacheClient.configurationAttributes(true);
      assertThat(response).isOk();
      Json attributes = Json.read(join(response).getBody());
      assertEquals(10, attributes.asJsonMap().size());
      assertEquals("long", attributes.at("clustering.remote-timeout").at("type").asString());
      assertEquals(15000, attributes.at("clustering.remote-timeout").at("value").asLong());
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
      response = client.cache("cache1").configuration(mediaList);
      assertThat(response).isOk();
      String cache1Cfg = join(response).getBody();

      response = client.cache("cache2").configuration();
      assertThat(response).isOk();
      String cache2Cfg = join(response).getBody();

      assertEquals(cache1Cfg, cache2Cfg.replace("cache2", "cache1"));

      response = client.cache("cache1").configuration("application/xml");
      assertThat(response).isOk();
      String cache1Xml = join(response).getBody();

      ParserRegistry registry = new ParserRegistry();
      Configuration xmlConfig = registry.parse(cache1Xml).getCurrentConfigurationBuilder().build();
      assertEquals(1200000, xmlConfig.clustering().l1().lifespan());
      assertEquals(60500, xmlConfig.clustering().stateTransfer().timeout());

      response = client.cache("cache1").configuration("application/xml; q=0.9");
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

      Json jsonNode = Json.read(join(response).getBody());
      assertEquals(jsonNode.at("current_number_of_entries").asInteger(), 2);
      assertEquals(jsonNode.at("stores").asInteger(), 2);

      response = cacheClient.clear();
      assertThat(response).isOk();
      response = cacheClient.stats();
      assertThat(response).isOk().hasJson().hasProperty("current_number_of_entries").is(0);
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

      Json jsonNode = Json.read(join(response).getBody());
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
      jsonNode = Json.read(join(response).getBody());

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
            Json jsonNode = Json.read(restResponse.getBody());
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
         putInCache("default", i, "" + i, APPLICATION_JSON_TYPE);
      }

      CompletionStage<RestResponse> response = client.cache("default").size();

      assertThat(response).isOk();
      assertThat(response).containsReturnedText("100");
   }

   @Test
   public void testCacheFullDetail() {
      RestResponse response = join(client.cache("default").details());
      Json document = Json.read(response.getBody());
      assertThat(response).isOk();
      assertThat(document.at("stats")).isNotNull();
      assertThat(document.at("size")).isNotNull();
      assertThat(document.at("configuration")).isNotNull();
      assertThat(document.at("rehash_in_progress")).isNotNull();
      assertThat(document.at("persistent")).isNotNull();
      assertThat(document.at("bounded")).isNotNull();
      assertThat(document.at("indexed")).isNotNull();
      assertThat(document.at("has_remote_backup")).isNotNull();
      assertThat(document.at("secured")).isNotNull();
      assertThat(document.at("indexing_in_progress")).isNotNull();
      assertThat(document.at("queryable")).isNotNull();
      assertThat(document.at("rebalancing_enabled")).isNotNull();
      assertThat(document.at("key_storage").asString()).isEqualTo("application/unknown");
      assertThat(document.at("value_storage").asString()).isEqualTo("application/unknown");

      response = join(client.cache("proto").details());
      document = Json.read(response.getBody());
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
      String invalidConfig = "<infinispan>\n" +
            " <cache-container>\n" +
            "   <replicated-cache name=\"books\">\n" +
            "     <encoding media-type=\"application/x-java-object\"/>\n" +
            "     <indexing>\n" +
            "       <indexed-entities>\n" +
            "         <indexed-entity>Dummy</indexed-entity>\n" +
            "        </indexed-entities>\n" +
            "     </indexing>\n" +
            "   </replicated-cache>\n" +
            " </cache-container>\n" +
            "</infinispan>";

      CompletionStage<RestResponse> response = client.cache("CACHE").createWithConfiguration(RestEntity.create(APPLICATION_XML, invalidConfig));
      assertThat(response).isBadRequest().hasReturnedText("Unable to instantiate 'Dummy'");

      response = client.cache("CACHE").exists();
      assertThat(response).isOk();

      CompletionStage<RestResponse> healthResponse = client.cacheManager("default").health();
      assertThat(healthResponse).isOk().containsReturnedText("{\"status\":\"FAILED\",\"cache_name\":\"CACHE\"}");

      // The only way to recover from a broken cache is to delete it
      response = client.cache("CACHE").delete();
      assertThat(response).isOk();

      response = client.cache("CACHE").exists();
      assertThat(response).isNotFound();
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

      return Json.read(response.getBody());
   }

   @Test
   public void testCacheNames() {
      CompletionStage<RestResponse> response = client.caches();

      assertThat(response).isOk();

      Json jsonNode = Json.read(join(response).getBody());
      Set<String> cacheNames = cacheManagers.get(0).getCacheNames();
      int size = jsonNode.asList().size();
      assertEquals(cacheNames.size(), size);
      for (int i = 0; i < size; i++) {
         assertTrue(cacheNames.contains(jsonNode.at(i).asString()));
      }
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

      Json json = Json.read(response.getBody());
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
   public void testGetAllKeys() {
      RestResponse response = join(client.cache("default").keys());
      Collection<?> emptyKeys = Json.read(response.getBody()).asJsonList();
      assertEquals(0, emptyKeys.size());

      putTextEntryInCache("default", "1", "value");
      response = join(client.cache("default").keys());
      Collection<?> singleSet = Json.read(response.getBody()).asJsonList();
      assertEquals(1, singleSet.size());

      int entries = 10;
      for (int i = 0; i < entries; i++) {
         putTextEntryInCache("default", String.valueOf(i), "value");
      }
      response = join(client.cache("default").keys());
      Set<?> keys = Json.read(response.getBody()).asJsonList().stream().map(Json::asInteger).collect(Collectors.toSet());
      assertEquals(entries, keys.size());
      assertTrue(IntStream.range(0, entries).allMatch(keys::contains));

      response = join(client.cache("default").keys(5));
      Set<?> keysLimited = Json.read(response.getBody()).asJsonList().stream().map(Json::asInteger).collect(Collectors.toSet());
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
      Collection<?> emptyKeys = Json.read(response.getBody()).asJsonList();
      assertEquals(0, emptyKeys.size());

      // Test key with escape.
      putInCache(cacheName, "{\"text\": \"I'm right \\\\\"here\\\\\".\"}", APPLICATION_JSON_TYPE, value, APPLICATION_XML);
      response = join(client.cache(cacheName).keys());
      Collection<?> singleSet = Json.read(response.getBody()).asJsonList();
      assertEquals(1, singleSet.size());
      join(client.cache(cacheName).clear());

      int entries = 10;
      for (int i = 0; i < entries; i++) {
         putInCache(cacheName, String.format("{\"v\": %d}", i), APPLICATION_JSON_TYPE, value, APPLICATION_XML);
      }
      response = join(client.cache(cacheName).keys());
      List<Json> keys = Json.read(response.getBody()).asJsonList();
      assertEquals(entries, keys.size());

      response = join(client.cache(cacheName).keys(5));
      List<?> keysLimited = Json.read(response.getBody()).asJsonList();
      assertEquals(5, keysLimited.size());
   }

   private void putInCache(String cacheName, String key, String keyType, String value, MediaType valueType) {
      CompletionStage<RestResponse> r = client.cache(cacheName).put(key, keyType, RestEntity.create(valueType, value));
      ResponseAssertion.assertThat(r).isOk();
   }

   @Test
   public void testStreamEntries() {
      RestResponse response = join(client.cache("default").entries());
      Collection<?> emptyEntries = Json.read(response.getBody()).asJsonList();
      assertEquals(0, emptyEntries.size());
      putTextEntryInCache("default", "key_0", "value_0");
      response = join(client.cache("default").entries());
      Collection<?> singleSet = Json.read(response.getBody()).asJsonList();
      assertEquals(1, singleSet.size());
      for (int i = 0; i < 20; i++) {
         putTextEntryInCache("default", "key_" + i, "value_" + i);
      }
      response = join(client.cache("default").entries());
      List<Json> jsons = Json.read(response.getBody()).asJsonList();
      assertEquals(20, jsons.size());

      response = join(client.cache("default").entries(3));
      jsons = Json.read(response.getBody()).asJsonList();
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
      Collection<?> emptyEntries = Json.read(response.getBody()).asJsonList();
      assertEquals(0, emptyEntries.size());
      insertEntity(3, "Another", 3, "Three");
      response = join(client.cache("indexedCache").entries(-1, true));

      if (response.getStatus() != 200) {
         Assertions.fail(response.getBody());
      }
      List<Json> jsons = Json.read(response.getBody()).asJsonList();
      assertThat(jsons).hasSize(1);
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
      String contentTypeHeader = response.getHeader(ResponseHeader.VALUE_CONTENT_TYPE_HEADER.getValue());

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
      List<Json> entries = Json.read(response.getBody()).asJsonList();
      return entries.stream().collect(Collectors.toMap(j -> asString(j.at("key")), j -> asString(j.at("value"))));
   }

   @Test
   public void testStreamEntriesWithMetadata() {
      RestResponse response = join(client.cache("default").entries(-1, true));
      Collection<?> emptyEntries = Json.read(response.getBody()).asJsonList();
      assertEquals(0, emptyEntries.size());
      putTextEntryInCache("default", "key_0", "value_0");
      response = join(client.cache("default").entries(-1, true));
      Collection<?> singleSet = Json.read(response.getBody()).asJsonList();
      assertEquals(1, singleSet.size());
      for (int i = 0; i < 20; i++) {
         putTextEntryInCache("default", "key_" + i, "value_" + i);
      }
      response = join(client.cache("default").entries(-1, true));
      List<Json> jsons = Json.read(response.getBody()).asJsonList();
      assertEquals(20, jsons.size());

      response = join(client.cache("default").entries(3, true));
      jsons = Json.read(response.getBody()).asJsonList();
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
      join(client.cache("default").put("key1", TEXT_PLAIN_TYPE, textValue, 1000, 5000));
      RestResponse response = join(client.cache("default").entries(1, true));
      List<Json> jsons = Json.read(response.getBody()).asJsonList();

      assertEquals(1, jsons.size());
      Json first = jsons.get(0);
      String entry = first.toPrettyString();
      assertThat(entry).contains("\"key\" : \"key1");
      assertThat(entry).contains("\"value\" : \"value1");
      assertThat(entry).contains("\"timeToLiveSeconds\" : 1000");
      assertThat(entry).contains("\"maxIdleTimeSeconds\" : 5000");
   }

   @Test
   public void testProtobufMetadataManipulation() {
      // Special role {@link ProtobufMetadataManager#SCHEMA_MANAGER_ROLE} is needed for authz. Subject USER has it
      String cache = PROTOBUF_METADATA_CACHE_NAME;
      putStringValueInCache(cache, "file1.proto", "message A{}");
      putStringValueInCache(cache, "file2.proto", "message B{}");

      RestResponse response = join(client.cache(PROTOBUF_METADATA_CACHE_NAME).keys());
      String contentAsString = response.getBody();
      Collection<?> keys = Json.read(contentAsString).asJsonList();
      assertEquals(2, keys.size());
   }

   @Test
   public void testGetProtoCacheConfig() {
      testGetProtoCacheConfig(APPLICATION_XML_TYPE);
      testGetProtoCacheConfig(APPLICATION_JSON_TYPE);
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

   private void testGetProtoCacheConfig(String accept) {
      getCacheConfig(accept, PROTOBUF_METADATA_CACHE_NAME);
   }

   private String getCacheConfig(String accept, String name) {
      RestResponse response = join(client.cache(name).configuration(accept));
      assertThat(response).isOk();
      return response.getBody();
   }

   @Test
   public void testConversionFromXML() {
      RestRawClient rawClient = client.raw();

      String xml = "<infinispan>\n" +
            "    <cache-container>\n" +
            "        <distributed-cache name=\"cacheName\" mode=\"SYNC\">\n" +
            "            <memory>\n" +
            "                <object size=\"20\"/>\n" +
            "            </memory>\n" +
            "        </distributed-cache>\n" +
            "    </cache-container>\n" +
            "</infinispan>";

      CompletionStage<RestResponse> response = rawClient.post("/rest/v2/caches?action=convert", Collections.singletonMap("Accept", APPLICATION_JSON_TYPE), xml, APPLICATION_XML_TYPE);
      assertThat(response).isOk();
      checkJSON(response, "cacheName");

      response = rawClient.post("/rest/v2/caches?action=convert", Collections.singletonMap("Accept", APPLICATION_YAML_TYPE), xml, APPLICATION_XML_TYPE);
      assertThat(response).isOk();
      checkYaml(response, "cacheName");
   }

   @Test
   public void testConversionFromJSON() throws Exception {
      RestRawClient rawClient = client.raw();

      String json = "{\"distributed-cache\":{\"mode\":\"SYNC\",\"memory\":{\"storage\":\"OBJECT\",\"max-count\":\"20\"}}}";

      CompletionStage<RestResponse> response = rawClient.post("/rest/v2/caches?action=convert", Collections.singletonMap("Accept", APPLICATION_XML_TYPE), json, APPLICATION_JSON_TYPE);
      assertThat(response).isOk();
      checkXML(response);

      response = rawClient.post("/rest/v2/caches?action=convert", Collections.singletonMap("Accept", APPLICATION_YAML_TYPE), json, APPLICATION_JSON_TYPE);
      assertThat(response).isOk();
      checkYaml(response, "");
   }

   @Test
   public void testConversionFromYAML() throws Exception {
      RestRawClient rawClient = client.raw();

      String yaml = "distributedCache:\n" +
            "  mode: 'SYNC'\n" +
            "  memory:\n" +
            "    storage: 'OBJECT'\n" +
            "    maxCount: 20";

      CompletionStage<RestResponse> response = rawClient.post("/rest/v2/caches?action=convert", Collections.singletonMap("Accept", APPLICATION_XML_TYPE), yaml, APPLICATION_YAML_TYPE);
      assertThat(response).isOk();
      checkXML(response);

      response = rawClient.post("/rest/v2/caches?action=convert", Collections.singletonMap("Accept", APPLICATION_JSON_TYPE), yaml, APPLICATION_YAML_TYPE);
      assertThat(response).isOk();
      checkJSON(response, "");
   }

   private void checkJSON(CompletionStage<RestResponse> response, String name) {
      Json jsonNode = Json.read(join(response).getBody());
      if (!name.isBlank()) {
         jsonNode = jsonNode.at(name);
      }
      Json distCache = jsonNode.at("distributed-cache");
      Json memory = distCache.at("memory");
      assertEquals("SYNC", distCache.at("mode").asString());
      assertEquals(20, memory.at("max-count").asInteger());
   }

   private void checkXML(CompletionStage<RestResponse> response) throws Exception {
      String xml = join(response).getBody();
      Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
      Element root = doc.getDocumentElement();
      assertEquals("distributed-cache", root.getTagName());
      assertEquals("SYNC", root.getAttribute("mode"));
      NodeList children = root.getElementsByTagName("memory");
      assertEquals(1, children.getLength());
      Element memory = (Element) children.item(0);
      assertEquals("OBJECT", memory.getAttribute("storage"));
      assertEquals("20", memory.getAttribute("max-count"));
   }

   private void checkYaml(CompletionStage<RestResponse> response, String name) {
      try (YamlConfigurationReader yaml = new YamlConfigurationReader(new StringReader(join(response).getBody()), new URLConfigurationResourceResolver(null), new Properties(), PropertyReplacer.DEFAULT, NamingStrategy.KEBAB_CASE)) {
         Map<String, Object> config = yaml.asMap();
         assertEquals("SYNC", getYamlProperty(config, name, "distributedCache", "mode"));
         assertEquals("OBJECT", getYamlProperty(config, name, "distributedCache", "memory", "storage"));
         assertEquals("20", getYamlProperty(config, name, "distributedCache", "memory", "maxCount"));
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
      return (T) yaml.get(names[names.length -1 ]);
   }

   @Test
   public void testCacheExists() {
      assertEquals(404, checkCache("nonexistent"));
      assertEquals(204, checkCache("invalid"));
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
   public void testSearchStatistics() {
      RestCacheClient cacheClient = adminClient.cache("indexedCache");
      join(cacheClient.clear());

      // Clear all stats
      RestResponse response = join(cacheClient.clearSearchStats());
      assertThat(response).isOk();
      response = join(cacheClient.searchStats());
      Json statJson = Json.read(response.getBody());
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
      statJson = Json.read(response.getBody());
      assertAllQueryStatsEmpty(statJson.at("query"));

      // Execute some indexed queries
      String indexedQuery = "FROM Entity WHERE value > 5";
      IntStream.range(0, 3).forEach(i -> {
         RestResponse response1 = join(cacheClient.query(indexedQuery));
         assertThat(response1).isOk();
         Json queryJson = Json.read(response1.getBody());
         assertEquals(2, queryJson.at("total_results").asInteger());
      });
      response = join(cacheClient.searchStats());
      statJson = Json.read(response.getBody());

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
      Json queryJson = Json.read(response.getBody());
      assertEquals(0, queryJson.at("total_results").asInteger());
      response = join(cacheClient.searchStats());
      statJson = Json.read(response.getBody());

      // Hybrid queries should be recorded
      assertEquals(1, statJson.at("query").at("hybrid").at("count").asLong());
      assertTrue(statJson.at("query").at("hybrid").at("average").asLong() > 0);
      assertTrue(statJson.at("query").at("hybrid").at("max").asLong() > 0);

      // Check index stats
      response = join(cacheClient.searchStats());
      statJson = Json.read(response.getBody());
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
      String notQuiteIndexed = "package schemas;\n" +
            " /* @Indexed */\n" +
            " message Entity {\n" +
            "    optional string name=1;\n" +
            " }";

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
      assertThat(restResponse).containsReturnedText("make sure at least one field has the @Field annotation");

      // Cache should not have any data
      response = cacheClient.size();
      assertThat(response).containsReturnedText("0");
   }

   @Test
   public void testLazySearchMapping() {
      String proto = " package future;\n" +
            " /* @Indexed */\n" +
            " message Entity {\n" +
            "    /* @Field */\n" +
            "    optional string name=1;\n" +
            " }";

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
      Closeable listen = client.raw().listen("/rest/v2/caches/default?action=listen", Collections.singletonMap("Accept", "text/plain"), sseListener);
      assertTrue(sseListener.openLatch.await(10, TimeUnit.SECONDS));
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
         return r.getBody();
      });

      // Ensure that the endpoints can be utilised with internal caches
      RestCacheClient adminCacheClient = adminClient.cache(GlobalConfigurationManager.CONFIG_STATE_CACHE_NAME);
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
      return join(response).getStatus();
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
      assertEquals(present, response.getBody().contains(String.valueOf(value)));
   }
}
