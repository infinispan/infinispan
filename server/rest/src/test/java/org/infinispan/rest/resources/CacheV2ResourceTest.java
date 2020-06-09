package org.infinispan.rest.resources;

import static org.eclipse.jetty.http.HttpHeader.ACCEPT;
import static org.eclipse.jetty.http.HttpHeader.CONTENT_TYPE;
import static org.eclipse.jetty.http.HttpMethod.GET;
import static org.eclipse.jetty.http.HttpMethod.HEAD;
import static org.eclipse.jetty.http.HttpMethod.POST;
import static org.infinispan.commons.api.CacheContainerAdmin.AdminFlag.VOLATILE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;
import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.infinispan.commons.util.Util.getResourceAsString;
import static org.infinispan.context.Flag.SKIP_CACHE_LOAD;
import static org.infinispan.context.Flag.SKIP_INDEXING;
import static org.infinispan.globalstate.GlobalConfigurationManager.CONFIG_STATE_CACHE_NAME;
import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.net.URLEncoder;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.infinispan.Cache;
import org.infinispan.commons.configuration.JsonWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.globalstate.ScopedState;
import org.infinispan.globalstate.impl.CacheState;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Test(groups = "functional", testName = "rest.CacheV2ResourceTest")
public class CacheV2ResourceTest extends AbstractRestResourceTest {

   private static final String PERSISTENT_LOCATION = tmpDirectory(CacheV2ResourceTest.class.getName());
   private final ObjectMapper objectMapper = new ObjectMapper();

   @Override
   protected void defineCaches(EmbeddedCacheManager cm) {
      cm.defineConfiguration("default", getDefaultCacheBuilder().build());
      cm.defineConfiguration("indexedCache", getIndexedPersistedCache().build());
   }

   @Override
   public Object[] factory() {
      return new Object[]{
            new CacheV2ResourceTest().withSecurity(true),
            new CacheV2ResourceTest().withSecurity(false)
      };
   }

   private ConfigurationBuilder getIndexedPersistedCache() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.indexing().enable()
            .addProperty("default.directory_provider", "local-heap")
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
            .persistentLocation(Paths.get(PERSISTENT_LOCATION, Integer.toString(id)).toString());
      return config;
   }

   @Test
   public void testCacheV2KeyOps() throws Exception {
      String urlWithoutCM = String.format("http://localhost:%d/rest/v2/caches/default", restServer().getPort());

      ContentResponse response = client.newRequest(urlWithoutCM + "/key").method(HttpMethod.POST).content(new StringContentProvider("value")).send();
      ResponseAssertion.assertThat(response).isOk();

      response = client.newRequest(urlWithoutCM + "/key").method(HttpMethod.POST).content(new StringContentProvider("value")).send();
      ResponseAssertion.assertThat(response).isConflicted();

      response = client.newRequest(urlWithoutCM + "/key").method(HttpMethod.PUT).content(new StringContentProvider("value-new")).send();
      ResponseAssertion.assertThat(response).isOk();

      response = client.newRequest(urlWithoutCM + "/key").method(HttpMethod.GET).send();
      ResponseAssertion.assertThat(response).hasReturnedText("value-new");

      response = client.newRequest(urlWithoutCM + "/key").method(HEAD).send();
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasNoContent();

      response = client.newRequest(urlWithoutCM + "/key").method(HttpMethod.DELETE).send();
      ResponseAssertion.assertThat(response).isOk();

      response = client.newRequest(urlWithoutCM + "/key").method(HttpMethod.GET).send();
      ResponseAssertion.assertThat(response).isNotFound();
   }

   @Test
   public void testCreateCacheEncodedName() throws Exception {
      testCreateAndUseCache("a/");
      testCreateAndUseCache("a/b/c");
      testCreateAndUseCache("a-b-c");
      testCreateAndUseCache("áb\\ćé/+-$");
      testCreateAndUseCache("org.infinispan.cache");
      testCreateAndUseCache("a%25bc");
   }

   @Test
   public void testCreateCacheEncoding() throws Exception {
      String cacheName = "encoding-test";
      String json = "{\"local-cache\":{\"encoding\":{\"media-type\":\"text/plain\"}}}";

      createCache(json, cacheName);
      String cacheConfig = getCacheConfig(APPLICATION_JSON_TYPE, cacheName);

      JsonNode encoding = objectMapper.readTree(cacheConfig).get("local-cache").get("encoding");
      JsonNode keyMediaType = encoding.get("key").get("media-type");
      JsonNode valueMediaType = encoding.get("value").get("media-type");

      assertEquals(TEXT_PLAIN_TYPE, keyMediaType.asText());
      assertEquals(TEXT_PLAIN_TYPE, valueMediaType.asText());
   }

   private void testCreateAndUseCache(String name) throws Exception {
      String baseURL = String.format("http://localhost:%d/rest/v2/caches/", restServer().getPort());
      String cacheName = URLEncoder.encode(name, "UTF-8");
      String url = baseURL + cacheName;
      String cacheConfig = "{\"distributed-cache\":{\"mode\":\"SYNC\"}}";

      ContentResponse response = client.newRequest(url)
            .method(HttpMethod.POST)
            .header("Content-type", APPLICATION_JSON_TYPE)
            .content(new StringContentProvider(cacheConfig))
            .send();

      ResponseAssertion.assertThat(response).isOk();

      ContentResponse sizeResponse = client.newRequest(url + "?action=size").send();
      ResponseAssertion.assertThat(sizeResponse).isOk();
      ResponseAssertion.assertThat(sizeResponse).containsReturnedText("0");

      ContentResponse namesResponse = client.newRequest(baseURL).send();
      ResponseAssertion.assertThat(namesResponse).isOk();
      List<String> names = Arrays.asList(objectMapper.readValue(namesResponse.getContentAsString(), String[].class));
      assertTrue(names.contains(name));

      ContentResponse putResponse = client.newRequest(url + "/key")
            .method(HttpMethod.POST)
            .content(new StringContentProvider("value"))
            .send();
      ResponseAssertion.assertThat(putResponse).isOk();

      ContentResponse getResponse = client.newRequest(url + "/key").send();
      ResponseAssertion.assertThat(getResponse).isOk();
      ResponseAssertion.assertThat(getResponse).containsReturnedText("value");
   }

   @Test
   public void testCacheV2LifeCycle() throws Exception {
      String url = String.format("http://localhost:%d/rest/v2/caches/", restServer().getPort());

      String xml = getResourceAsString("cache.xml", getClass().getClassLoader());
      String json = getResourceAsString("cache.json", getClass().getClassLoader());

      ContentResponse response = client.newRequest(url + "cache1").header("Content-type", APPLICATION_XML_TYPE)
            .method(HttpMethod.POST).header("flags", "VOLATILE").content(new StringContentProvider(xml)).send();
      ResponseAssertion.assertThat(response).isOk();
      assertPersistence("cache1", false);

      response = client.newRequest(url + "cache2").header("Content-type", APPLICATION_JSON_TYPE)
            .method(HttpMethod.POST).content(new StringContentProvider(json)).send();
      ResponseAssertion.assertThat(response).isOk();
      assertPersistence("cache2", true);

      String mediaList = "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";
      response = client.newRequest(url + "cache1?action=config").method(GET).header("Accept", mediaList).send();
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).bodyNotEmpty();
      String cache1Cfg = response.getContentAsString();

      response = client.newRequest(url + "cache2?action=config").method(GET).send();
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).bodyNotEmpty();
      String cache2Cfg = response.getContentAsString();

      assertEquals(cache1Cfg, cache2Cfg);
   }

   @Test
   public void testCreateDeleteCache() throws Exception {
      String url = String.format("http://localhost:%d/rest/v2/caches/", restServer().getPort());

      String xml = getResourceAsString("cache.xml", getClass().getClassLoader());

      ContentResponse response = client.newRequest(url + "cacheCRUD").header("Content-type", APPLICATION_XML_TYPE)
            .method(HttpMethod.POST).header("flags", "VOLATILE").content(new StringContentProvider(xml)).send();
      ResponseAssertion.assertThat(response).isOk();

      response = client.newRequest(url + "cacheCRUD?action=stats").method(GET).send();
      ResponseAssertion.assertThat(response).isOk();

      response = client.newRequest(url + "cacheCRUD").method(HttpMethod.DELETE).send();
      ResponseAssertion.assertThat(response).isOk();

      response = client.newRequest(url + "cacheCRUD?action=stats").method(GET).send();
      ResponseAssertion.assertThat(response).isNotFound();
   }

   private void assertPersistence(String name, boolean persisted) {
      EmbeddedCacheManager cm = cacheManagers.iterator().next();
      Cache<ScopedState, CacheState> configCache = cm.getCache(CONFIG_STATE_CACHE_NAME);
      assertEquals(persisted, configCache.entrySet()
            .stream().anyMatch(e -> e.getKey().getName().equals(name) && !e.getValue().getFlags().contains(VOLATILE)));
   }

   @Test
   public void testCacheV2Stats() throws Exception {
      String cacheJson = "{ \"distributed-cache\" : { \"statistics\":true } }";
      String cacheURL = String.format("http://localhost:%d/rest/v2/caches/statCache", restServer().getPort());

      String url = String.format(cacheURL, restServer().getPort());
      ContentResponse response = client.newRequest(url)
            .method(HttpMethod.POST)
            .header(CONTENT_TYPE, APPLICATION_JSON_TYPE)
            .header("flags", "VOLATILE")
            .content(new StringContentProvider(cacheJson))
            .send();
      ResponseAssertion.assertThat(response).isOk();

      putStringValueInCache("statCache", "key1", "data");
      putStringValueInCache("statCache", "key2", "data");

      response = client.newRequest(cacheURL + "?action=stats").send();
      ResponseAssertion.assertThat(response).isOk();

      JsonNode jsonNode = objectMapper.readTree(response.getContent());
      assertEquals(jsonNode.get("current_number_of_entries").asInt(), 2);
      assertEquals(jsonNode.get("stores").asInt(), 2);

      response = client.newRequest(cacheURL + "?action=clear").send();
      ResponseAssertion.assertThat(response).isOk();
      response = client.newRequest(cacheURL + "?action=stats").send();
      ResponseAssertion.assertThat(response).isOk();
      assertEquals(objectMapper.readTree(response.getContent()).get("current_number_of_entries").asInt(), 0);
   }

   @Test
   public void testCacheSize() throws Exception {
      for (int i = 0; i < 100; i++) {
         putInCache("default", i, "" + i, APPLICATION_JSON_TYPE);
      }

      String URL = String.format("http://localhost:%d/rest/v2/caches/default?action=size", restServer().getPort());

      ContentResponse response = client.newRequest(URL).send();

      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).containsReturnedText("100");
   }

   @Test
   public void testCacheFullDetail() throws Exception {
      String URL = String.format("http://localhost:%d/rest/v2/caches/default", restServer().getPort());

      ContentResponse response = client.newRequest(URL).send();
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).containsReturnedText("stats");
      ResponseAssertion.assertThat(response).containsReturnedText("size");
      ResponseAssertion.assertThat(response).containsReturnedText("configuration");
      ResponseAssertion.assertThat(response).containsReturnedText("rehash_in_progress");
      ResponseAssertion.assertThat(response).containsReturnedText("persistent");
      ResponseAssertion.assertThat(response).containsReturnedText("bounded");
      ResponseAssertion.assertThat(response).containsReturnedText("indexed");
      ResponseAssertion.assertThat(response).containsReturnedText("has_remote_backup");
      ResponseAssertion.assertThat(response).containsReturnedText("secured");
      ResponseAssertion.assertThat(response).containsReturnedText("indexing_in_progress");
      ResponseAssertion.assertThat(response).containsReturnedText("queryable");
   }

   public void testCacheQueryable() throws Exception {
      // Default config
      createCache(new ConfigurationBuilder(), "cacheNotQueryable");
      JsonNode details = getCacheDetail("cacheNotQueryable");
      assertFalse(details.get("queryable").asBoolean());

      // Indexed
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().addProperty("default.directory_provider", "local-heap").enable();
      builder.indexing().enable();
      createCache(builder, "cacheIndexed");
      details = getCacheDetail("cacheIndexed");
      assertTrue(details.get("queryable").asBoolean());

      // NonIndexed
      ConfigurationBuilder proto = new ConfigurationBuilder();
      proto.encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);
      createCache(proto, "cacheQueryable");
      details = getCacheDetail("cacheQueryable");
      assertTrue(details.get("queryable").asBoolean());
   }

   private void createCache(ConfigurationBuilder builder, String name) throws Exception {
      String json = new JsonWriter().toJSON(builder.build());
      createCache(json, name);
   }

   private void createCache(String json, String name) throws Exception {
      String url = String.format("http://localhost:%d/rest/v2/caches/%s", restServer().getPort(), name);
      ContentResponse response = client.newRequest(url).header("Content-type", APPLICATION_JSON_TYPE)
            .method(POST).content(new StringContentProvider(json)).send();
      ResponseAssertion.assertThat(response).isOk();
   }

   private JsonNode getCacheDetail(String name) throws Exception {
      String url = String.format("http://localhost:%d/rest/v2/caches/%s", restServer().getPort(), name);
      ContentResponse response = client.newRequest(url).header("Accept", APPLICATION_JSON_TYPE).send();
      ResponseAssertion.assertThat(response).isOk();

      return objectMapper.readTree(response.getContentAsString());
   }

   @Test
   public void testCacheNames() throws Exception {
      String URL = String.format("http://localhost:%d/rest/v2/caches/", restServer().getPort());

      ContentResponse response = client.newRequest(URL).send();

      ResponseAssertion.assertThat(response).isOk();

      JsonNode jsonNode = objectMapper.readTree(response.getContent());
      Set<String> cacheNames = cacheManagers.get(0).getCacheNames();
      assertEquals(cacheNames.size(), jsonNode.size());
      for (int i = 0; i < jsonNode.size(); i++) {
         assertTrue(cacheNames.contains(jsonNode.get(i).asText()));
      }
   }

   @Test
   public void testFlags() throws Exception {
      String proto = "/* @Indexed */ message Entity { /* @Field */ required int32 value=1; }";
      registerSchema("sample.proto", proto);
      ContentResponse response = insertEntity(1, 1000);
      ResponseAssertion.assertThat(response).isOk();
      assertIndexed(1000);

      response = insertEntity(2, 1200, SKIP_INDEXING.toString(), SKIP_CACHE_LOAD.toString());
      ResponseAssertion.assertThat(response).isOk();
      assertNotIndexed(1200);

      response = insertEntity(3, 1200, "Invalid");
      ResponseAssertion.assertThat(response).isBadRequest();
   }

   @Test
   public void testValidateCacheQueryable() throws Exception {
      registerSchema("simple.proto", "message Simple { required int32 value=1;}");
      correctReportNotQueryableCache("jsonCache", new ConfigurationBuilder().encoding().mediaType(APPLICATION_JSON_TYPE).build());
   }

   private void correctReportNotQueryableCache(String name, Configuration configuration) throws Exception {
      createAndWriteToCache(name, configuration);

      ContentResponse response = queryCache(name);
      ResponseAssertion.assertThat(response).isBadRequest();

      JsonNode json = new ObjectMapper().readTree(response.getContentAsString());
      assertTrue(json.get("error").get("cause").toString().matches(".*ISPN028015.*"));
   }

   private ContentResponse queryCache(String name) throws Exception {
      String url = "http://localhost:%d/rest/v2/caches/%s?action=search&query=%s";
      String query = URLEncoder.encode("FROM Simple", "UTF-8");
      String queryURL = String.format(url, restServer().getPort(), name, query);
      return client.newRequest(queryURL).send();
   }

   private void createAndWriteToCache(String name, Configuration configuration) throws Exception {
      String url = String.format("http://localhost:%d/rest/v2/caches/%s", restServer().getPort(), name);
      String jsonConfig = new JsonWriter().toJSON(configuration);
      ContentResponse response = client.newRequest(url).header("Content-type", APPLICATION_JSON_TYPE)
            .method(POST).content(new StringContentProvider(jsonConfig)).send();
      ResponseAssertion.assertThat(response).isOk();

      response = client.newRequest(url + "/1")
            .method(POST).content(new StringContentProvider("{\"_type\":\"Simple\",\"value\":1}")).send();

      ResponseAssertion.assertThat(response).isOk();
   }

   @Test
   public void testGetAllKeys() throws Exception {
      String url = String.format("http://localhost:%d/rest/v2/caches/default?action=%s", restServer().getPort(), "keys");

      ContentResponse response = client.newRequest(url).method(GET).send();
      Set emptyKeys = objectMapper.readValue(response.getContentAsString(), Set.class);
      assertEquals(0, emptyKeys.size());


      putStringValueInCache("default", "1", "value");
      response = client.newRequest(url).method(GET).send();
      Set singleSet = objectMapper.readValue(response.getContentAsString(), Set.class);
      assertEquals(1, singleSet.size());

      int entries = 1000;
      for (int i = 0; i < entries; i++) {
         putStringValueInCache("default", String.valueOf(i), "value");
      }
      response = client.newRequest(url).method(GET).send();
      Set keys = objectMapper.readValue(response.getContentAsString(), Set.class);
      assertEquals(entries, keys.size());
      assertTrue(IntStream.range(0, entries).allMatch(keys::contains));
   }

   @Test
   public void testProtobufMetadataManipulation() throws Exception {
      /**
       * Special role {@link ProtobufMetadataManager#SCHEMA_MANAGER_ROLE} is needed for authz. Subject USER has it
       */
      String cache = PROTOBUF_METADATA_CACHE_NAME;
      String url = String.format("http://localhost:%d/rest/v2/caches/%s?action=%s", restServer().getPort(), cache, "keys");

      putStringValueInCache(cache, "file1.proto", "message A{}");
      putStringValueInCache(cache, "file2.proto", "message B{}");

      ContentResponse response = client.newRequest(url).method(GET).send();
      String contentAsString = response.getContentAsString();
      Set keys = objectMapper.readValue(contentAsString, Set.class);
      assertEquals(2, keys.size());
   }

   @Test
   public void testGetProtoCacheConfig() throws Exception {
      testGetProtoCacheConfig(APPLICATION_XML_TYPE);
      testGetProtoCacheConfig(APPLICATION_JSON_TYPE);
   }

   private void testGetProtoCacheConfig(String accept) throws Exception {
      getCacheConfig(accept, PROTOBUF_METADATA_CACHE_NAME);
   }

   private String getCacheConfig(String accept, String name) throws Exception {
      String url = String.format("http://localhost:%d/rest/v2/caches/%s?action=config", restServer().getPort(), name);
      ContentResponse response = client.newRequest(url).header(ACCEPT, accept).send();
      ResponseAssertion.assertThat(response).isOk();
      return response.getContentAsString();
   }

   @Test
   public void testJSONConversion() throws Exception {
      String url = String.format("http://localhost:%d/rest/v2/caches?action=toJSON", restServer().getPort());

      String xml = "<infinispan>\n" +
            "    <cache-container>\n" +
            "        <distributed-cache name=\"cacheName\" mode=\"SYNC\">\n" +
            "            <memory>\n" +
            "                <object size=\"20\"/>\n" +
            "            </memory>\n" +
            "        </distributed-cache>\n" +
            "    </cache-container>\n" +
            "</infinispan>";

      ContentResponse response = client.newRequest(url).method(POST).content(new StringContentProvider(xml)).send();
      ResponseAssertion.assertThat(response).isOk();

      JsonNode jsonNode = objectMapper.readTree(response.getContentAsString());

      JsonNode distCache = jsonNode.get("distributed-cache");
      JsonNode memory = distCache.get("memory");
      assertEquals("SYNC", distCache.get("mode").asText());
      assertEquals(20, memory.get("max-count").asInt());
   }

   @Test
   public void testCacheExists() throws Exception {
      assertEquals(404, checkCache("invalid"));
      assertEquals(200, checkCache("default"));
      assertEquals(200, checkCache("indexedCache"));
   }

   private int checkCache(String name) throws Exception {
      String url = String.format("http://localhost:%d/rest/v2/caches/%s", restServer().getPort(), name);
      ContentResponse response = client.newRequest(url).method(HEAD).send();
      return response.getStatus();
   }

   private void registerSchema(String name, String schema) throws Exception {
      String url = String.format("http://localhost:%d/rest/v2/caches/___protobuf_metadata/%s", restServer().getPort(), name);
      ContentResponse response = client.newRequest(url).method(HttpMethod.PUT).content(new StringContentProvider(schema)).send();
      ResponseAssertion.assertThat(response).isOk();
   }

   private ContentResponse insertEntity(int key, int value, String... flags) throws Exception {
      String url = String.format("http://localhost:%d/rest/v2/caches/indexedCache/%s", restServer().getPort(), key);
      String json = String.format("{\"_type\": \"Entity\",\"value\": %d}", value);
      Request req = client.newRequest(url)
            .method(HttpMethod.POST)
            .content(new StringContentProvider(json))
            .header(CONTENT_TYPE, APPLICATION_JSON_TYPE);
      if (flags.length > 0) req.header("flags", String.join(",", flags));
      return req.send();
   }

   private void assertIndexed(int value) throws Exception {
      assertIndex(value, true);
   }

   private void assertNotIndexed(int value) throws Exception {
      assertIndex(value, false);
   }

   private void assertIndex(int value, boolean present) throws Exception {
      String query = URLEncoder.encode("FROM Entity where value = " + value);
      String url = String.format("http://localhost:%d/rest/v2/caches/indexedCache?action=search&query=%s", restServer().getPort(), query);
      ContentResponse response = client.newRequest(url).method(GET).send();
      ResponseAssertion.assertThat(response).isOk();
      assertEquals(present, response.getContentAsString().contains(String.valueOf(value)));
   }
}
