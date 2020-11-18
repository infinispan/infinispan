package org.infinispan.rest.search;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.query.remote.json.JSONConstants.HIT;
import static org.infinispan.query.remote.json.JSONConstants.TOTAL_RESULTS;
import static org.infinispan.rest.JSONConstants.TYPE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.client.rest.impl.okhttp.StringRestEntityOkHttp;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapper;
import org.infinispan.rest.RequestHeader;
import org.infinispan.rest.RestTestSCI;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Base class for query over Rest tests.
 *
 * @since 9.2
 */
@Test(groups = "functional")
public abstract class BaseRestSearchTest extends MultipleCacheManagersTest {

   private static final int ENTRIES = 50;

   private static final String CACHE_NAME = "search-rest";
   private static final String PROTO_FILE_NAME = "person.proto";
   static final ObjectMapper MAPPER = new ObjectMapper();

   protected RestClient client;
   protected RestCacheClient cacheClient;
   private final List<RestServerHelper> restServers = new ArrayList<>();

   protected int getNumNodes() {
      return 3;
   }

   @Override
   protected void createCacheManagers() throws Exception {
      // global config
      GlobalConfigurationBuilder globalCfg = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalCfg.serialization().addContextInitializer(RestTestSCI.INSTANCE);

      // test cache config
      ConfigurationBuilder builder = getConfigBuilder();
      builder.statistics().enabled(true);

      // create a 'default' config which is not indexed
      ConfigurationBuilder defaultBuilder = new ConfigurationBuilder();

      // start cache managers + default cache
      createClusteredCaches(getNumNodes(), globalCfg, defaultBuilder, isServerMode(), "default");

      // start rest sever for each cache manager
      cacheManagers.forEach(cm -> {
         RestServerHelper restServer = new RestServerHelper(cm);
         restServer.start(TestResourceTracker.getCurrentTestShortName() + "-" + cm.getAddress());
         restServers.add(restServer);
      });

      // start rest client
      RestClientConfigurationBuilder clientConfigurationBuilder = new RestClientConfigurationBuilder();
      restServers.forEach(s -> clientConfigurationBuilder.addServer().host(s.getHost()).port(s.getPort()));
      client = RestClient.forConfiguration(clientConfigurationBuilder.build());
      cacheClient = client.cache(CACHE_NAME);
      // register protobuf schema
      String protoFileContents = Util.getResourceAsString(PROTO_FILE_NAME, getClass().getClassLoader());
      registerProtobuf(protoFileContents);

      // start indexed test cache that depends on the protobuf schema
      cacheManagers.forEach(cm -> {
         cm.defineConfiguration(CACHE_NAME, builder.build());
         cm.getCache(CACHE_NAME);
      });
   }

   protected boolean isServerMode() {
      return true;
   }

   @DataProvider(name = "HttpMethodProvider")
   protected static Object[][] provideCacheMode() {
      return new Object[][]{{GET}, {POST}};
   }

   protected RestServerHelper pickServer() {
      return restServers.get(0);
   }

   protected String getPath(String cacheName) {
      return String.format("/rest/v2/caches/%s?action=search", cacheName);
   }

   protected String getPath() {
      return String.format("/rest/v2/caches/%s?action=search", CACHE_NAME);
   }

   @BeforeClass
   public void setUp() {
      populateData();
   }

   @AfterMethod
   @Override
   protected void clearContent() {
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void shouldReportInvalidQueries(Method method) throws Exception {
      CompletionStage<RestResponse> response;
      String wrongQuery = "from Whatever";
      String path = getPath();
      if (method == POST) {
         response = client.raw().post(path, "{ \"query\": \"" + wrongQuery + "\"}", APPLICATION_JSON_TYPE);
      } else {
         String getURL = path.concat("&query=").concat(URLEncoder.encode(wrongQuery, "UTF-8"));
         response = client.raw().get(getURL);
      }
      ResponseAssertion.assertThat(response).isBadRequest();
      String contentAsString = join(response).getBody();

      assertTrue(contentAsString.contains("Unknown entity name") ||
            contentAsString.contains("Unknown type name"), contentAsString);
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void shouldReturnEmptyResults(Method method) throws Exception {
      JsonNode query = query("from org.infinispan.rest.search.entity.Person p where p.name = 'nobody'", method);

      assertZeroHits(query);
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void testSimpleQuery(Method method) throws Exception {
      JsonNode queryResult = query("from org.infinispan.rest.search.entity.Person p where p.surname = 'Cage'", method);
      assertEquals(queryResult.get("total_results").intValue(), 1);

      ArrayNode hits = (ArrayNode) queryResult.get("hits");
      assertEquals(hits.size(), 1);

      JsonNode result = hits.iterator().next();
      JsonNode firstHit = result.get(HIT);
      assertEquals(firstHit.get("id").intValue(), 2);
      assertEquals(firstHit.get("name").asText(), "Luke");
      assertEquals(firstHit.get("surname").asText(), "Cage");
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void testMultiResultQuery(Method method) throws Exception {
      JsonNode results = query("from org.infinispan.rest.search.entity.Person p where p.id < 5 and p.gender = 'MALE'", method);

      assertEquals(results.get(TOTAL_RESULTS).intValue(), 3);

      ArrayNode hits = (ArrayNode) results.get("hits");
      assertEquals(hits.size(), 3);
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void testProjections(Method method) throws Exception {
      JsonNode results = query("Select name, surname from org.infinispan.rest.search.entity.Person", method);

      assertEquals(results.get(TOTAL_RESULTS).intValue(), ENTRIES);

      List<JsonNode> names = results.findValues("name");
      List<JsonNode> surnames = results.findValues("surname");
      List<JsonNode> streets = results.findValues("street");
      List<JsonNode> gender = results.findValues("gender");

      assertEquals(10, names.size());
      assertEquals(10, surnames.size());
      assertEquals(0, streets.size());
      assertEquals(0, gender.size());
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void testGrouping(Method method) throws Exception {
      JsonNode results = query("select p.gender, count(p.name) from org.infinispan.rest.search.entity.Person p where p.id < 5 group by p.gender order by p.gender", method);

      assertEquals(results.get(TOTAL_RESULTS).intValue(), 2);

      ArrayNode hits = (ArrayNode) results.get("hits");

      JsonNode males = hits.get(0);
      assertEquals(males.path(HIT).path("name").intValue(), 3);

      JsonNode females = hits.get(1);
      assertEquals(females.path(HIT).path("name").intValue(), 1);
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void testOffset(Method method) throws Exception {
      String q = "select p.name from org.infinispan.rest.search.entity.Person p where p.id < 5 order by p.name desc";
      JsonNode results = query(q, method, 2, 2);

      assertEquals(results.get("total_results").intValue(), 4);
      ArrayNode hits = (ArrayNode) results.get("hits");
      assertEquals(hits.size(), 2);

      assertEquals(hits.get(0).path(HIT).path("name").asText(), "Jessica");
      assertEquals(hits.get(1).path(HIT).path("name").asText(), "Danny");
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void testIncompleteSearch(Method method) throws Exception {
      String searchUrl = getPath();
      CompletionStage<RestResponse> response;
      if (method.equals(POST)) {
         response = client.raw().post(searchUrl);
      } else {
         response = client.raw().get(searchUrl);
      }

      ResponseAssertion.assertThat(response).isBadRequest();
      String contentAsString = join(response).getBody();
      JsonNode jsonNode = MAPPER.readTree(contentAsString);

      assertTrue(jsonNode.get("error").path("message").asText().contains("Invalid search request"));
   }

   @Test
   public void testReadDocument() {
      CompletionStage<RestResponse> response = get("1", "*/*");

      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).bodyNotEmpty();
   }

   @Test
   public void testReadDocumentFromBrowser() throws Exception {
      String mediaType = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";
      RestResponse fromBrowser = join(cacheClient.get("2", mediaType));

      ResponseAssertion.assertThat(fromBrowser).isOk();
      ResponseAssertion.assertThat(fromBrowser).hasContentType(APPLICATION_JSON_TYPE);

      JsonNode person = MAPPER.readTree(fromBrowser.getBody());
      assertEquals(person.get("id").intValue(), 2);
   }

   @Test
   public void testErrorPropagation() throws Exception {
      CompletionStage<RestResponse> response = executeQueryRequest(GET,
            "from org.infinispan.rest.search.entity.Person where id:1", 0, 10);

      ResponseAssertion.assertThat(response).isBadRequest();
   }

   private int getCount() throws Exception {
      JsonNode results = query("from org.infinispan.rest.search.entity.Person", GET);
      return results.get("total_results").asInt();
   }

   @Test
   public void testMassIndexing() {
      boolean indexEnabled = getConfigBuilder().indexing().enabled();

      CompletionStage<RestResponse> clearResponse = client.cache(CACHE_NAME).clearIndex();
      if (indexEnabled) {
         ResponseAssertion.assertThat(clearResponse).isOk();
      } else {
         ResponseAssertion.assertThat(clearResponse).isBadRequest();
      }

      if (indexEnabled) eventually(() -> getCount() == 0);

      CompletionStage<RestResponse> massIndexResponse = client.cache(CACHE_NAME).reindex();

      if (indexEnabled) {
         ResponseAssertion.assertThat(massIndexResponse).isOk();
      } else {
         ResponseAssertion.assertThat(massIndexResponse).isBadRequest();
      }

      eventually(() -> getCount() == ENTRIES);
   }

   @Test
   public void testQueryStats() throws Exception {
      RestResponse response = join(cacheClient.queryStats());
      if (!getConfigBuilder().indexing().enabled()) {
         ResponseAssertion.assertThat(response).isBadRequest();
      } else {
         ResponseAssertion.assertThat(response).isOk();
         JsonNode stats = MAPPER.readTree(response.getBody());
         assertTrue(stats.get("search_query_execution_count").asLong() >= 0);
         assertTrue(stats.get("search_query_total_time").asLong() >= 0);
         assertTrue(stats.get("search_query_execution_max_time").asLong() >= 0);
         assertTrue(stats.get("search_query_execution_avg_time").asLong() >= 0);
         assertTrue(stats.get("object_loading_total_time").asLong() >= 0);
         assertTrue(stats.get("object_loading_execution_max_time").asLong() >= 0);
         assertTrue(stats.get("object_loading_execution_avg_time").asLong() >= 0);
         assertTrue(stats.get("objects_loaded_count").asLong() >= 0);
         assertNotNull(stats.get("search_query_execution_max_time_query_string").asText());

         RestResponse clearResponse = join(cacheClient.clearQueryStats());
         response = join(cacheClient.queryStats());
         stats = MAPPER.readTree(response.getBody());
         ResponseAssertion.assertThat(clearResponse).isOk();
         assertEquals(stats.get("search_query_execution_count").asInt(), 0);
         assertEquals(stats.get("search_query_execution_max_time").asInt(), 0);
      }
   }

   @Test
   public void testIndexStats() throws Exception {
      RestResponse response = join(cacheClient.indexStats());

      if (!getConfigBuilder().indexing().enabled()) {
         ResponseAssertion.assertThat(response).isBadRequest();
      } else {
         ResponseAssertion.assertThat(response).isOk();
         JsonNode stats = MAPPER.readTree(response.getBody());
         ArrayNode indexClassNames = (ArrayNode) stats.get("indexed_class_names");
         String indexedClass = ProtobufValueWrapper.class.getName();

         assertEquals(indexClassNames.get(0).asText(), indexedClass);
         assertNotNull(stats.get("indexed_entities_count"));
         assertTrue(stats.get("index_sizes").get(CACHE_NAME + "_protobuf").asInt() > 0);
      }
   }

   @AfterClass(alwaysRun = true)
   public void tearDown() throws Exception {
      client.close();
      restServers.forEach(RestServerHelper::stop);
   }

   protected void populateData() {
      ObjectNode person1 = createPerson(1, "Jessica", "Jones", "46th St", "NY 10036", "FEMALE", 1111, 2222, 3333);
      ObjectNode person2 = createPerson(2, "Luke", "Cage", "Malcolm X Boulevard", "NY 11221", "MALE", 4444, 5555);
      ObjectNode person3 = createPerson(3, "Matthew", "Murdock", "57th St", "NY 10019", "MALE");
      ObjectNode person4 = createPerson(4, "Danny", "Randy", "Vanderbilt Av.", "NY 10017", "MALE", 2122561084);

      index(1, person1.toString());
      index(2, person2.toString());
      index(3, person3.toString());
      index(4, person4.toString());

      for (int i = 5; i <= ENTRIES; i++) {
         String text = "Generic" + i;
         ObjectNode generic = createPerson(i, text, text, text, text, "MALE", 2122561084);
         index(i, generic.toString());
      }

      eventually(() -> getCount() == ENTRIES);
   }

   private void index(int id, String person) {
      write(id, person, Method.POST, MediaType.APPLICATION_JSON);
   }

   protected void put(int id, String contents) {
      write(id, contents, Method.PUT, MediaType.APPLICATION_JSON);
   }

   protected void write(int id, String contents, Method method, MediaType contentType) {
      RestEntity entity = new StringRestEntityOkHttp(contentType, contents);
      CompletionStage<RestResponse> response;
      if (method.equals(POST)) {
         response = client.cache(CACHE_NAME).post(String.valueOf(id), entity);
      } else {
         response = client.cache(CACHE_NAME).put(String.valueOf(id), entity);
      }
      ResponseAssertion.assertThat(response).isOk();
   }

   protected CompletionStage<RestResponse> get(String id, String accept) {
      String path = String.format("/rest/v2/caches/%s/%s", CACHE_NAME, id);
      return client.raw().get(path, Collections.singletonMap(RequestHeader.ACCEPT_HEADER.getValue(), accept));
   }

   protected ObjectNode createPerson(int id, String name, String surname, String street, String postCode, String gender, int... phoneNumbers) {
      ObjectNode person = MAPPER.createObjectNode();
      person.put(TYPE, "org.infinispan.rest.search.entity.Person");
      person.put("id", id);
      person.put("name", name);
      person.put("surname", surname);
      person.put("gender", gender);

      ObjectNode address = person.putObject("address");
      if (needType()) address.put(TYPE, "org.infinispan.rest.search.entity.Address");
      address.put("street", street);
      address.put("postCode", postCode);

      ArrayNode numbers = person.putArray("phoneNumbers");
      for (int phone : phoneNumbers) {
         ObjectNode number = numbers.addObject();
         if (needType()) number.put(TYPE, "org.infinispan.rest.search.entity.PhoneNumber");
         number.put("number", phone);
      }
      return person;
   }

   protected void registerProtobuf(String protoFileContents) {
      CompletionStage<RestResponse> response = client.schemas().post(PROTO_FILE_NAME, protoFileContents);
      ResponseAssertion.assertThat(response).hasNoErrors();
   }

   private void assertZeroHits(JsonNode queryResponse) {
      ArrayNode hits = (ArrayNode) queryResponse.get("hits");
      assertEquals(hits.size(), 0);
   }

   private JsonNode query(String q, Method method) throws Exception {
      return query(q, method, 0, 10);
   }

   private CompletionStage<RestResponse> executeQueryRequest(Method method, String q, int offset, int maxResults) throws Exception {
      String path = getPath();
      if (method == POST) {
         ObjectNode queryReq = MAPPER.createObjectNode();
         queryReq.put("query", q);
         queryReq.put("offset", offset);
         queryReq.put("max_results", maxResults);
         return client.raw().post(path, queryReq.toString(), APPLICATION_JSON_TYPE);
      }
      String queryReq = path + "&query=" + URLEncoder.encode(q, "UTF-8") +
            "&offset=" + offset +
            "&max_results=" + maxResults;
      return client.raw().get(queryReq);
   }

   private JsonNode query(String q, Method method, int offset, int maxResults) throws Exception {
      CompletionStage<RestResponse> response = executeQueryRequest(method, q, offset, maxResults);
      ResponseAssertion.assertThat(response).isOk();
      String contentAsString = join(response).getBody();
      return MAPPER.readTree(contentAsString);
   }

   protected boolean needType() {
      return false;
   }

   protected abstract ConfigurationBuilder getConfigBuilder();
}
