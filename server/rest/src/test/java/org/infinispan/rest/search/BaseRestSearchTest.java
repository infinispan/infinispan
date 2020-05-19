package org.infinispan.rest.search;

import static org.eclipse.jetty.http.HttpMethod.GET;
import static org.eclipse.jetty.http.HttpMethod.POST;
import static org.eclipse.jetty.http.HttpStatus.BAD_REQUEST_400;
import static org.eclipse.jetty.http.HttpStatus.OK_200;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;
import static org.infinispan.query.remote.json.JSONConstants.HIT;
import static org.infinispan.query.remote.json.JSONConstants.TOTAL_RESULTS;
import static org.infinispan.rest.JSONConstants.TYPE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapper;
import org.infinispan.rest.RestTestSCI;
import org.infinispan.rest.assertion.ResponseAssertion;
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

   protected HttpClient client;
   private List<RestServerHelper> restServers = new ArrayList<>();

   protected int getNumNodes() {
      return 3;
   }

   @Override
   protected void createCacheManagers() {
      GlobalConfigurationBuilder globalCfg = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalCfg.serialization().addContextInitializer(RestTestSCI.INSTANCE);
      ConfigurationBuilder builder = getConfigBuilder();
      builder.statistics().enabled(true);
      createClusteredCaches(getNumNodes(), globalCfg, builder, isServerMode(), CACHE_NAME, "default");
      waitForClusterToForm(CACHE_NAME);
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

   protected String getUrl(RestServerHelper restServerHelper) {
      return getUrl(restServerHelper, CACHE_NAME);
   }

   protected String getUrl(RestServerHelper restServerHelper, String cacheName) {
      return String.format("http://localhost:%d/rest/v2/caches/%s?action=search", restServerHelper.getPort(), cacheName);
   }

   @BeforeClass
   public void setUp() throws Exception {
      IntStream.range(0, getNumNodes()).forEach(n -> {
         EmbeddedCacheManager manager = cacheManagers.get(n);
         RestServerHelper restServer = new RestServerHelper(manager);
         restServer.start(TestResourceTracker.getCurrentTestShortName() + "-" + manager.getAddress());
         restServers.add(restServer);
      });

      client = new HttpClient();
      client.start();

      String protoFile = Util.getResourceAsString(PROTO_FILE_NAME, getClass().getClassLoader());
      registerProtobuf(PROTO_FILE_NAME, protoFile);
      populateData();
   }

   @AfterMethod
   @Override
   protected void clearContent() {
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void shouldReportInvalidQueries(HttpMethod method) throws Exception {
      ContentResponse response;
      String wrongQuery = "from Whatever";
      String searchUrl = getUrl(pickServer());
      if (method == POST) {
         response = client
               .newRequest(searchUrl)
               .method(POST)
               .content(new StringContentProvider("{ \"query\": \"" + wrongQuery + "\"}"))
               .send();
      } else {
         response = client
               .newRequest(searchUrl.concat("&query=").concat(URLEncoder.encode(wrongQuery, "UTF-8")))
               .method(GET)
               .send();
      }
      assertEquals(response.getStatus(), BAD_REQUEST_400);
      String contentAsString = response.getContentAsString();
      assertTrue(contentAsString.contains("Unknown entity name") ||
            contentAsString.contains("Unknown type name"), contentAsString);
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void shouldReturnEmptyResults(HttpMethod method) throws Exception {
      JsonNode query = query("from org.infinispan.rest.search.entity.Person p where p.name = 'nobody'", method);

      assertZeroHits(query);
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void testSimpleQuery(HttpMethod method) throws Exception {
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
   public void testMultiResultQuery(HttpMethod method) throws Exception {
      JsonNode results = query("from org.infinispan.rest.search.entity.Person p where p.id < 5 and p.gender = 'MALE'", method);

      assertEquals(results.get(TOTAL_RESULTS).intValue(), 3);

      ArrayNode hits = (ArrayNode) results.get("hits");
      assertEquals(hits.size(), 3);
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void testProjections(HttpMethod method) throws Exception {
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
   public void testGrouping(HttpMethod method) throws Exception {
      JsonNode results = query("select p.gender, count(p.name) from org.infinispan.rest.search.entity.Person p where p.id < 5 group by p.gender order by p.gender", method);

      assertEquals(results.get(TOTAL_RESULTS).intValue(), 2);

      ArrayNode hits = (ArrayNode) results.get("hits");

      JsonNode males = hits.get(0);
      assertEquals(males.path(HIT).path("name").intValue(), 3);

      JsonNode females = hits.get(1);
      assertEquals(females.path(HIT).path("name").intValue(), 1);
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void testOffset(HttpMethod method) throws Exception {
      String q = "select p.name from org.infinispan.rest.search.entity.Person p where p.id < 5 order by p.name desc";
      JsonNode results = query(q, method, 2, 2, CACHE_NAME);

      assertEquals(results.get("total_results").intValue(), 4);
      ArrayNode hits = (ArrayNode) results.get("hits");
      assertEquals(hits.size(), 2);

      assertEquals(hits.get(0).path(HIT).path("name").asText(), "Jessica");
      assertEquals(hits.get(1).path(HIT).path("name").asText(), "Danny");
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void testIncompleteSearch(HttpMethod method) throws Exception {
      String searchUrl = getUrl(pickServer());
      ContentResponse response = client.newRequest(searchUrl).method(method).send();

      ResponseAssertion.assertThat(response).isBadRequest();
      String contentAsString = response.getContentAsString();
      JsonNode jsonNode = MAPPER.readTree(contentAsString);

      assertTrue(jsonNode.get("error").path("message").asText().contains("Invalid search request"));
   }

   @Test
   public void testReadDocument() throws Exception {
      ContentResponse response = get("1", "*/*");

      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).bodyNotEmpty();
   }

   @Test
   public void testReadDocumentFromBrowser() throws Exception {
      ContentResponse fromBrowser = get("2", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");

      ResponseAssertion.assertThat(fromBrowser).isOk();
      ResponseAssertion.assertThat(fromBrowser).bodyNotEmpty();
      ResponseAssertion.assertThat(fromBrowser).hasContentType(APPLICATION_JSON_TYPE);

      JsonNode person = MAPPER.readTree(fromBrowser.getContentAsString());
      assertEquals(person.get("id").intValue(), 2);
   }

   @Test
   public void testErrorPropagation() throws Exception {
      ContentResponse response = executeQueryRequest(CACHE_NAME, HttpMethod.GET,
            "from org.infinispan.rest.search.entity.Person where id:1", 0, 10);

      assertEquals(response.getStatus(), BAD_REQUEST_400);
   }

   private int getCount() throws Exception {
      JsonNode results = query("from org.infinispan.rest.search.entity.Person", GET);
      return results.get("total_results").asInt();
   }

   @Test
   public void testMassIndexing() throws Exception {
      boolean indexEnabled = getConfigBuilder().indexing().enabled();
      RestServerHelper helper = restServers.get(0);
      int port = helper.getPort();

      String clearIndexURL = String.format("http://localhost:%d/rest/v2/caches/%s/search/indexes?action=clear", port, CACHE_NAME);
      String massIndexURL = String.format("http://localhost:%d/rest/v2/caches/%s/search/indexes?action=mass-index", port, CACHE_NAME);
      Request massIndexRequest = client.newRequest(massIndexURL);
      Request clearIndexRequest = client.newRequest(clearIndexURL);

      ContentResponse clearResponse = clearIndexRequest.send();
      assertEquals(clearResponse.getStatus(), indexEnabled ? OK_200 : BAD_REQUEST_400);

      if (indexEnabled) eventually(() -> getCount() == 0);

      ContentResponse massIndexResponse = massIndexRequest.send();

      assertEquals(massIndexResponse.getStatus(), indexEnabled ? OK_200 : BAD_REQUEST_400);

      eventually(() -> getCount() == ENTRIES);
   }

   @Test
   public void testQueryStats() throws Exception {
      RestServerHelper helper = restServers.get(0);
      int port = helper.getPort();
      String getStatsURL = String.format("http://localhost:%d/rest/v2/caches/%s/search/query/stats", port, CACHE_NAME);
      String resetStatsURL = String.format("http://localhost:%d/rest/v2/caches/%s/search/query/stats?action=clear", port, CACHE_NAME);
      Request statsRequest = client.newRequest(getStatsURL);
      Request clearStatsRequest = client.newRequest(resetStatsURL);
      ContentResponse response = statsRequest.send();
      if (!getConfigBuilder().indexing().enabled()) {
         assertEquals(response.getStatus(), BAD_REQUEST_400);
      } else {
         assertEquals(response.getStatus(), OK_200);
         JsonNode stats = MAPPER.readTree(response.getContentAsString());
         assertTrue(stats.get("search_query_execution_count").asInt() >= 0);
         assertTrue(stats.get("search_query_total_time").asInt() >= 0);
         assertTrue(stats.get("search_query_execution_max_time").asInt() >= 0);
         assertTrue(stats.get("search_query_execution_avg_time").asInt() >= 0);
         assertTrue(stats.get("object_loading_total_time").asInt() >= 0);
         assertTrue(stats.get("object_loading_execution_max_time").asInt() >= 0);
         assertTrue(stats.get("object_loading_execution_avg_time").asInt() >= 0);
         assertTrue(stats.get("objects_loaded_count").asInt() >= 0);
         assertNotNull(stats.get("search_query_execution_max_time_query_string").asText());

         ContentResponse clearResponse = clearStatsRequest.send();
         response = statsRequest.send();
         stats = MAPPER.readTree(response.getContentAsString());
         assertEquals(clearResponse.getStatus(), OK_200);
         assertEquals(stats.get("search_query_execution_count").asInt(), 0);
         assertEquals(stats.get("search_query_execution_max_time").asInt(), 0);
      }
   }

   @Test
   public void testIndexStats() throws Exception {
      RestServerHelper helper = restServers.get(0);
      int port = helper.getPort();
      String getStatsURL = String.format("http://localhost:%d/rest/v2/caches/%s/search/indexes/stats", port, CACHE_NAME);
      Request statsRequest = client.newRequest(getStatsURL);

      ContentResponse response = statsRequest.send();
      if (!getConfigBuilder().indexing().enabled()) {
         assertEquals(response.getStatus(), BAD_REQUEST_400);
      } else {
         assertEquals(response.getStatus(), OK_200);
         JsonNode stats = MAPPER.readTree(response.getContentAsString());
         ArrayNode indexClassNames = (ArrayNode) stats.get("indexed_class_names");
         String indexedClass = ProtobufValueWrapper.class.getName();

         assertEquals(indexClassNames.get(0).asText(), indexedClass);
         assertNotNull(stats.get("indexed_entities_count"));
         assertTrue(stats.get("index_sizes").get(CACHE_NAME + "_protobuf").asInt() > 0);
      }
   }

   @AfterClass
   public void tearDown() throws Exception {
      client.stop();
      restServers.forEach(RestServerHelper::stop);
   }

   protected void populateData() throws Exception {
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

   private void index(int id, String person) throws Exception {
      write(id, person, HttpMethod.POST, MediaType.APPLICATION_JSON);
   }

   protected void put(int id, String contents) throws Exception {
      write(id, contents, HttpMethod.PUT, MediaType.APPLICATION_JSON);
   }

   protected void write(int id, String contents, HttpMethod method, MediaType contentType) throws Exception {
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/v2/caches/%s/%d", pickServer().getPort(), CACHE_NAME, id))
            .method(method)
            .content(new StringContentProvider(contents))
            .header(HttpHeader.CONTENT_TYPE, contentType.toString())
            .send();
      assertEquals(response.getStatus(), HttpStatus.NO_CONTENT_204);
   }

   protected ContentResponse get(String id, String accept) throws Exception {
      return client.newRequest(String.format("http://localhost:%d/rest/v2/caches/%s/%s", pickServer().getPort(), CACHE_NAME, id))
            .header(HttpHeader.ACCEPT, accept)
            .send();
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

   protected void registerProtobuf(String protoFileName, String protoFileContents) throws Exception {
      String protobufMetadataUrl = getProtobufMetadataUrl(protoFileName);
      ContentResponse response = client
            .newRequest(protobufMetadataUrl)
            .content(new StringContentProvider(protoFileContents))
            .method(POST)
            .send();
      assertEquals(response.getStatus(), HttpStatus.NO_CONTENT_204);
      String errorKey = protoFileName.concat(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX);

      ContentResponse errorCheck = client.newRequest(getProtobufMetadataUrl(errorKey)).method(GET).send();

      assertEquals(errorCheck.getStatus(), HttpStatus.NOT_FOUND_404);
   }

   private String getProtobufMetadataUrl(String key) {
      return String.format("http://localhost:%d/rest/v2/caches/%s/%s", pickServer().getPort(), PROTOBUF_METADATA_CACHE_NAME, key);
   }

   private void assertZeroHits(JsonNode queryResponse) {
      ArrayNode hits = (ArrayNode) queryResponse.get("hits");
      assertEquals(hits.size(), 0);
   }

   private JsonNode query(String q, HttpMethod method) throws Exception {
      return query(q, method, 0, 10, CACHE_NAME);
   }

   private ContentResponse executeQueryRequest(String cacheName, HttpMethod method, String q, int offset, int maxResults) throws Exception {
      Request request;
      String searchUrl = getUrl(pickServer(), cacheName);
      if (method == POST) {
         ObjectNode queryReq = MAPPER.createObjectNode();
         queryReq.put("query", q);
         queryReq.put("offset", offset);
         queryReq.put("max_results", maxResults);
         request = client.newRequest(searchUrl).method(POST).content(new StringContentProvider(queryReq.toString()));
      } else {
         String queryReq = searchUrl + "&query=" + URLEncoder.encode(q, "UTF-8") +
               "&offset=" + offset +
               "&max_results=" + maxResults;
         request = client.newRequest(queryReq).method(GET);
      }
      return request.send();
   }

   private JsonNode query(String q, HttpMethod method, int offset, int maxResults, String cacheName) throws Exception {
      ContentResponse response = executeQueryRequest(cacheName, method, q, offset, maxResults);
      String contentAsString = response.getContentAsString();
      assertEquals(response.getStatus(), OK_200);
      return MAPPER.readTree(contentAsString);
   }

   protected boolean needType() {
      return false;
   }

   abstract ConfigurationBuilder getConfigBuilder();

}
