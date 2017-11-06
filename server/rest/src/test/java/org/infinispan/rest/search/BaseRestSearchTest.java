package org.infinispan.rest.search;

import static org.eclipse.jetty.http.HttpMethod.GET;
import static org.eclipse.jetty.http.HttpMethod.POST;
import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;
import static org.infinispan.rest.JSONConstants.HIT;
import static org.infinispan.rest.JSONConstants.TOTAL_RESULTS;
import static org.infinispan.rest.JSONConstants.TYPE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.net.URLEncoder;
import java.util.List;

import org.apache.http.HttpStatus;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Base class for query over Rest tests.
 *
 * @since 9.2
 */
@Test(groups = "functional")
public abstract class BaseRestSearchTest extends AbstractInfinispanTest {

   private static final String CACHE_NAME = "search-rest";
   private static final String PROTO_FILE_NAME = "person.proto";
   private static final ObjectMapper MAPPER = new ObjectMapper();

   private HttpClient client;
   private RestServerHelper restServer;
   private String searchUrl;

   @DataProvider(name = "HttpMethodProvider")
   protected static Object[][] provideCacheMode() {
      return new Object[][]{{GET}, {POST}};
   }

   @BeforeClass
   public void setUp() throws Exception {
      restServer = RestServerHelper.defaultRestServer("default");
      restServer.defineCache(CACHE_NAME, getConfigBuilder());
      restServer.start();

      client = new HttpClient();
      client.start();

      searchUrl = String.format("http://localhost:%d/rest/%s?action=search", restServer.getPort(), CACHE_NAME);

      String protoFile = Util.read(IndexedRestSearchTest.class.getClassLoader().getResourceAsStream(PROTO_FILE_NAME));
      registerProtobuf(PROTO_FILE_NAME, protoFile);
      populateData();
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void shouldReportInvalidQueries(HttpMethod method) throws Exception {
      ContentResponse response;
      String wrongQuery = "from Whatever";
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
      assertEquals(response.getStatus(), HttpStatus.SC_BAD_REQUEST);
      String contentAsString = response.getContentAsString();
      assertTrue(contentAsString.contains("Message descriptor not found") ||
            contentAsString.contains("Unknown entity"));
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void shouldReturnEmptyResults(HttpMethod method) throws Exception {
      JsonNode query = query("from org.infinispan.rest.search.entity.Person p where p.name = 'nobody'", method);

      assertZeroHits(query);
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void testSimpleQuery(HttpMethod method) throws Exception {
      JsonNode queryResult = query("from org.infinispan.rest.search.entity.Person p where p.surname = 'Cage'", method);
      assertEquals(queryResult.get("total_results").getIntValue(), 1);

      ArrayNode hits = ArrayNode.class.cast(queryResult.get("hits"));
      assertEquals(hits.size(), 1);

      JsonNode result = hits.iterator().next();
      JsonNode firstHit = result.get(HIT);
      assertEquals(firstHit.get("id").getIntValue(), 2);
      assertEquals(firstHit.get("name").asText(), "Luke");
      assertEquals(firstHit.get("surname").asText(), "Cage");
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void testMultiResultQuery(HttpMethod method) throws Exception {
      JsonNode results = query("from org.infinispan.rest.search.entity.Person p where p.gender = 'MALE'", method);

      assertEquals(results.get(TOTAL_RESULTS).getIntValue(), 3);

      ArrayNode hits = ArrayNode.class.cast(results.get("hits"));
      assertEquals(hits.size(), 3);
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void testProjections(HttpMethod method) throws Exception {
      JsonNode results = query("Select name, surname from org.infinispan.rest.search.entity.Person", method);

      assertEquals(results.get(TOTAL_RESULTS).getIntValue(), 4);

      List<JsonNode> names = results.findValues("name");
      List<JsonNode> surnames = results.findValues("surname");
      List<JsonNode> streets = results.findValues("street");
      List<JsonNode> gender = results.findValues("gender");

      assertEquals(4, names.size());
      assertEquals(4, surnames.size());
      assertEquals(0, streets.size());
      assertEquals(0, gender.size());
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void testGrouping(HttpMethod method) throws Exception {
      JsonNode results = query("select p.gender, count(p.name) from org.infinispan.rest.search.entity.Person p group by p.gender order by p.gender", method);

      assertEquals(results.get(TOTAL_RESULTS).getIntValue(), 2);

      ArrayNode hits = ArrayNode.class.cast(results.get("hits"));

      JsonNode males = hits.get(0);
      assertEquals(males.path(HIT).path("name").getIntValue(), 3);

      JsonNode females = hits.get(1);
      assertEquals(females.path(HIT).path("name").getIntValue(), 1);
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void testOffSet(HttpMethod method) throws Exception {
      String q = "select p.name from org.infinispan.rest.search.entity.Person p order by p.name desc";
      JsonNode results = query(q, method, 2, 2);

      assertEquals(results.get("total_results").getIntValue(), 4);
      ArrayNode hits = ArrayNode.class.cast(results.get("hits"));
      assertEquals(hits.size(), 2);

      assertEquals(hits.get(0).path(HIT).path("name").asText(), "Jessica");
      assertEquals(hits.get(1).path(HIT).path("name").asText(), "Danny");
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void testIncompleteSearch(HttpMethod method) throws Exception {
      ContentResponse response = client.newRequest(searchUrl).method(method).send();

      ResponseAssertion.assertThat(response).isBadRequest();
      String contentAsString = response.getContentAsString();
      JsonNode jsonNode = MAPPER.readTree(contentAsString);

      assertTrue(jsonNode.get("error").path("message").asText().contains("Invalid search request"));
   }

   @AfterClass
   public void tearDown() throws Exception {
      client.stop();
      restServer.stop();
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
   }

   private void index(int id, String person) throws Exception {
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%d", restServer.getPort(), CACHE_NAME, id))
            .method(POST)
            .content(new StringContentProvider(person))
            .header(HttpHeader.CONTENT_TYPE, "application/json")
            .send();
      assertEquals(response.getStatus(), HttpStatus.SC_OK);
   }

   private ObjectNode createPerson(int id, String name, String surname, String street, String postCode, String gender, int... phoneNumbers) {
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
      assertEquals(response.getStatus(), HttpStatus.SC_OK);
      String errorKey = protoFileName.concat(".error");

      ContentResponse errorCheck = client.newRequest(getProtobufMetadataUrl(errorKey)).method(GET).send();

      assertEquals(errorCheck.getStatus(), HttpStatus.SC_NOT_FOUND);
   }

   private String getProtobufMetadataUrl(String key) {
      return String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), PROTOBUF_METADATA_CACHE_NAME, key);
   }

   private void assertZeroHits(JsonNode queryResponse) throws Exception {
      ArrayNode hits = ArrayNode.class.cast(queryResponse.get("hits"));
      assertEquals(hits.size(), 0);
   }

   private JsonNode query(String q, HttpMethod method) throws Exception {
      return query(q, method, 0, 10);
   }

   private JsonNode query(String q, HttpMethod method, int offset, int maxResults) throws Exception {
      Request request;
      if (method == POST) {
         ObjectNode queryReq = MAPPER.createObjectNode();
         queryReq.put("query", q);
         queryReq.put("offset", offset);
         queryReq.put("max_results", maxResults);
         request = client.newRequest(searchUrl).method(POST).content(new StringContentProvider(queryReq.toString()));
      } else {
         StringBuilder queryReq = new StringBuilder(searchUrl);
         queryReq.append("&query=").append(URLEncoder.encode(q, "UTF-8"));
         queryReq.append("&offset=").append(offset);
         queryReq.append("&max_results=").append(maxResults);
         request = client.newRequest(queryReq.toString()).method(GET);
      }
      ContentResponse response = request.send();
      String contentAsString = response.getContentAsString();
      System.out.println(contentAsString);
      assertEquals(response.getStatus(), HttpStatus.SC_OK);
      return MAPPER.readTree(contentAsString);
   }

   protected boolean needType() {
      return false;
   }

   abstract ConfigurationBuilder getConfigBuilder();

}
