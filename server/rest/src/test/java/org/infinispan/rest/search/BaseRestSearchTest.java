package org.infinispan.rest.search;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.commons.util.concurrent.CompletionStages.join;
import static org.infinispan.rest.JSONConstants.TYPE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.server.core.query.json.JSONConstants.HIT;
import static org.infinispan.server.core.query.json.JSONConstants.HIT_COUNT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.query.Search;
import org.infinispan.query.core.stats.IndexInfo;
import org.infinispan.query.core.stats.SearchStatistics;
import org.infinispan.rest.RequestHeader;
import org.infinispan.rest.RestTestSCI;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.rest.search.entity.Person;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Base class for query over Rest tests.
 *
 * @since 9.2
 */
@Test(groups = "functional")
public abstract class BaseRestSearchTest extends MultipleCacheManagersTest {

   private static final int ENTRIES = 50;

   private static final String PROTO_FILE_NAME = "person.proto";

   protected RestClient client;
   protected RestCacheClient cacheClient;
   private final List<RestServerHelper> restServers = new ArrayList<>();
   private final List<RestClient> clients = new ArrayList<>();

   protected int getNumNodes() {
      return 3;
   }

   protected abstract String cacheName();

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
         restServer.start(TestResourceTracker.getCurrentTestShortName());
         restServers.add(restServer);
         RestClientConfigurationBuilder clientConfigurationBuilder = new RestClientConfigurationBuilder();
         clientConfigurationBuilder.addServer().host(restServer.getHost()).port(restServer.getPort());
         clients.add(RestClient.forConfiguration(clientConfigurationBuilder.build()));
      });

      client = clients.get(0);
      cacheClient = client.cache(cacheName());
      // register protobuf schema
      String protoFileContents = Util.getResourceAsString(PROTO_FILE_NAME, getClass().getClassLoader());
      registerProtobuf(protoFileContents);

      // start indexed test cache that depends on the protobuf schema
      cacheManagers.forEach(cm -> {
         cm.defineConfiguration(cacheName(), builder.build());
         cm.getCache(cacheName());
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
      return String.format("/rest/v2/caches/%s?action=search", cacheName());
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
      String wrongQuery = "from Whatever";
      checkUnknownEntityError(wrongQuery, method);
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void shouldReportInvalidQueriesWhenIndexed(Method method) throws Exception {
      boolean indexingEnabled = cacheManagers.get(0).getCache(cacheName()).getCacheConfiguration().indexing().enabled();
      String queryStr = "from org.infinispan.rest.search.entity.Address";
      if (indexingEnabled) {
         checkUnknownEntityError(queryStr, method);
      } else {
         assertZeroHits(query(queryStr, method));
      }
   }

   private void checkUnknownEntityError(String wrongQuery, Method method) {
      CompletionStage<RestResponse> response;
      String path = getPath();
      if (method == POST) {
         response = client.raw().post(path, RestEntity.create(APPLICATION_JSON, "{ \"query\": \"" + wrongQuery + "\"}"));
      } else {
         String getURL = path.concat("&query=").concat(URLEncoder.encode(wrongQuery, StandardCharsets.UTF_8));
         response = client.raw().get(getURL);
      }
      ResponseAssertion.assertThat(response).isBadRequest();
      String contentAsString = join(response).body();

      assertTrue(contentAsString.contains("Unknown entity name") ||
            contentAsString.contains("Unknown type name"), contentAsString);
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void shouldReturnEmptyResults(Method method) throws Exception {
      Json query = query("from org.infinispan.rest.search.entity.Person p where p.name = 'nobody'", method);

      assertZeroHits(query);
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void testSimpleQuery(Method method) throws Exception {
      Json queryResult = query("from org.infinispan.rest.search.entity.Person p where p.surname = 'Cage'", method);
      assertEquals(queryResult.at("hit_count").asInteger(), 1);

      Json hits = queryResult.at("hits");
      List<Json> jsonHits = hits.asJsonList();
      assertEquals(jsonHits.size(), 1);

      Json result = jsonHits.iterator().next();
      Json firstHit = result.at(HIT);
      assertEquals(firstHit.at("id").asInteger(), 2);
      assertEquals(firstHit.at("name").asString(), "Luke");
      assertEquals(firstHit.at("surname").asString(), "Cage");
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void testMultiResultQuery(Method method) throws Exception {
      Json results = query("from org.infinispan.rest.search.entity.Person p where p.id < 5 and p.gender = 'MALE'", method);

      assertEquals(results.at(HIT_COUNT).asInteger(), 3);

      Json hits = results.at("hits");
      assertEquals(hits.asList().size(), 3);
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void testProjections(Method method) throws Exception {
      Json results = query("Select name, surname from org.infinispan.rest.search.entity.Person", method);

      assertEquals(results.at(HIT_COUNT).asInteger(), ENTRIES);

      Json hits = results.at("hits");

      List<?> names = findValues(hits, "name");
      List<?> surnames = findValues(hits, "surname");
      List<?> streets = findValues(hits, "street");
      List<?> gender = findValues(hits, "gender");

      assertEquals(10, names.size());
      assertEquals(10, surnames.size());
      assertEquals(0, streets.size());
      assertEquals(0, gender.size());
   }

   private List<?> findValues(Json hits, String fieldName) {
      return hits.asJsonList().stream()
            .map(j -> j.at("hit"))
            .map(h -> h.asMap().get(fieldName))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void testGrouping(Method method) throws Exception {
      Json results = query("select p.gender, count(p.name) from org.infinispan.rest.search.entity.Person p where p.id < 5 group by p.gender order by p.gender", method);

      assertEquals(results.at(HIT_COUNT).asInteger(), 2);

      Json hits = results.at("hits");

      Json males = hits.at(0);
      assertEquals(males.at(HIT).at("COUNT(name)").asInteger(), 3);

      Json females = hits.at(1);
      assertEquals(females.at(HIT).at("COUNT(name)").asInteger(), 1);
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void testOffset(Method method) throws Exception {
      String q = "select p.name from org.infinispan.rest.search.entity.Person p where p.id < 5 order by p.name desc";
      Json results = query(q, method, 2, 2);

      assertEquals(results.at("hit_count").asInteger(), 4);
      assertEquals(results.at("hit_count_exact").asBoolean(), true);
      Json hits = results.at("hits");
      assertEquals(hits.asList().size(), 2);

      assertEquals(hits.at(0).at(HIT).at("name").asString(), "Jessica");
      assertEquals(hits.at(1).at(HIT).at("name").asString(), "Danny");
   }

   @Test(dataProvider = "HttpMethodProvider")
   public void testIncompleteSearch(Method method) {
      String searchUrl = getPath();
      CompletionStage<RestResponse> response;
      if (method.equals(POST)) {
         response = client.raw().post(searchUrl);
      } else {
         response = client.raw().get(searchUrl);
      }

      ResponseAssertion.assertThat(response).isBadRequest();
      String contentAsString = join(response).body();
      Json jsonNode = Json.read(contentAsString);

      assertTrue(jsonNode.at("error").at("message").asString().contains("Invalid search request"));
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

      Json person = Json.read(fromBrowser.body());
      assertEquals(person.at("id").asInteger(), 2);
   }

   @Test
   public void testErrorPropagation() throws Exception {
      CompletionStage<RestResponse> response = executeQueryRequest(GET,
            "from org.infinispan.rest.search.entity.Person where id:1", 0, 10);

      ResponseAssertion.assertThat(response).isBadRequest();
   }

   private int getCount() throws Exception {
      Json results = query("from org.infinispan.rest.search.entity.Person", GET);
      return results.at("hit_count").asInteger();
   }

   @Test
   public void testMassIndexing() {
      boolean indexEnabled = getConfigBuilder().indexing().enabled();

      CompletionStage<RestResponse> clearResponse = client.cache(cacheName()).clearIndex();
      if (indexEnabled) {
         ResponseAssertion.assertThat(clearResponse).isOk();
      } else {
         ResponseAssertion.assertThat(clearResponse).isBadRequest();
      }

      if (indexEnabled) eventually(() -> getCount() == 0);

      CompletionStage<RestResponse> massIndexResponse = client.cache(cacheName()).reindex();

      if (indexEnabled) {
         ResponseAssertion.assertThat(massIndexResponse).isOk();
      } else {
         ResponseAssertion.assertThat(massIndexResponse).isBadRequest();
      }

      eventually(() -> getCount() == ENTRIES);
   }

   @Test
   public void testReindexAfterSchemaChanges() throws Exception {
      if (!getConfigBuilder().indexing().enabled()) return;

      // Update the schema adding an extra field
      String changedSchema = Util.getResourceAsString("person-changed.proto", getClass().getClassLoader());
      registerProtobuf(changedSchema);

      // reindex
      join(client.cache(cacheName()).reindex()).close();

      // Query on the new field
      Json result = query("FROM org.infinispan.rest.search.entity.Person where newField = 'value'", GET);
      assertZeroHits(result);
   }

   @Test
   public void testQueryStats() throws Exception {
      RestResponse response = join(cacheClient.queryStats());
      if (!getConfigBuilder().indexing().enabled()) {
         ResponseAssertion.assertThat(response).isBadRequest();
      } else {
         ResponseAssertion.assertThat(response).isOk();
         Json stats = Json.read(response.body());
         assertTrue(stats.at("search_query_execution_count").asLong() >= 0);
         assertTrue(stats.at("search_query_total_time").asLong() >= 0);
         assertTrue(stats.at("search_query_execution_max_time").asLong() >= 0);
         assertTrue(stats.at("search_query_execution_avg_time").asLong() >= 0);
         assertTrue(stats.at("object_loading_total_time").asLong() >= 0);
         assertTrue(stats.at("object_loading_execution_max_time").asLong() >= 0);
         assertTrue(stats.at("object_loading_execution_avg_time").asLong() >= 0);
         assertTrue(stats.at("objects_loaded_count").asLong() >= 0);
         assertNotNull(stats.at("search_query_execution_max_time_query_string").asString());

         RestResponse clearResponse = join(cacheClient.clearQueryStats());
         response = join(cacheClient.queryStats());
         stats = Json.read(response.body());
         ResponseAssertion.assertThat(clearResponse).isOk();
         assertEquals(stats.at("search_query_execution_count").asLong(), 0);
         assertEquals(stats.at("search_query_execution_max_time").asLong(), 0);
      }
   }

   @Test
   public void testIndexStats() {
      RestResponse response = join(cacheClient.indexStats());

      if (!getConfigBuilder().indexing().enabled()) {
         ResponseAssertion.assertThat(response).isBadRequest();
      } else {
         ResponseAssertion.assertThat(response).isOk();
         Json stats = Json.read(response.body());
         Json indexClassNames = stats.at("indexed_class_names");

         String indexName = "org.infinispan.rest.search.entity.Person";
         assertEquals(indexClassNames.at(0).asString(), indexName);
         assertNotNull(stats.at("indexed_entities_count"));
         //TODO: Index sizes are not currently exposed (HSEARCH-4056)
         assertTrue(stats.at("index_sizes").at(indexName).asInteger() >= 0);
      }
   }

   @Test
   public void testLocalQuery() {
      Configuration configuration = getConfigBuilder().build();
      ClusteringConfiguration clustering = configuration.clustering();

      int sum = clients.stream().map(cli -> {
         RestResponse queryResponse = join(cli.cache(cacheName()).query("FROM org.infinispan.rest.search.entity.Person", true));
         return Json.read(queryResponse.body()).at(HIT_COUNT).asInteger();
      }).mapToInt(value -> value).sum();

      int expected = ENTRIES;

      if (clustering.cacheMode().isClustered()) {
         expected = ENTRIES * clustering.hash().numOwners();
      }
      assertEquals(expected, sum);
   }

   @Test
   public void testLocalReindexing() {
      boolean indexEnabled = getConfigBuilder().indexing().enabled();
      if (!indexEnabled || getNumNodes() < 2) return;

      // reindex() reindex the whole cluster
      join(clients.get(0).cache(cacheName()).reindex());
      assertAllIndexed();

      clearIndexes();

      // Local indexing should not touch the indexes of other caches
      join(clients.get(0).cache(cacheName()).reindexLocal());
      assertOnlyIndexed(0);

      clearIndexes();

      join(clients.get(1).cache(cacheName()).reindexLocal());
      assertOnlyIndexed(1);

      clearIndexes();

      join(clients.get(2).cache(cacheName()).reindexLocal());
      assertOnlyIndexed(2);

   }

   void clearIndexes() {
      join(clients.get(0).cache(cacheName()).clearIndex());
   }

   private void assertIndexState(BiConsumer<IndexInfo, Integer> cacheIndexInfo) {
      IntStream.range(0, getNumNodes()).forEach(i -> {
         Cache<?, ?> cache = cache(i, cacheName());
         SearchStatistics searchStatistics = Search.getSearchStatistics(cache);
         Map<String, IndexInfo> indexInfo = join(searchStatistics.getIndexStatistics().computeIndexInfos());
         cacheIndexInfo.accept(indexInfo.get(Person.class.getName()), i);
      });
   }

   private void assertAllIndexed() {
      assertIndexState((indexInfo, i) -> assertTrue(indexInfo.count() > 0));
   }

   private void assertOnlyIndexed(int id) {
      assertIndexState((indexInfo, i) -> {
         long count = indexInfo.count();
         if (i == id) {
            assertTrue(count > 0);
         } else {
            assertEquals(count, 0);
         }
      });
   }

   @AfterClass(alwaysRun = true)
   public void tearDown() throws Exception {
      clients.forEach(Util::close);
      restServers.forEach(RestServerHelper::stop);
   }

   protected void populateData() {
      Json person1 = createPerson(1, "Jessica", "Jones", "46th St", "NY 10036", "FEMALE", 1111, 2222, 3333);
      Json person2 = createPerson(2, "Luke", "Cage", "Malcolm X Boulevard", "NY 11221", "MALE", 4444, 5555);
      Json person3 = createPerson(3, "Matthew", "Murdock", "57th St", "NY 10019", "MALE");
      Json person4 = createPerson(4, "Danny", "Randy", "Vanderbilt Av.", "NY 10017", "MALE", 2122561084);

      index(1, person1.toString());
      index(2, person2.toString());
      index(3, person3.toString());
      index(4, person4.toString());

      for (int i = 5; i <= ENTRIES; i++) {
         String text = "Generic" + i;
         Json generic = createPerson(i, text, text, text, text, "MALE", 2122561084);
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
      RestEntity entity = RestEntity.create(contentType, contents);
      CompletionStage<RestResponse> response;
      if (method.equals(POST)) {
         response = client.cache(cacheName()).post(String.valueOf(id), entity);
      } else {
         response = client.cache(cacheName()).put(String.valueOf(id), entity);
      }
      ResponseAssertion.assertThat(response).isOk();
   }

   protected CompletionStage<RestResponse> get(String id, String accept) {
      String path = String.format("/rest/v2/caches/%s/%s", cacheName(), id);
      return client.raw().get(path, Collections.singletonMap(RequestHeader.ACCEPT_HEADER.toString(), accept));
   }

   protected Json createPerson(int id, String name, String surname, String street, String postCode, String gender, int... phoneNumbers) {
      Json person = Json.object();
      person.set(TYPE, "org.infinispan.rest.search.entity.Person");
      person.set("id", id);
      person.set("name", name);
      person.set("surname", surname);
      person.set("gender", gender);

      Json address = Json.object();
      if (needType()) address.set(TYPE, "org.infinispan.rest.search.entity.Address");
      address.set("street", street);
      address.set("postCode", postCode);

      person.set("address", address);

      Json numbers = Json.array();
      for (int phone : phoneNumbers) {
         Json number = Json.object();
         if (needType()) number.set(TYPE, "org.infinispan.rest.search.entity.PhoneNumber");
         number.set("number", phone);
      }
      person.set("phoneNumbers", numbers);
      return person;
   }

   protected void registerProtobuf(String protoFileContents) {
      CompletionStage<RestResponse> response = client.schemas().put(PROTO_FILE_NAME, protoFileContents);
      ResponseAssertion.assertThat(response).hasNoErrors();
   }

   private void assertZeroHits(Json queryResponse) {
      Json hits = queryResponse.at("hits");
      assertEquals(hits.asList().size(), 0);
   }

   private Json query(String q, Method method) throws Exception {
      return query(q, method, 0, 10);
   }

   private CompletionStage<RestResponse> executeQueryRequest(Method method, String q, int offset, int maxResults) throws Exception {
      String path = getPath();
      if (method == POST) {
         Json queryReq = Json.object();
         queryReq.set("query", q);
         queryReq.set("offset", offset);
         queryReq.set("max_results", maxResults);
         return client.raw().post(path, RestEntity.create(APPLICATION_JSON, queryReq.toString()));
      }
      String queryReq = path + "&query=" + URLEncoder.encode(q, "UTF-8") +
            "&offset=" + offset +
            "&max_results=" + maxResults;
      return client.raw().get(queryReq);
   }

   private Json query(String q, Method method, int offset, int maxResults) throws Exception {
      CompletionStage<RestResponse> response = executeQueryRequest(method, q, offset, maxResults);
      ResponseAssertion.assertThat(response).isOk();
      String contentAsString = join(response).body();
      return Json.read(contentAsString);
   }

   protected boolean needType() {
      return false;
   }

   protected abstract ConfigurationBuilder getConfigBuilder();
}
