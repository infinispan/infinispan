package org.infinispan.rest.search;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM_TYPE;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.stream.LongStream;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexStorage;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests for the query "total_results" for all types of query.
 *
 * @since 12.1
 */
@Test(groups = "functional", testName = "rest.search.SearchCountClusteredTest")
public class SearchCountClusteredTest extends MultiNodeRestTest {

   static final int INDEXED_ENTRIES = 300;
   private static final int NOT_INDEXED_ENTRIES = 200;
   static final String INDEXED_CACHE = "indexed";
   static final String NOT_INDEXED_CACHE = "not-indexed";
   public static final int DEFAULT_PAGE_SIZE = 10;

   @Override
   int getMembers() {
      return 3;
   }

   protected CacheMode getCacheMode() {
      return CacheMode.DIST_SYNC;
   }

   @Override
   protected Map<String, ConfigurationBuilder> getCacheConfigs() {
      Map<String, ConfigurationBuilder> caches = new HashMap<>();

      final ConfigurationBuilder indexedCache = new ConfigurationBuilder();
      final CacheMode cacheMode = getCacheMode();
      if (cacheMode.isClustered()) {
         indexedCache.clustering().cacheMode(cacheMode);
      }
      indexedCache.statistics().enable().indexing().enable().addIndexedEntity("IndexedEntity").storage(IndexStorage.LOCAL_HEAP);
      indexedCache.encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);
      caches.put(INDEXED_CACHE, indexedCache);

      final ConfigurationBuilder notIndexedCache = new ConfigurationBuilder();
      if (cacheMode.isClustered()) {
         notIndexedCache.clustering().cacheMode(cacheMode);
      }
      notIndexedCache.encoding().mediaType(APPLICATION_PROTOSTREAM_TYPE);
      caches.put(NOT_INDEXED_CACHE, notIndexedCache);

      return caches;
   }

   @Override
   protected String getProtoFile() {
      return "count.proto";
   }

   @BeforeClass
   public void setUp() {
      LongStream.range(0, INDEXED_ENTRIES).forEach(i -> {
         String str = "index " + i;
         String value = Json.object()
               .set("_type", "IndexedEntity")
               .set("indexedStoredField", str)
               .set("indexedNotStoredField", str)
               .set("sortableStoredField", i % 20)
               .set("sortableNotStoredField", "index_" + i % 20)
               .set("notIndexedField", str).toString();
         RestResponse response = await(indexedCache().put(String.valueOf(i), RestEntity.create(APPLICATION_JSON, value)));
         assertEquals(204, response.getStatus());
         assertTrue(response.getBody().isEmpty());
      });

      LongStream.range(0, NOT_INDEXED_ENTRIES).forEach(i -> {
         String value = "text " + i;
         Json json = Json.object().set("_type", "NotIndexedEntity").set("field1", value).set("field2", value);
         RestResponse response =
               await(nonIndexedCache().put(String.valueOf(i), RestEntity.create(APPLICATION_JSON, json.toString())));
         assertEquals(204, response.getStatus());
         assertTrue(response.getBody().isEmpty());
      });
   }

   /**
    * Pure indexed queries
    */
   @Test
   public void testMatchAll() {
      CompletionStage<RestResponse> result = queryWithoutPagination(indexedCache(), "FROM IndexedEntity");
      assertTotalAndPageSize(result, INDEXED_ENTRIES, INDEXED_ENTRIES);
   }

   @Test
   public void testMatchAllPagination() {
      CompletionStage<RestResponse> result = queryWithPagination(indexedCache(), "FROM IndexedEntity", 17, 0);
      assertTotalAndPageSize(result, INDEXED_ENTRIES, 17);
   }

   @Test
   public void testLimit() {
      CompletionStage<RestResponse> result = queryWithoutPagination(indexedCache(), "FROM IndexedEntity");
      assertTotalAndPageSize(result, INDEXED_ENTRIES, INDEXED_ENTRIES);
   }

   @Test
   public void testLimitPagination() {
      CompletionStage<RestResponse> result = queryWithDefaultPagination(indexedCache(), "FROM IndexedEntity");
      assertTotalAndPageSize(result, INDEXED_ENTRIES, DEFAULT_PAGE_SIZE);
   }

   @Test
   public void testSortedStored() {
      CompletionStage<RestResponse> result = queryWithoutPagination(indexedCache(), "FROM IndexedEntity ORDER BY sortableStoredField DESC");
      assertTotalAndPageSize(result, INDEXED_ENTRIES, INDEXED_ENTRIES);
   }

   @Test
   public void testSortedStoredPagination() {
      CompletionStage<RestResponse> result = queryWithPagination(indexedCache(), "SELECT sortableStoredField FROM IndexedEntity ORDER BY sortableStoredField DESC", 35, 0);
      assertTotalAndPageSize(result, INDEXED_ENTRIES, 35);
   }

   @Test
   public void testSelectIndexed() {
      CompletionStage<RestResponse> result = queryWithoutPagination(indexedCache(), "SELECT indexedStoredField FROM IndexedEntity");
      assertTotalAndPageSize(result, INDEXED_ENTRIES, INDEXED_ENTRIES);
   }

   @Test
   public void testSelectIndexedPagination() {
      CompletionStage<RestResponse> result = queryWithDefaultPagination(indexedCache(), "SELECT indexedStoredField FROM IndexedEntity");
      assertTotalAndPageSize(result, INDEXED_ENTRIES, DEFAULT_PAGE_SIZE);
   }

   @Test
   public void testPagination() {
      CompletionStage<RestResponse> result = queryWithPagination(indexedCache(), "SELECT indexedStoredField FROM IndexedEntity", 8, 2);
      assertTotalAndPageSize(result, INDEXED_ENTRIES, 8);
   }

   @Test
   public void testAggregation() {
      CompletionStage<RestResponse> result = queryWithoutPagination(indexedCache(), "SELECT count(indexedStoredField) FROM IndexedEntity");
      int indexedStoredField = getFieldAggregationValue(result, "indexedStoredField");
      assertEquals(INDEXED_ENTRIES, indexedStoredField);
   }

   @Test
   public void testGrouping() {
      CompletionStage<RestResponse> result = queryWithoutPagination(indexedCache(), "SELECT count(sortableStoredField) FROM IndexedEntity GROUP BY sortableStoredField");
      assertTotalAndPageSize(result, 20, 20);
   }

   @Test
   public void testGroupingPagination() {
      CompletionStage<RestResponse> result = queryWithDefaultPagination(indexedCache(), "SELECT count(sortableStoredField) FROM IndexedEntity GROUP BY sortableStoredField");
      assertTotalAndPageSize(result, 20, 10);
   }

   @Test
   public void testCountOnly() {
      CompletionStage<RestResponse> result = queryWithPagination(indexedCache(), "FROM IndexedEntity", 0, 0);
      assertTotalAndPageSize(result, INDEXED_ENTRIES, 0);
   }

   /**
    * Hybrid queries
    */
   @Test
   public void testSortedNotStored() {
      CompletionStage<RestResponse> result = queryWithoutPagination(indexedCache(), "FROM IndexedEntity ORDER BY sortableNotStoredField DESC");
      assertTotalAndPageSize(result, INDEXED_ENTRIES, INDEXED_ENTRIES);
   }

   @Test
   public void testSortedNotStoredPagination() {
      CompletionStage<RestResponse> result = queryWithDefaultPagination(indexedCache(), "FROM IndexedEntity ORDER BY sortableNotStoredField DESC");
      assertTotalAndPageSize(result, INDEXED_ENTRIES, DEFAULT_PAGE_SIZE);
   }

   @Test
   public void testSelectNonStoredField() {
      CompletionStage<RestResponse> result = queryWithoutPagination(indexedCache(), "SELECT indexedNotStoredField FROM IndexedEntity");
      assertTotalAndPageSize(result, INDEXED_ENTRIES, INDEXED_ENTRIES);
   }

   @Test
   public void testSelectNonStoredFieldPagination() {
      CompletionStage<RestResponse> result = queryWithDefaultPagination(indexedCache(), "SELECT indexedNotStoredField FROM IndexedEntity");
      assertTotalAndPageSize(result, INDEXED_ENTRIES, DEFAULT_PAGE_SIZE);
   }

   @Test
   public void testSelectNotIndexedField() {
      CompletionStage<RestResponse> result = queryWithoutPagination(indexedCache(), "SELECT notIndexedField FROM IndexedEntity");
      assertTotalAndPageSize(result, INDEXED_ENTRIES, INDEXED_ENTRIES);
   }

   @Test
   public void testSelectNotIndexedFieldPagination() {
      CompletionStage<RestResponse> result = queryWithDefaultPagination(indexedCache(), "SELECT notIndexedField FROM IndexedEntity");
      assertTotalAndPageSize(result, INDEXED_ENTRIES, DEFAULT_PAGE_SIZE);
   }

   @Test
   public void testHybridPaginated() {
      CompletionStage<RestResponse> result = queryWithDefaultPagination(indexedCache(), "SELECT notIndexedField FROM IndexedEntity WHERE indexedStoredField : 'index'");
      assertTotalAndPageSize(result, INDEXED_ENTRIES, DEFAULT_PAGE_SIZE);
   }

   @Test
   public void testHybridNonPaginated() {
      CompletionStage<RestResponse> result = queryWithoutPagination(indexedCache(), "SELECT notIndexedField FROM IndexedEntity WHERE indexedStoredField : 'index'");
      assertTotalAndPageSize(result, INDEXED_ENTRIES, INDEXED_ENTRIES);
   }

   @Test
   public void testAggregationHybrid() {
      CompletionStage<RestResponse> result = queryWithoutPagination(indexedCache(), "SELECT count(indexedStoredField), max(notIndexedField) FROM IndexedEntity");
      int indexedStoredField = getFieldAggregationValue(result, "indexedStoredField");
      assertEquals(INDEXED_ENTRIES, indexedStoredField);
   }

   @Test
   public void testAggregationHybridPagination() {
      CompletionStage<RestResponse> result = queryWithDefaultPagination(indexedCache(), "SELECT count(indexedStoredField), max(notIndexedField) FROM IndexedEntity");
      int indexedStoredField = getFieldAggregationValue(result, "indexedStoredField");
      assertEquals(INDEXED_ENTRIES, indexedStoredField);
   }

   @Test
   public void testCountOnlyHybrid() {
      CompletionStage<RestResponse> result = queryWithPagination(indexedCache(), "SELECT notIndexedField FROM IndexedEntity", 0, 0);
      assertTotalAndPageSize(result, INDEXED_ENTRIES, 0);
   }

   /**
    * Non-indexed queries
    */
   @Test
   public void testMatchAllNotIndexed() {
      CompletionStage<RestResponse> result = queryWithoutPagination(nonIndexedCache(), "FROM NotIndexedEntity");
      assertTotalAndPageSize(result, NOT_INDEXED_ENTRIES, NOT_INDEXED_ENTRIES);
   }

   public void testMatchAllNotIndexedPaginated() {
      CompletionStage<RestResponse> result = queryWithDefaultPagination(nonIndexedCache(), "FROM NotIndexedEntity");
      assertTotalAndPageSize(result, NOT_INDEXED_ENTRIES, DEFAULT_PAGE_SIZE);
   }

   @Test
   public void testMaxResultsNotIndexedPaginated() {
      CompletionStage<RestResponse> result = queryWithPagination(nonIndexedCache(), "FROM NotIndexedEntity", 5, 1);
      assertTotalAndPageSize(result, NOT_INDEXED_ENTRIES, 5);
   }

   @Test
   public void testMaxResultsNotIndexed() {
      CompletionStage<RestResponse> result = queryWithoutPagination(nonIndexedCache(), "FROM NotIndexedEntity");
      assertTotalAndPageSize(result, NOT_INDEXED_ENTRIES, NOT_INDEXED_ENTRIES);
   }

   @Test
   public void testSortedNotIndexed() {
      CompletionStage<RestResponse> result = queryWithoutPagination(nonIndexedCache(), "FROM NotIndexedEntity ORDER BY field2");
      assertTotalAndPageSize(result, NOT_INDEXED_ENTRIES, NOT_INDEXED_ENTRIES);
   }

   @Test
   public void testSortedNotIndexedPaginated() {
      CompletionStage<RestResponse> result = queryWithDefaultPagination(nonIndexedCache(), "FROM NotIndexedEntity ORDER BY field2");
      assertTotalAndPageSize(result, NOT_INDEXED_ENTRIES, DEFAULT_PAGE_SIZE);
   }

   @Test
   public void testPaginatedNotIndexed() {
      CompletionStage<RestResponse> result = queryWithPagination(nonIndexedCache(), "SELECT field1 FROM NotIndexedEntity", 5, 2);
      assertTotalAndPageSize(result, NOT_INDEXED_ENTRIES, 5);
   }

   @Test
   public void testSelectNotIndexed() {
      CompletionStage<RestResponse> result = queryWithDefaultPagination(nonIndexedCache(), "SELECT field1 FROM NotIndexedEntity");
      assertTotalAndPageSize(result, NOT_INDEXED_ENTRIES, DEFAULT_PAGE_SIZE);
   }

   @Test
   public void testSelectNotIndexedPaginated() {
      CompletionStage<RestResponse> result = queryWithDefaultPagination(nonIndexedCache(), "SELECT field1 FROM NotIndexedEntity");
      assertTotalAndPageSize(result, NOT_INDEXED_ENTRIES, DEFAULT_PAGE_SIZE);
   }

   @Test
   public void testAggregationNotIndexed() {
      CompletionStage<RestResponse> result = queryWithoutPagination(nonIndexedCache(), "SELECT count(field1), max(field2) FROM NotIndexedEntity");
      int field1Count = getFieldAggregationValue(result, "field1");
      assertEquals(NOT_INDEXED_ENTRIES, field1Count);
   }

   @Test
   public void testAggregationNotIndexedPagination() {
      CompletionStage<RestResponse> result = queryWithDefaultPagination(nonIndexedCache(), "SELECT count(field1), max(field2) FROM NotIndexedEntity");
      int field1Count = getFieldAggregationValue(result, "field1");
      assertEquals(NOT_INDEXED_ENTRIES, field1Count);
   }

   @Test
   public void testCountOnlyNotIndexed() {
      CompletionStage<RestResponse> result = queryWithPagination(nonIndexedCache(), "SELECT field1 FROM NotIndexedEntity", 0, 0);
      assertTotalAndPageSize(result, NOT_INDEXED_ENTRIES, 0);
   }

   private void assertTotalAndPageSize(CompletionStage<RestResponse> response, int totalResults, int pageSize) {
      RestResponse restResponse = await(response);
      String body = restResponse.getBody();
      Json responseDoc = Json.read(body);
      Json total = responseDoc.at("total_results");
      assertEquals(totalResults, total.asLong());
      long hitsSize = responseDoc.at("hits").asJsonList().size();
      assertEquals(pageSize, hitsSize);
   }

   private CompletionStage<RestResponse> queryWithoutPagination(RestCacheClient client, String query) {
      // don't do that is very inefficient (see ISPN-14194)
      return client.query(query, Integer.MAX_VALUE, 0);
   }

   private CompletionStage<RestResponse> queryWithDefaultPagination(RestCacheClient client, String query) {
      return client.query(query);
   }

   private CompletionStage<RestResponse> queryWithPagination(RestCacheClient client, String query, int maxResults, int offset) {
      return client.query(query, maxResults, offset);
   }

   private int getFieldAggregationValue(CompletionStage<RestResponse> response, String field) {
      RestResponse restResponse = await(response);
      String body = restResponse.getBody();
      return Json.read(body).at("hits").asJsonList().get(0).at("hit").at(field).asInteger();
   }

   private RestCacheClient indexedCache() {
      return cacheClients.get(INDEXED_CACHE);
   }

   private RestCacheClient nonIndexedCache() {
      return cacheClients.get(NOT_INDEXED_CACHE);
   }
}
