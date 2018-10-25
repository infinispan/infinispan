package org.infinispan.query.queries.faceting;

import static org.testng.AssertJUnit.assertEquals;

import java.util.List;

import org.apache.lucene.search.Query;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.hibernate.search.query.facet.Facet;
import org.hibernate.search.query.facet.FacetingRequest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 * @author Hardy Ferentschik
 */
@Test(groups = {"functional"}, testName = "query.queries.faceting.SimpleFacetingTest")
public class SimpleFacetingTest extends SingleCacheManagerTest {

   private static final String indexFieldName = "cubicCapacity";
   private static final String facetName = "ccs";

   private SearchManager qf;

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
         .indexing()
            .index(Index.ALL)
            .addIndexedEntity(Car.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @BeforeClass
   public void prepareSearchFactory() throws Exception {
      qf = Search.getSearchManager(cache);
      cache.put("195 Inter", new Car("Ferrari 195 Inter", "Rosso corsa", 2341));
      cache.put("212 Inter", new Car("Ferrari 212 Inter", "black", 4000));
      cache.put("500_Superfast", new Car("Ferrari 500_Superfast", "Rosso corsa", 4000));
      //test for duplication:
      cache.put("500_Superfast", new Car("Ferrari 500_Superfast", "Rosso corsa", 4000));
   }

   @AfterMethod
   public void cleanupData() {
      cache.clear();
   }

   public void testFaceting() {
      QueryBuilder queryBuilder = qf.buildQueryBuilderForClass(Car.class).get();

      FacetingRequest request = queryBuilder.facet()
            .name(facetName)
            .onField(indexFieldName)
            .discrete()
            .createFacetingRequest();

      Query luceneQuery = queryBuilder.all().createQuery();

      CacheQuery<?> query = qf.getQuery(luceneQuery);

      query.getFacetManager().enableFaceting(request);

      List<Facet> facetList = query.getFacetManager().getFacets(facetName);

      assertEquals("Wrong number of facets", 2, facetList.size());

      assertEquals("4000", facetList.get(0).getValue());
      assertEquals(2, facetList.get(0).getCount());
      assertEquals(indexFieldName, facetList.get(0).getFieldName());

      assertEquals("2341", facetList.get(1).getValue());
      assertEquals(1, facetList.get(1).getCount());
      assertEquals(indexFieldName, facetList.get(1).getFieldName());
   }

}
