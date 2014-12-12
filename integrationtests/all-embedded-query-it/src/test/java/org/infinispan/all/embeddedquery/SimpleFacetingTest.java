package org.infinispan.all.embeddedquery;

import org.apache.lucene.search.Query;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.hibernate.search.query.facet.Facet;
import org.hibernate.search.query.facet.FacetingRequest;
import org.infinispan.all.embeddedquery.testdomain.Car;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Copy of SimpleFacetingTest for testing uber-jars
 *
 * @author Jiri Holusa (jholusa@redhat.com)
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @author Hardy Ferentschik
 */
public class SimpleFacetingTest extends AbstractQueryTest {
   
   private static final String indexFieldName = "cubicCapacity";
   private static final String facetName = "ccs";

   private static SearchManager qf;

   @BeforeClass
   public static void prepareSearchFactory() throws Exception {
      cache = createCacheManager().getCache();
      qf = Search.getSearchManager(cache);
      cache.put( "195 Inter", new Car( "Ferrari 195 Inter", "Rosso corsa", 2341 ) );
      cache.put( "212 Inter", new Car( "Ferrari 212 Inter", "black", 4000 ) );
      cache.put( "500_Superfast", new Car( "Ferrari 500_Superfast", "Rosso corsa", 4000 ) );
      //test for duplication:
      cache.put( "500_Superfast", new Car( "Ferrari 500_Superfast", "Rosso corsa", 4000 ) );
   }
   
   @AfterClass
   public static void cleanupData() {
      cache.clear();
   }

   @Test
   public void testFaceting() throws Exception {
      QueryBuilder queryBuilder = qf.buildQueryBuilderForClass( Car.class ).get();
      
      FacetingRequest request = queryBuilder.facet()
            .name( facetName )
            .onField( indexFieldName )
            .discrete()
            .createFacetingRequest();
      
      Query luceneQuery = queryBuilder.all().createQuery();
      
      CacheQuery query = qf.getQuery(luceneQuery);

      query.getFacetManager().enableFaceting( request );

      List<Facet> facetList = query.getFacetManager().getFacets( facetName );
      
      assertEquals("Wrong number of facets", 2, facetList.size());
      
      assertEquals("4000", facetList.get(0).getValue());
      assertEquals(2, facetList.get(0).getCount());
      assertEquals(indexFieldName, facetList.get(0).getFieldName());
      
      assertEquals("2341", facetList.get(1).getValue());
      assertEquals(1, facetList.get(1).getCount());
      assertEquals(indexFieldName, facetList.get(1).getFieldName());
   }

}
