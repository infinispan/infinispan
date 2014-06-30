package org.infinispan.query.dynamicexample;

import java.util.List;

import org.apache.lucene.search.Query;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2011 Red Hat Inc.
 */
@Test(groups = "functional", testName = "query.dynamicexample.DynamicPropertiesTest")
public class DynamicPropertiesTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
         .transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
         .indexing()
            .index(Index.ALL)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @Test
   public void searchOnEmptyIndex() {
      cache.put("1",
            new DynamicPropertiesEntity()
               .set("name", "OpenBlend 2011")
               .set("city", "Ljubljana")
               .set("location", "castle")
               );
      cache.put("2",
            new DynamicPropertiesEntity()
               .set("name", "JUDCon London 2011")
               .set("city", "London")
               );
      cache.put("3",
            new DynamicPropertiesEntity()
               .set("name", "JavaOne 2011")
               .set("city", "San Francisco")
               .set("awards", "Duke Award to Arquillian")
               );
      SearchManager qf = Search.getSearchManager(cache);
      QueryBuilder queryBuilder = qf.buildQueryBuilderForClass(DynamicPropertiesEntity.class).get();

      // Searching for a specific entity:
      Query query = queryBuilder
            .phrase()
               .onField("city")
               .sentence("London")
            .createQuery();

      List list = qf.getQuery(query).list();
      assert list.size() == 1;
      DynamicPropertiesEntity result = (DynamicPropertiesEntity) list.get(0);
      assert result.getProperties().get("name").equals("JUDCon London 2011");

      // Search for all of them:
      Query dateQuery = queryBuilder
            .phrase()
               .onField("name")
               .sentence("2011")
            .createQuery();

      list = qf.getQuery(dateQuery).list();
      assert list.size() == 3;

      // Now search for a property define on a single entity only:
      Query awardsQuery = queryBuilder
            .phrase()
               .onField("awards")
               .sentence("Duke")
            .createQuery();

      list = qf.getQuery(awardsQuery).list();
      assert list.size() == 1;
      result = (DynamicPropertiesEntity) list.get(0);
      assert result.getProperties().get("city").equals("San Francisco");
   }

}
