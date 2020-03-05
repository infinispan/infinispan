package org.infinispan.query.dynamicexample;

import java.util.List;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * @author Sanne Grinovero &lt;sanne@infinispan.org&gt; (C) 2011 Red Hat Inc.
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
            .enable()
            .addIndexedEntity(DynamicPropertiesEntity.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @Test(enabled = false, description = "Ickle currently does not support dynamic fields, see ISPN-11222")
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

      // Searching for a specific entity:
      String query = String.format("FROM %s WHERE city:'London'", DynamicPropertiesEntity.class.getName());
      List<?> list = qf.getQuery(query).list();
      assert list.size() == 1;
      DynamicPropertiesEntity result = (DynamicPropertiesEntity) list.get(0);
      assert result.getProperties().get("name").equals("JUDCon London 2011");

      // Search for all of them:
      String dateQuery = String.format("FROM %s WHERE name:'2011'", DynamicPropertiesEntity.class.getName());

      list = qf.getQuery(dateQuery).list();
      assert list.size() == 3;

      // Now search for a property define on a single entity only:
      String awardsQuery = String.format("FROM %s WHERE awards:'Duke'", DynamicPropertiesEntity.class.getName());

      list = qf.getQuery(awardsQuery).list();
      assert list.size() == 1;
      result = (DynamicPropertiesEntity) list.get(0);
      assert result.getProperties().get("city").equals("San Francisco");
   }

}
