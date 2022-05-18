package org.infinispan.query.backend;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.testng.Assert.assertEquals;

import java.util.Date;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.helper.SearchConfig;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Test for multiple entities types in the same cache sharing the same index
 *
 * @author gustavonalle
 * @since 7.1
 */
@Test(groups = "functional", testName = "query.backend.MultipleEntitiesTest")
public class MultipleEntitiesTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(false);
      cfg.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Bond.class)
            .addIndexedEntity(Debenture.class)
            .addProperty(SearchConfig.ERROR_HANDLER, StaticTestingErrorHandler.class.getName());
      return TestCacheManagerFactory.createCacheManager(QueryTestSCI.INSTANCE, cfg);
   }

   @Test
   public void testIndexAndQuery() {
      QueryFactory queryFactory = Search.getQueryFactory(cache);

      cache.put(123405, new Bond(new Date(System.currentTimeMillis()), 450L));

      cache.put(123502, new Debenture("GB", 116d));

      cache.put(223456, new Bond(new Date(System.currentTimeMillis()), 550L));

      Query<?> query = queryFactory.create("FROM " + Bond.class.getName());
      Query<?> query2 = queryFactory.create("FROM " + Debenture.class.getName());
      assertEquals(query.list().size() + query2.list().size(), 3);

      Query<?> queryBond = queryFactory.create("FROM " + Bond.class.getName());
      assertEquals(queryBond.execute().hitCount().orElse(-1), 2);

      Query<?> queryDeb = queryFactory.create("FROM " + Debenture.class.getName());
      assertEquals(queryDeb.execute().hitCount().orElse(-1), 1);
   }
}

@Indexed(index = "instruments")
class Bond {
   @Basic
   Date maturity;
   @Basic
   Long price;

   public Bond(Date maturity, Long price) {
      this.maturity = maturity;
      this.price = price;
   }
}

@Indexed(index = "instruments_")
class Debenture {

   @Basic
   String issuer;

   @Basic
   Double rate;

   public Debenture(String issuer, Double rate) {
      this.issuer = issuer;
      this.rate = rate;
   }
}
