package org.infinispan.query.backend;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Date;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.backend.IndexWorkVisitor;
import org.hibernate.search.backend.impl.lucene.LuceneBackendResources;
import org.hibernate.search.backend.impl.lucene.WorkspaceHolder;
import org.hibernate.search.backend.impl.lucene.works.ByTermUpdateWorkExecutor;
import org.hibernate.search.backend.impl.lucene.works.LuceneWorkExecutor;
import org.hibernate.search.indexes.spi.DirectoryBasedIndexManager;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
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
            .addIndexedEntity(Bond.class)
            .addIndexedEntity(Debenture.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("error_handler", StaticTestingErrorHandler.class.getName())
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(QueryTestSCI.INSTANCE, cfg);
   }

   @Test
   public void testIndexAndQuery() {
      QueryFactory queryFactory = Search.getQueryFactory(cache);

      SearchIntegrator searchIntegrator = TestingUtil.extractComponent(cache, SearchIntegrator.class);

      cache.put(123405, new Bond(new Date(System.currentTimeMillis()), 450L));
      assertEfficientIndexingUsed(searchIntegrator, Bond.class);

      cache.put(123502, new Debenture("GB", 116d));
      assertEfficientIndexingUsed(searchIntegrator, Debenture.class);

      cache.put(223456, new Bond(new Date(System.currentTimeMillis()), 550L));
      assertEfficientIndexingUsed(searchIntegrator, Bond.class);

      Query<?> query = queryFactory.create("FROM " + Bond.class.getName());
      Query<?> query2 = queryFactory.create("FROM " + Debenture.class.getName());
      assertEquals(query.list().size() + query2.list().size(), 3);

      Query<?> queryBond = queryFactory.create("FROM " + Bond.class.getName());
      assertEquals(queryBond.execute().hitCount().orElse(-1), 2);

      Query<?> queryDeb = queryFactory.create("FROM " + Debenture.class.getName());
      assertEquals(queryDeb.execute().hitCount().orElse(-1), 1);
   }

   private void assertEfficientIndexingUsed(SearchIntegrator searchIntegrator, Class<?> clazz) {
      DirectoryBasedIndexManager im = (DirectoryBasedIndexManager) searchIntegrator.getIndexBindings().get(clazz)
            .getIndexManagerSelector().all().iterator().next();
      WorkspaceHolder workspaceHolder = im.getWorkspaceHolder();
      LuceneBackendResources indexResources = workspaceHolder.getIndexResources();
      IndexWorkVisitor<Void, LuceneWorkExecutor> visitor = indexResources.getWorkVisitor();
      assertTrue(TestingUtil.extractField(visitor, "updateExecutor") instanceof ByTermUpdateWorkExecutor);
   }
}

@Indexed(index = "instruments")
class Bond {
   @Field
   Date maturity;
   @Field
   Long price;

   public Bond(Date maturity, Long price) {
      this.maturity = maturity;
      this.price = price;
   }
}

@Indexed(index = "instruments")
class Debenture {

   @Field
   String issuer;

   @Field
   Double rate;

   public Debenture(String issuer, Double rate) {
      this.issuer = issuer;
      this.rate = rate;
   }
}
