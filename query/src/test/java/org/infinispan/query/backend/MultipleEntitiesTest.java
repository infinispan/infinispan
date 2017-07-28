package org.infinispan.query.backend;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Date;

import org.apache.lucene.search.MatchAllDocsQuery;
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
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
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
      cfg.indexing().index(Index.ALL)
            .addIndexedEntity(Bond.class)
            .addIndexedEntity(Debenture.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @Test
   public void testIndexAndQuery() throws Exception {
      SearchManager searchManager = Search.getSearchManager(cache);

      cache.put(123405, new Bond(new Date(System.currentTimeMillis()), 450L));
      assertEfficientIndexingUsed(searchManager.unwrap(SearchIntegrator.class), Bond.class);

      cache.put(123502, new Debenture("GB", 116d));
      assertEfficientIndexingUsed(searchManager.unwrap(SearchIntegrator.class), Debenture.class);

      cache.put(223456, new Bond(new Date(System.currentTimeMillis()), 550L));
      assertEfficientIndexingUsed(searchManager.unwrap(SearchIntegrator.class), Bond.class);

      CacheQuery<?> query = searchManager.getQuery(new MatchAllDocsQuery(), Bond.class, Debenture.class);
      assertEquals(query.list().size(), 3);

      CacheQuery<?> queryBond = searchManager.getQuery(new MatchAllDocsQuery(), Bond.class);
      assertEquals(queryBond.getResultSize(), 2);

      CacheQuery<?> queryDeb = searchManager.getQuery(new MatchAllDocsQuery(), Debenture.class);
      assertEquals(queryDeb.getResultSize(), 1);
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
