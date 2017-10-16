package org.infinispan.all.embeddedquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.QueryBuilder;
import org.hibernate.search.spi.IndexedTypeSet;
import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.spi.impl.IndexedTypeSets;
import org.infinispan.Cache;
import org.infinispan.all.embeddedquery.testdomain.Car;
import org.infinispan.all.embeddedquery.testdomain.NumericType;
import org.infinispan.all.embeddedquery.testdomain.Person;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.transaction.TransactionMode;

/**
 * Abstract class for query tests for uber jars.
 *
 * @author Jiri Holusa (jholusa@redhat.com)
 */
public abstract class AbstractQueryTest {

   protected static Cache<Object, Object> cache;

   protected static final Analyzer STANDARD_ANALYZER = new StandardAnalyzer();

   protected static EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder gcfg = new GlobalConfigurationBuilder();
      gcfg.globalJmxStatistics().allowDuplicateDomains(true);

      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.transaction()
          .transactionMode(TransactionMode.TRANSACTIONAL)
          .indexing()
          .index(Index.ALL)
          .addIndexedEntity(NumericType.class)
          .addIndexedEntity(Person.class)
          .addIndexedEntity(Car.class)
          .addProperty("default.directory_provider", "local-heap")
          .addProperty("error_handler", "org.infinispan.all.embeddedquery.testdomain.StaticTestingErrorHandler")
          .addProperty("lucene_version", "LUCENE_CURRENT");

      EmbeddedCacheManager cm = new DefaultCacheManager(gcfg.build(), cfg.build());

      return cm;
   }

   protected <E> CacheQuery<E> createCacheQuery(Cache m_cache, String fieldName, String searchString) {
      QueryBuilder queryBuilder = new QueryBuilder(STANDARD_ANALYZER);
      Query query = queryBuilder.createBooleanQuery(fieldName, searchString);
      SearchManager queryFactory = Search.getSearchManager(m_cache);
      return queryFactory.getQuery(query);
   }

   protected void assertIndexingKnows(BasicCache<?, ?> cache, Class<?>... types) {
      ComponentRegistry cr = ((Cache) cache).getAdvancedCache().getComponentRegistry();
      SearchIntegrator searchIntegrator = cr.getComponent(SearchIntegrator.class);
      assertNotNull(searchIntegrator);
      IndexedTypeSet expectedTypes = IndexedTypeSets.fromClasses(types);
      IndexedTypeSet indexedTypes = searchIntegrator.getIndexBindings().keySet();
      assertEquals(expectedTypes, indexedTypes);
   }
}
