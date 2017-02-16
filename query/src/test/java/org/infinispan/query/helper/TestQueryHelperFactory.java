package org.infinispan.query.helper;

import static org.testng.AssertJUnit.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.queries.faceting.Car;
import org.infinispan.query.test.Person;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;

/**
 * Creates a test query helper
 *
 * @author Manik Surtani
 * @author Sanne Grinovero
 * @since 4.0
 */
public class TestQueryHelperFactory {

   public static final Analyzer STANDARD_ANALYZER = new StandardAnalyzer();

   public static QueryParser createQueryParser(String defaultFieldName) {
      return new QueryParser(defaultFieldName, STANDARD_ANALYZER);
   }

   public static Version getLuceneVersion() {
      return Version.LATEST; //Change as needed
   }

   public static <E> CacheQuery<E> createCacheQuery(Cache m_cache, String fieldName, String searchString) throws ParseException {
      QueryParser qp = createQueryParser(fieldName);
      Query parsedQuery = qp.parse(searchString);
      SearchManager queryFactory = Search.getSearchManager(m_cache);
      return queryFactory.getQuery(parsedQuery);
   }

   public static SearchIntegrator extractSearchFactory(Cache cache) {
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      SearchIntegrator component = componentRegistry.getComponent(SearchIntegrator.class);
      assertNotNull(component);
      return component;
   }

   public static List createTopologyAwareCacheNodes(int numberOfNodes, CacheMode cacheMode, boolean transactional,
                                                    boolean indexLocalOnly, boolean isRamDirectoryProvider, String defaultCacheName) {
      List caches = new ArrayList();

      ConfigurationBuilder builder = AbstractCacheTest.getDefaultClusteredCacheConfig(cacheMode, transactional);

      builder.indexing().index(indexLocalOnly ? Index.LOCAL : Index.ALL);

      if (isRamDirectoryProvider) {
         builder.indexing()
            .addIndexedEntity(Person.class)
            .addIndexedEntity(Car.class)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler");
      } else {
         builder.indexing()
            .addIndexedEntity(Person.class)
            .addIndexedEntity(Car.class)
            .addProperty("default.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager")
            .addProperty("lucene_version", "LUCENE_CURRENT")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler");
         if (cacheMode.isClustered()) {
            builder.clustering().stateTransfer().fetchInMemoryState(true);
         }
      }

      for (int i = 0; i < numberOfNodes; i++) {
         GlobalConfigurationBuilder globalConfigurationBuilder = GlobalConfigurationBuilder
               .defaultClusteredBuilder();
         globalConfigurationBuilder.transport().machineId("a" + i).rackId("b" + i).siteId("test" + i).defaultCacheName(defaultCacheName);

         EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(
               globalConfigurationBuilder, builder);

         caches.add(cm1.getCache());
      }

      return caches;
   }

}
