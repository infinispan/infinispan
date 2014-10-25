package org.infinispan.query.helper;

import static org.junit.Assert.assertNotNull;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.hibernate.search.spi.SearchFactoryIntegrator;
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
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;

import java.util.ArrayList;
import java.util.List;

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
      return Version.LUCENE_4_10_1; //Change as needed
   }

   public static CacheQuery createCacheQuery(Cache m_cache, String fieldName, String searchString) throws ParseException {
      QueryParser qp = createQueryParser(fieldName);
      Query parsedQuery = qp.parse(searchString);
      SearchManager queryFactory = Search.getSearchManager(m_cache);
      CacheQuery cacheQuery = queryFactory.getQuery(parsedQuery);
      return cacheQuery;
   }
   
   public static SearchFactoryIntegrator extractSearchFactory(Cache cache) {
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      SearchFactoryIntegrator component = componentRegistry.getComponent(SearchFactoryIntegrator.class);
      assertNotNull(component);
      return component;
   }

   public static List createTopologyAwareCacheNodes(int numberOfNodes, CacheMode cacheMode, boolean transactional,
                                                    boolean indexLocalOnly, boolean isRamDirectoryProvider) {
      List caches = new ArrayList();

      ConfigurationBuilder builder = AbstractCacheTest.getDefaultClusteredCacheConfig(cacheMode, transactional);

      builder.indexing().index(indexLocalOnly ? Index.LOCAL : Index.ALL);

      if(isRamDirectoryProvider) {
         builder.indexing()
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler");
      } else {
         builder.indexing()
            .addProperty("default.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager")
            .addProperty("lucene_version", "LUCENE_CURRENT")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler");
         if (cacheMode.isClustered()) {
            builder.clustering().stateTransfer().fetchInMemoryState(true);
         }
      }

      for(int i = 0; i < numberOfNodes; i++) {
         GlobalConfigurationBuilder globalConfigurationBuilder = GlobalConfigurationBuilder
               .defaultClusteredBuilder();
         globalConfigurationBuilder.transport().machineId("a" + i).rackId("b" + i).siteId("test" + i);

         EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(
               globalConfigurationBuilder, builder);

         caches.add(cm1.getCache());
      }

      return caches;
   }
   
}
