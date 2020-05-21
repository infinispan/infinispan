package org.infinispan.query.helper;

import static org.testng.AssertJUnit.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.test.CustomKey3;
import org.infinispan.query.test.CustomKey3Transformer;
import org.infinispan.query.test.QueryTestSCI;
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

   public static <E> CacheQuery<E> createCacheQuery(Class<?> entity, Cache<?, ?> cache, String fieldName, String searchString) {
      SearchManager queryFactory = Search.getSearchManager(cache);
      String q = String.format("FROM %s WHERE %s:'%s'", entity.getName(), fieldName, searchString);
      return queryFactory.getQuery(q);
   }

   public static <T> List<T> queryAll(Cache<?, ?> cache, Class<T> entityType) {
      Query query = Search.getQueryFactory(cache).create("FROM " + entityType.getName());
      return query.list();
   }

   public static SearchIntegrator extractSearchFactory(Cache<?, ?> cache) {
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      SearchIntegrator component = componentRegistry.getComponent(SearchIntegrator.class);
      assertNotNull(component);
      return component;
   }

   public static List<EmbeddedCacheManager> createTopologyAwareCacheNodes(int numberOfNodes, CacheMode cacheMode, boolean transactional,
                                                                          boolean indexLocalOnly, boolean isRamDirectoryProvider, String defaultCacheName, Class<?>... indexedTypes) {
      return createTopologyAwareCacheNodes(numberOfNodes, cacheMode, transactional, indexLocalOnly,
            isRamDirectoryProvider, defaultCacheName, f -> {
            }, indexedTypes);
   }

   public static List<EmbeddedCacheManager> createTopologyAwareCacheNodes(int numberOfNodes, CacheMode cacheMode, boolean transactional,
                                                                          boolean indexLocalOnly, boolean isRamDirectoryProvider, String defaultCacheName,
                                                                          Consumer<ConfigurationBuilderHolder> holderConsumer, Class<?>... indexedTypes) {
      List<EmbeddedCacheManager> managers = new ArrayList<>();

      ConfigurationBuilder builder = AbstractCacheTest.getDefaultClusteredCacheConfig(cacheMode, transactional);

      builder.indexing().enable().addKeyTransformer(CustomKey3.class, CustomKey3Transformer.class);

      if (isRamDirectoryProvider) {
         builder.indexing()
               .addProperty("default.directory_provider", "local-heap")
               .addProperty("lucene_version", "LUCENE_CURRENT")
               .addProperty("error_handler", StaticTestingErrorHandler.class.getName());
      } else {
         builder.indexing()
               .addProperty("default.directory_provider", "local-heap")
               .addProperty("lucene_version", "LUCENE_CURRENT")
               .addProperty("error_handler", StaticTestingErrorHandler.class.getName());
         if (cacheMode.isClustered()) {
            builder.clustering().stateTransfer().fetchInMemoryState(true);
         }
      }
      for (Class<?> indexedType : indexedTypes) {
         builder.indexing().addIndexedEntity(indexedType);
      }

      for (int i = 0; i < numberOfNodes; i++) {
         ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
         GlobalConfigurationBuilder globalConfigurationBuilder = holder.getGlobalConfigurationBuilder().clusteredDefault();
         globalConfigurationBuilder.transport().machineId("a" + i).rackId("b" + i).siteId("test" + i).defaultCacheName(defaultCacheName);
         globalConfigurationBuilder.serialization().addContextInitializer(QueryTestSCI.INSTANCE);

         holderConsumer.accept(holder);
         holder.newConfigurationBuilder(defaultCacheName).read(builder.build());

         EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(holder);

         managers.add(cm);
      }

      return managers;
   }

}
