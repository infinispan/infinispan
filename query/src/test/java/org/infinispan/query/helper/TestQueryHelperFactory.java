package org.infinispan.query.helper;

import static org.testng.AssertJUnit.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.test.CustomKey3;
import org.infinispan.query.test.CustomKey3Transformer;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.search.mapper.mapping.SearchMapping;
import org.infinispan.search.mapper.mapping.SearchMappingHolder;
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

   public static <E> Query<E> createCacheQuery(Class<?> entity, Cache<?, ?> cache, String fieldName, String searchString) {
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      String q = String.format("FROM %s WHERE %s:'%s'", entity.getName(), fieldName, searchString);
      return queryFactory.create(q);
   }

   public static <T> List<T> queryAll(Cache<?, ?> cache, Class<T> entityType) {
      Query<T> query = Search.getQueryFactory(cache).create("FROM " + entityType.getName());
      return query.execute().list();
   }

   public static SearchMapping extractSearchMapping(Cache<?, ?> cache) {
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      SearchMappingHolder component = componentRegistry.getComponent(SearchMappingHolder.class);
      assertNotNull(component);
      SearchMapping searchMapping = component.getSearchMapping();
      assertNotNull(searchMapping);
      return searchMapping;
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
               .addProperty(SearchConfig.DIRECTORY_TYPE, SearchConfig.HEAP)
               .addProperty(SearchConfig.ERROR_HANDLER, StaticTestingErrorHandler.class.getName());
      } else {
         builder.indexing()
               .addProperty(SearchConfig.DIRECTORY_TYPE, SearchConfig.HEAP)
               .addProperty(SearchConfig.ERROR_HANDLER, StaticTestingErrorHandler.class.getName());
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
