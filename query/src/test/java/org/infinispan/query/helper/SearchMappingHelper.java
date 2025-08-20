package org.infinispan.query.helper;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.infinispan.AdvancedCache;
import org.infinispan.query.concurrent.FailureCounter;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.query.impl.EntityLoaderFactory;
import org.infinispan.query.impl.IndexerConfig;
import org.infinispan.query.mapper.mapping.SearchMapping;
import org.infinispan.query.mapper.mapping.SearchMappingBuilder;
import org.infinispan.query.mapper.mapping.impl.DefaultAnalysisConfigurer;
import org.infinispan.query.mapper.model.impl.InfinispanBootstrapIntrospector;
import org.infinispan.util.concurrent.BlockingManager;
import org.mockito.Mockito;

public class SearchMappingHelper {

   private static final String BACKEND_PREFIX = "hibernate.search.backend";

   private SearchMappingHelper() {
   }

   public static SearchMapping createSearchMappingForTests(BlockingManager blockingManager, Class<?>... types) {
      Map<String, Object> properties = new LinkedHashMap<>();
      properties.put(BACKEND_PREFIX + ".analysis.configurer", new DefaultAnalysisConfigurer());
      properties.put("directory.type", "local-heap");

      InfinispanBootstrapIntrospector introspector = SearchMappingBuilder.introspector(MethodHandles.lookup());

      // do not pass any identifier bridges and mock the cache and query statistics: those guys won't be used
      AdvancedCache<?, ?> cache = Mockito.mock(AdvancedCache.class);
      LocalQueryStatistics queryStatistics = Mockito.mock(LocalQueryStatistics.class);
      return SearchMapping.builder(introspector, null, Collections.emptyList(), blockingManager, new FailureCounter(),
                  new IndexerConfig(10_000))
                   .setProperties(properties)
                   .addEntityTypes(new HashSet<>(Arrays.asList(types)))
                   .setEntityLoader(new EntityLoaderFactory<>(cache, queryStatistics))
                   .build(Optional.empty());
   }
}
