package org.infinispan.query.helper;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

import org.infinispan.search.mapper.mapping.SearchMapping;
import org.infinispan.search.mapper.mapping.SearchMappingBuilder;
import org.infinispan.search.mapper.mapping.impl.DefaultAnalysisConfigurer;
import org.infinispan.search.mapper.model.impl.InfinispanBootstrapIntrospector;

public class SearchMappingHelper {

   private static final String BACKEND_PREFIX = "hibernate.search.backend";

   private SearchMappingHelper() {
   }

   public static SearchMapping createSearchMappingForTests(Class<?> ... types) {
      Map<String, Object> properties = new LinkedHashMap<>();
      properties.put(BACKEND_PREFIX + ".type", "lucene");
      properties.put(BACKEND_PREFIX + ".analysis.configurer", new DefaultAnalysisConfigurer());
      properties.put(SearchConfig.DIRECTORY_TYPE, SearchConfig.HEAP);
      properties.put(SearchConfig.THREAD_POOL_SIZE, "1");
      properties.put(SearchConfig.QUEUE_COUNT, "1");

      InfinispanBootstrapIntrospector introspector = SearchMappingBuilder.introspector(MethodHandles.lookup());

      // do not pass any entity loader nor identifier bridges
      return SearchMapping.builder(introspector, null, Collections.emptyList())
            .setProperties(properties)
            .addEntityTypes(new HashSet<>(Arrays.asList(types)))
            .build();
   }
}
