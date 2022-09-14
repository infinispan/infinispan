package org.infinispan.query.helper;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.infinispan.search.mapper.mapping.SearchMapping;
import org.infinispan.search.mapper.mapping.SearchMappingBuilder;
import org.infinispan.search.mapper.mapping.impl.DefaultAnalysisConfigurer;
import org.infinispan.search.mapper.model.impl.InfinispanBootstrapIntrospector;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.NonBlockingManager;

public class SearchMappingHelper {

   private static final String BACKEND_PREFIX = "hibernate.search.backend";

   private SearchMappingHelper() {
   }

   public static SearchMapping createSearchMappingForTests(BlockingManager blockingManager,
                                                           NonBlockingManager nonBlockingManager, Class<?>... types) {
      Map<String, Object> properties = new LinkedHashMap<>();
      properties.put(BACKEND_PREFIX + ".analysis.configurer", new DefaultAnalysisConfigurer());
      properties.put("directory.type", "local-heap");

      InfinispanBootstrapIntrospector introspector = SearchMappingBuilder.introspector(MethodHandles.lookup());

      // do not pass any entity loader nor identifier bridges
      return SearchMapping.builder(introspector, null, Collections.emptyList(), blockingManager, nonBlockingManager)
                   .setProperties(properties)
                   .addEntityTypes(new HashSet<>(Arrays.asList(types)))
                   .build(Optional.empty());
   }
}
