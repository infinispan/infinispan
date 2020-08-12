package org.infinispan.search.mapper.mapping;

import java.util.Collection;
import java.util.Map;

import org.hibernate.search.engine.environment.bean.BeanReference;
import org.hibernate.search.mapper.pojo.bridge.IdentifierBridge;
import org.hibernate.search.mapper.pojo.model.spi.PojoBootstrapIntrospector;

/**
 * Stores some fields that could be useful to build a {@link SearchMappingBuilder} also at a later time.
 */
public class SearchMappingCommonBuilding {

   private final BeanReference<? extends IdentifierBridge<Object>> identifierBridge;
   private final Map<String, Object> properties;
   private final ClassLoader aggregatedClassLoader;
   private final Collection<ProgrammaticSearchMappingProvider> mappingProviders;

   public SearchMappingCommonBuilding(BeanReference<? extends IdentifierBridge<Object>> identifierBridge,
                                      Map<String, Object> properties, ClassLoader aggregatedClassLoader,
                                      Collection<ProgrammaticSearchMappingProvider> mappingProviders) {
      this.identifierBridge = identifierBridge;
      this.properties = properties;
      this.aggregatedClassLoader = aggregatedClassLoader;
      this.mappingProviders = mappingProviders;
   }

   public SearchMappingBuilder builder(PojoBootstrapIntrospector introspector) {
      return SearchMapping.builder(introspector, aggregatedClassLoader, mappingProviders)
            .setProvidedIdentifierBridge(identifierBridge)
            .setProperties(properties);
   }
}
