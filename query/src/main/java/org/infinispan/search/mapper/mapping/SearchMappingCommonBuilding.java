package org.infinispan.search.mapper.mapping;

import java.util.Collection;
import java.util.Map;

import org.hibernate.search.engine.environment.bean.BeanReference;
import org.hibernate.search.mapper.pojo.bridge.IdentifierBridge;
import org.hibernate.search.mapper.pojo.model.spi.PojoBootstrapIntrospector;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.NonBlockingManager;

/**
 * Stores some fields that could be useful to build a {@link SearchMappingBuilder} also at a later time.
 */
public class SearchMappingCommonBuilding {

   private final BeanReference<? extends IdentifierBridge<Object>> identifierBridge;
   private final Map<String, Object> properties;
   private final ClassLoader aggregatedClassLoader;
   private final Collection<ProgrammaticSearchMappingProvider> mappingProviders;
   private final BlockingManager blockingManager;
   private final NonBlockingManager nonBlockingManager;

   public SearchMappingCommonBuilding(BeanReference<? extends IdentifierBridge<Object>> identifierBridge,
                                      Map<String, Object> properties, ClassLoader aggregatedClassLoader,
                                      Collection<ProgrammaticSearchMappingProvider> mappingProviders,
                                      BlockingManager blockingManager, NonBlockingManager nonBlockingManager) {
      this.identifierBridge = identifierBridge;
      this.properties = properties;
      this.aggregatedClassLoader = aggregatedClassLoader;
      this.mappingProviders = mappingProviders;
      this.blockingManager = blockingManager;
      this.nonBlockingManager = nonBlockingManager;
   }

   public SearchMappingBuilder builder(PojoBootstrapIntrospector introspector) {
      return SearchMapping.builder(introspector, aggregatedClassLoader, mappingProviders,
                  blockingManager, nonBlockingManager)
            .setProvidedIdentifierBridge(identifierBridge)
            .setProperties(properties);
   }
}
