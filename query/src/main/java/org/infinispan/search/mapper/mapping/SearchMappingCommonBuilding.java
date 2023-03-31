package org.infinispan.search.mapper.mapping;

import java.util.Collection;
import java.util.Map;

import org.hibernate.search.backend.lucene.work.spi.LuceneWorkExecutorProvider;
import org.hibernate.search.engine.environment.bean.BeanReference;
import org.hibernate.search.mapper.pojo.bridge.IdentifierBridge;
import org.hibernate.search.mapper.pojo.model.spi.PojoBootstrapIntrospector;
import org.infinispan.query.concurrent.InfinispanIndexingFailureHandler;
import org.infinispan.util.concurrent.BlockingManager;

/**
 * Stores some fields that could be useful to build a {@link SearchMappingBuilder} also at a later time.
 */
public class SearchMappingCommonBuilding {

   private final BeanReference<? extends IdentifierBridge<Object>> identifierBridge;
   private final Map<String, Object> properties;
   private final ClassLoader aggregatedClassLoader;
   private final Collection<ProgrammaticSearchMappingProvider> mappingProviders;
   private final BlockingManager blockingManager;
   private final LuceneWorkExecutorProvider luceneWorkExecutorProvider;
   private final Integer numberOfShards;

   public SearchMappingCommonBuilding(BeanReference<? extends IdentifierBridge<Object>> identifierBridge,
                                      Map<String, Object> properties, ClassLoader aggregatedClassLoader,
                                      Collection<ProgrammaticSearchMappingProvider> mappingProviders,
                                      BlockingManager blockingManager,
                                      LuceneWorkExecutorProvider luceneWorkExecutorProvider, Integer numberOfShards) {
      this.identifierBridge = identifierBridge;
      this.properties = properties;
      this.aggregatedClassLoader = aggregatedClassLoader;
      this.mappingProviders = mappingProviders;
      this.blockingManager = blockingManager;
      this.luceneWorkExecutorProvider = luceneWorkExecutorProvider;
      this.numberOfShards = numberOfShards;
   }

   public SearchMappingBuilder builder(PojoBootstrapIntrospector introspector) {
      InfinispanIndexingFailureHandler indexingFailureHandler = new InfinispanIndexingFailureHandler();

      SearchMappingBuilder builder = SearchMapping.builder(introspector, aggregatedClassLoader, mappingProviders,
                  blockingManager, indexingFailureHandler.failureCounter())
            .setProvidedIdentifierBridge(identifierBridge)
            .setProperties(properties)
            .setProperty("backend_work_executor_provider", luceneWorkExecutorProvider)
            .setProperty("hibernate.search.background_failure_handler", indexingFailureHandler);
      return builder;
   }
}
