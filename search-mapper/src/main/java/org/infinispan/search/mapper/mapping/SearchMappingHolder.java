package org.infinispan.search.mapper.mapping;

import java.util.Collection;
import java.util.Map;

import org.hibernate.search.engine.environment.bean.BeanReference;
import org.hibernate.search.engine.search.loading.spi.EntityLoader;
import org.hibernate.search.mapper.pojo.bridge.IdentifierBridge;
import org.hibernate.search.mapper.pojo.model.spi.PojoBootstrapIntrospector;
import org.infinispan.search.mapper.common.EntityReference;

/**
 * Holds an instance of {@link SearchMapping}.
 * <p>
 * Useful when we create a mapping by the time the cache is already started, that is the case of remote query indexing.
 * <p>
 * The holder is mutable, but with it can change only one time. The instance contained is supposed to be immutable.
 *
 * @author Fabio Massimo Ercoli
 */
public class SearchMappingHolder {

   private final BeanReference<? extends IdentifierBridge<Object>> identifierBridge;
   private final Map<String, Object> properties;

   private SearchMappingBuilder builder;
   private volatile SearchMapping searchMapping;
   private volatile EntityLoader<EntityReference, ?> entityLoader;
   private volatile ClassLoader aggregatedClassLoader;
   private volatile Collection<ProgrammaticSearchMappingProvider> mappingProviders;

   public SearchMappingHolder(BeanReference<? extends IdentifierBridge<Object>> identifierBridge,
                              Map<String, Object> properties, ClassLoader aggregatedClassLoader,
                              Collection<ProgrammaticSearchMappingProvider> mappingProviders) {
      this.identifierBridge = identifierBridge;
      this.properties = properties;
      this.aggregatedClassLoader = aggregatedClassLoader;
      this.mappingProviders = mappingProviders;
   }

   public SearchMapping getSearchMapping() {
      return searchMapping;
   }

   public void setEntityLoader(EntityLoader<EntityReference, ?> entityLoader) {
      this.entityLoader = entityLoader;
   }

   public SearchMappingBuilder builder(PojoBootstrapIntrospector introspector) {
      builder = SearchMapping.builder(introspector, entityLoader, aggregatedClassLoader, mappingProviders)
            .setProvidedIdentifierBridge(identifierBridge)
            .setProperties(properties);
      return builder;
   }

   public void build() {
      if (searchMapping != null) {
         // At the moment we do not allow to change the mapping once it is created.
         // Changing the mapping would introduce complex synchronization issues to solve.
         // Basically, we would need to block all the incoming requests, close the old mapping,
         // open the new one and **only after that** we could accept further requests.
         return;
      }
      searchMapping = builder.build();
   }
}
