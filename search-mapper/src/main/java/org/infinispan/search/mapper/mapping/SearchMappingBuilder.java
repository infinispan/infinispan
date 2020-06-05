package org.infinispan.search.mapper.mapping;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.hibernate.search.engine.cfg.spi.ConfigurationPropertyChecker;
import org.hibernate.search.engine.cfg.spi.ConfigurationPropertySource;
import org.hibernate.search.engine.common.spi.SearchIntegration;
import org.hibernate.search.engine.common.spi.SearchIntegrationBuilder;
import org.hibernate.search.engine.common.spi.SearchIntegrationFinalizer;
import org.hibernate.search.engine.common.spi.SearchIntegrationPartialBuildState;
import org.hibernate.search.engine.environment.bean.BeanReference;
import org.hibernate.search.engine.search.loading.spi.EntityLoader;
import org.hibernate.search.mapper.pojo.bridge.IdentifierBridge;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.AnnotationMappingConfigurationContext;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.ProgrammaticMappingConfigurationContext;
import org.hibernate.search.mapper.pojo.model.spi.PojoBootstrapIntrospector;
import org.hibernate.search.util.common.impl.SuppressingCloser;
import org.hibernate.search.util.common.reflect.spi.ValueReadHandleFactory;
import org.infinispan.search.mapper.common.EntityReference;
import org.infinispan.search.mapper.impl.InfinispanMappingInitiator;
import org.infinispan.search.mapper.mapping.impl.ClassLoaderServiceImpl;
import org.infinispan.search.mapper.mapping.impl.InfinispanMapping;
import org.infinispan.search.mapper.mapping.impl.InfinispanMappingKey;
import org.infinispan.search.mapper.mapping.impl.IndexProperties;
import org.infinispan.search.mapper.model.impl.InfinispanBootstrapIntrospector;

public final class SearchMappingBuilder {

   public static InfinispanBootstrapIntrospector introspector(MethodHandles.Lookup lookup) {
      ValueReadHandleFactory valueReadHandleFactory = ValueReadHandleFactory.usingMethodHandle(lookup);
      return new InfinispanBootstrapIntrospector(valueReadHandleFactory);
   }

   private final ConfigurationPropertyChecker propertyChecker;
   private final IndexProperties indexProperties = new IndexProperties();
   private final ConfigurationPropertySource propertySource;
   private final SearchIntegrationBuilder integrationBuilder;
   private final InfinispanMappingKey mappingKey;
   private final InfinispanMappingInitiator mappingInitiator;

   SearchMappingBuilder(PojoBootstrapIntrospector introspector, EntityLoader<EntityReference, ?> entityLoader,
                        ClassLoader aggregatedClassLoader, Collection<ProgrammaticSearchMappingProvider> mappingProviders) {
      propertyChecker = ConfigurationPropertyChecker.create();
      propertySource = indexProperties.createPropertySource(propertyChecker);
      integrationBuilder = SearchIntegration.builder(propertySource, propertyChecker);
      mappingKey = new InfinispanMappingKey();
      mappingInitiator = new InfinispanMappingInitiator(introspector, entityLoader, mappingProviders);
      integrationBuilder.addMappingInitiator(mappingKey, mappingInitiator);
      // Enable annotated type discovery by default
      mappingInitiator.annotatedTypeDiscoveryEnabled(true);

      if (aggregatedClassLoader != null) {
         ClassLoaderServiceImpl classLoaderService = new ClassLoaderServiceImpl(aggregatedClassLoader);
         integrationBuilder.classResolver(classLoaderService);
         integrationBuilder.resourceResolver(classLoaderService);
         integrationBuilder.serviceResolver(classLoaderService);
      }
   }

   public ProgrammaticMappingConfigurationContext programmaticMapping() {
      return mappingInitiator.programmaticMapping();
   }

   public AnnotationMappingConfigurationContext annotationMapping() {
      return mappingInitiator.annotationMapping();
   }

   /**
    * Register a type as an entity type with the default name, its simple class name.
    *
    * @param type The type to be considered as an entity type.
    * @return {@code this}, for call chaining.
    */
   public SearchMappingBuilder addEntityType(Class<?> type) {
      return addEntityType(type, type.getSimpleName());
   }

   /**
    * Register a type as an entity type with the given name.
    *
    * @param type       The type to be considered as an entity type.
    * @param entityName The name of the entity.
    * @return {@code this}, for call chaining.
    */
   public SearchMappingBuilder addEntityType(Class<?> type, String entityName) {
      mappingInitiator.addEntityType(type, entityName);
      return this;
   }

   /**
    * @param types The types to be considered as entity types.
    * @return {@code this}, for call chaining.
    */
   public SearchMappingBuilder addEntityTypes(Set<Class<?>> types) {
      for (Class<?> type : types) {
         addEntityType(type);
      }
      return this;
   }

   public SearchMappingBuilder setProvidedIdentifierBridge(BeanReference<? extends IdentifierBridge<Object>> providedIdentifierBridge) {
      mappingInitiator.providedIdentifierBridge(providedIdentifierBridge);
      return this;
   }

   public SearchMappingBuilder setEntityConverter(EntityConverter entityConverter) {
      mappingInitiator.setEntityConverter(entityConverter);
      return this;
   }

   public SearchMappingBuilder setAnnotatedTypeDiscoveryEnabled(boolean annotatedTypeDiscoveryEnabled) {
      mappingInitiator.annotatedTypeDiscoveryEnabled(annotatedTypeDiscoveryEnabled);
      return this;
   }

   public SearchMappingBuilder setProperty(String name, Object value) {
      indexProperties.setProperty(name, value);
      return this;
   }

   public SearchMappingBuilder setProperties(Map<String, Object> map) {
      indexProperties.setProperties(map);
      return this;
   }

   public SearchMapping build() {
      SearchIntegrationPartialBuildState integrationPartialBuildState = integrationBuilder.prepareBuild();
      SearchIntegration integration;
      SearchMapping mapping;
      try {
         SearchIntegrationFinalizer finalizer = integrationPartialBuildState.finalizer(propertySource, propertyChecker);
         mapping = finalizer.finalizeMapping(mappingKey, (context, partialMapping) -> partialMapping.finalizeMapping());
         integration = finalizer.finalizeIntegration();
      } catch (RuntimeException e) {
         new SuppressingCloser(e)
               .push(SearchIntegrationPartialBuildState::closeOnFailure, integrationPartialBuildState);
         throw e;
      }

      try {
         /*
          * Since the user doesn't have access to the integration, but only to the (closeable) mapping,
          * make sure to close the integration whenever the mapping is closed by the user.
          */
         InfinispanMapping mappingImpl = (InfinispanMapping) mapping;
         mappingImpl.setIntegration(integration);
         return mappingImpl;
      } catch (RuntimeException e) {
         new SuppressingCloser(e).push(integration);
         throw e;
      }
   }
}
