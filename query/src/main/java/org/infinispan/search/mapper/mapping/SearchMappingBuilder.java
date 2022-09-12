package org.infinispan.search.mapper.mapping;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.hibernate.search.engine.cfg.ConfigurationPropertySource;
import org.hibernate.search.engine.cfg.spi.ConfigurationPropertyChecker;
import org.hibernate.search.engine.common.spi.SearchIntegration;
import org.hibernate.search.engine.common.spi.SearchIntegrationEnvironment;
import org.hibernate.search.engine.common.spi.SearchIntegrationFinalizer;
import org.hibernate.search.engine.common.spi.SearchIntegrationPartialBuildState;
import org.hibernate.search.engine.environment.bean.BeanReference;
import org.hibernate.search.mapper.pojo.bridge.IdentifierBridge;
import org.hibernate.search.mapper.pojo.loading.spi.PojoSelectionEntityLoader;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.ProgrammaticMappingConfigurationContext;
import org.hibernate.search.mapper.pojo.model.spi.PojoBootstrapIntrospector;
import org.hibernate.search.util.common.impl.SuppressingCloser;
import org.hibernate.search.util.common.reflect.spi.ValueReadHandleFactory;
import org.infinispan.search.mapper.impl.InfinispanMappingInitiator;
import org.infinispan.search.mapper.mapping.impl.ClassLoaderServiceImpl;
import org.infinispan.search.mapper.mapping.impl.IndexProperties;
import org.infinispan.search.mapper.mapping.impl.InfinispanMapping;
import org.infinispan.search.mapper.mapping.impl.InfinispanMappingKey;
import org.infinispan.search.mapper.model.impl.InfinispanBootstrapIntrospector;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.NonBlockingManager;

public final class SearchMappingBuilder {

   public static InfinispanBootstrapIntrospector introspector(MethodHandles.Lookup lookup) {
      ValueReadHandleFactory valueReadHandleFactory = ValueReadHandleFactory.usingMethodHandle(lookup);
      return new InfinispanBootstrapIntrospector(valueReadHandleFactory);
   }

   private final ConfigurationPropertyChecker propertyChecker;
   private final IndexProperties indexProperties = new IndexProperties();
   private final ConfigurationPropertySource propertySource;
   private final InfinispanMappingKey mappingKey;
   private final InfinispanMappingInitiator mappingInitiator;
   private final ClassLoaderServiceImpl classLoaderService;

   SearchMappingBuilder(PojoBootstrapIntrospector introspector, ClassLoader aggregatedClassLoader,
                        Collection<ProgrammaticSearchMappingProvider> mappingProviders,
                        BlockingManager blockingManager, NonBlockingManager nonBlockingManager) {
      propertyChecker = ConfigurationPropertyChecker.create();
      propertySource = indexProperties.createPropertySource(propertyChecker);

      mappingKey = new InfinispanMappingKey();
      mappingInitiator = new InfinispanMappingInitiator(introspector, mappingProviders,
            blockingManager, nonBlockingManager);

      // Enable annotated type discovery by default
      mappingInitiator.annotatedTypeDiscoveryEnabled(true);

      classLoaderService = (aggregatedClassLoader != null) ? new ClassLoaderServiceImpl(aggregatedClassLoader) : null;
   }

   public ProgrammaticMappingConfigurationContext programmaticMapping() {
      return mappingInitiator.programmaticMapping();
   }

   /**
    * Register a type as an entity type with the default name, its class name.
    *
    * @param type The type to be considered as an entity type.
    * @return {@code this}, for call chaining.
    */
   public SearchMappingBuilder addEntityType(Class<?> type) {
      return addEntityType(type, type.getName());
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

   public SearchMappingBuilder setEntityLoader(PojoSelectionEntityLoader<?> entityLoader) {
      mappingInitiator.setEntityLoader(entityLoader);
      return this;
   }

   public SearchMappingBuilder setEntityConverter(EntityConverter entityConverter) {
      mappingInitiator.setEntityConverter(entityConverter);
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

   public SearchMapping build(Optional<SearchIntegration> previousIntegration) {
      SearchIntegrationEnvironment.Builder envBuilder = SearchIntegrationEnvironment.builder(propertySource, propertyChecker);
      if (classLoaderService != null) {
         envBuilder.classResolver(classLoaderService);
         envBuilder.resourceResolver(classLoaderService);
         envBuilder.serviceResolver(classLoaderService);
      }
      SearchIntegrationEnvironment environment = envBuilder.build();

      SearchIntegrationPartialBuildState integrationPartialBuildState = null;
      SearchMapping mapping;
      SearchIntegration integration;
      try {
         SearchIntegration.Builder integrationBuilder = (previousIntegration.isPresent()) ?
               previousIntegration.get().restartBuilder(environment) :
               SearchIntegration.builder(environment);

         integrationBuilder.addMappingInitiator(mappingKey, mappingInitiator);
         integrationPartialBuildState = integrationBuilder.prepareBuild();

         SearchIntegrationFinalizer finalizer = integrationPartialBuildState.finalizer(propertySource, propertyChecker);
         mapping = finalizer.finalizeMapping(mappingKey, (context, partialMapping) -> partialMapping.finalizeMapping());
         integration = finalizer.finalizeIntegration();
      } catch (RuntimeException e) {
         new SuppressingCloser(e)
               .push(environment)
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
