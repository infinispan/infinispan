package org.infinispan.query.remote.impl.mapping;

import java.util.Set;

import org.hibernate.search.engine.search.loading.spi.EntityLoader;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.ProgrammaticMappingConfigurationContext;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.impl.indexing.ProtobufEntityConverter;
import org.infinispan.query.remote.impl.mapping.model.ProtobufBootstrapIntrospector;
import org.infinispan.query.remote.impl.mapping.reference.GlobalReferenceHolder;
import org.infinispan.query.remote.impl.mapping.typebridge.ProtobufMessageBinder;
import org.infinispan.search.mapper.common.EntityReference;
import org.infinispan.search.mapper.mapping.SearchMapping;
import org.infinispan.search.mapper.mapping.SearchMappingBuilder;
import org.infinispan.search.mapper.mapping.SearchMappingCommonBuilding;

public class SerializationContextSearchMapping {

   private final SerializationContext serializationContext;

   public static SerializationContextSearchMapping acquire(SerializationContext serializationContext) {
      return new SerializationContextSearchMapping(serializationContext);
   }

   private SerializationContextSearchMapping(SerializationContext serializationContext) {
      this.serializationContext = serializationContext;
   }

   // TODO ISPN-12169 make this class an helper
   public SearchMapping buildMapping(SearchMappingCommonBuilding commonBuilding,
                                     EntityLoader<EntityReference, ?> entityLoader, Set<String> indexedEntityTypes) {
      GlobalReferenceHolder globalReferenceHolder = new GlobalReferenceHolder(serializationContext.getGenericDescriptors());

      ProtobufBootstrapIntrospector introspector = new ProtobufBootstrapIntrospector();
      SearchMappingBuilder builder = commonBuilding.builder(introspector);
      builder.setEntityLoader(entityLoader);
      builder.setEntityConverter(new ProtobufEntityConverter(serializationContext, globalReferenceHolder.getRootMessages()));
      ProgrammaticMappingConfigurationContext programmaticMapping = builder.programmaticMapping();

      if (globalReferenceHolder.getRootMessages().isEmpty()) {
         return null;
      }

      boolean existIndexedEntities = false;
      for (GlobalReferenceHolder.RootMessageInfo rootMessage : globalReferenceHolder.getRootMessages()) {
         String fullName = rootMessage.getFullName();
         if (!indexedEntityTypes.contains(fullName)) {
            continue;
         }

         existIndexedEntities = true;

         programmaticMapping.type(fullName)
               .binder(new ProtobufMessageBinder(globalReferenceHolder, fullName))
               .indexed().index(rootMessage.getIndexName());

         builder.addEntityType(byte[].class, fullName);
      }

      return (existIndexedEntities) ? builder.build() : null;
   }
}
